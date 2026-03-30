use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::compute;
use arrow::compute::kernels::cmp;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use crate::logical_plan::expr::Expr;
use crate::physical_plan::PhysicalOperator;
use crate::shared::operators::BinaryOp;
use crate::shared::values::Value;

pub const FILTER_MASK_COLUMN: &str = "__filter_mask__";

pub struct FilterExec {
    predicate: Expr,
    parent: Box<dyn PhysicalOperator>,
}

impl FilterExec {
    pub fn new(predicate: Expr, parent: Box<dyn PhysicalOperator>) -> Self {
        FilterExec { predicate, parent }
    }
}

impl PhysicalOperator for FilterExec {
    fn execute(&mut self, batch: RecordBatch) -> Result<(), Box<dyn std::error::Error>> {
        let mask = eval_predicate(&self.predicate, &batch)?;

        // Append mask as a sentinel column
        let mut fields: Vec<Field> = batch.schema().fields().iter().map(|f| f.as_ref().clone()).collect();
        fields.push(Field::new(FILTER_MASK_COLUMN, DataType::Boolean, false));

        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        columns.push(Arc::new(mask));

        let schema = Arc::new(Schema::new(fields));
        let masked_batch = RecordBatch::try_new(schema, columns)?;

        self.parent.execute(masked_batch)
    }
}

// Walks the Expr tree and produces a BooleanArray mask for the given batch.
fn eval_predicate(expr: &Expr, batch: &RecordBatch) -> Result<BooleanArray, Box<dyn std::error::Error>> {
    match expr {
        Expr::BinaryExpr { left, op, right } => {
            match op {
                BinaryOp::And => {
                    let l = eval_predicate(left, batch)?;
                    let r = eval_predicate(right, batch)?;
                    Ok(compute::and(&l, &r)?)
                }
                BinaryOp::Or => {
                    let l = eval_predicate(left, batch)?;
                    let r = eval_predicate(right, batch)?;
                    Ok(compute::or(&l, &r)?)
                }
                _ => {
                    let l = eval_column_or_literal(left, batch)?;
                    let r = eval_column_or_literal(right, batch)?;
                    eval_comparison(op, &l, &r)
                }
            }
        }
        _ => Err("predicate root must be a BinaryExpr".into()),
    }
}

// Resolves a Column or Literal expr to an ArrayRef.
fn eval_column_or_literal(expr: &Expr, batch: &RecordBatch) -> Result<ArrayRef, Box<dyn std::error::Error>> {
    match expr {
        Expr::Column(name) => {
            let idx = batch.schema().index_of(name)?;
            Ok(batch.column(idx).clone())
        }
        Expr::Literal(value) => {
            let len = batch.num_rows();
            match value {
                Value::Int64(v) => Ok(Arc::new(Int64Array::from(vec![*v; len]))),
                Value::Float64(v) => Ok(Arc::new(Float64Array::from(vec![*v; len]))),
                Value::Utf8(v) => Ok(Arc::new(StringArray::from(vec![v.as_str(); len]))),
                Value::Boolean(v) => Ok(Arc::new(BooleanArray::from(vec![*v; len]))),
                _ => Err("unsupported literal type in predicate".into()),
            }
        }
        _ => Err("expected Column or Literal".into()),
    }
}

// Applies a comparison operator to two arrays and returns a BooleanArray.
fn eval_comparison(op: &BinaryOp, left: &ArrayRef, right: &ArrayRef) -> Result<BooleanArray, Box<dyn std::error::Error>> {
    match op {
        BinaryOp::Eq    => Ok(cmp::eq(left, right)?),
        BinaryOp::NotEq => Ok(cmp::neq(left, right)?),
        BinaryOp::Gt    => Ok(cmp::gt(left, right)?),
        BinaryOp::GtEq  => Ok(cmp::gt_eq(left, right)?),
        BinaryOp::Lt    => Ok(cmp::lt(left, right)?),
        BinaryOp::LtEq  => Ok(cmp::lt_eq(left, right)?),
        _ => Err("expected comparison operator".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::expr::Expr;
    use crate::shared::operators::BinaryOp;
    use crate::shared::values::Value;
    use arrow::array::BooleanArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::rc::Rc;
    use std::cell::RefCell;
    use std::sync::Arc;

    // Collects pushed batches for test assertions.
    struct MockParent(Rc<RefCell<Vec<RecordBatch>>>);

    impl PhysicalOperator for MockParent {
        fn execute(&mut self, batch: RecordBatch) -> Result<(), Box<dyn std::error::Error>> {
            self.0.borrow_mut().push(batch);
            Ok(())
        }
    }

    fn make_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
        ]));
        let array = Arc::new(Int64Array::from(vec![1, 5, 10, 3, 8]));
        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    // Verifies that FilterExec appends a __filter_mask__ boolean column to the batch
    // and pushes it to the parent. The mask should reflect which rows satisfy a > 4.
    #[test]
    fn test_filter_appends_mask_column() {
        let predicate = Expr::BinaryExpr {
            left: Box::new(Expr::Column("a".to_string())),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(Value::Int64(4))),
        };

        let collected = Rc::new(RefCell::new(vec![]));
        let mock = Box::new(MockParent(Rc::clone(&collected)));
        let mut filter = FilterExec::new(predicate, mock);

        filter.execute(make_batch()).unwrap();

        let batches = collected.borrow();
        assert_eq!(batches.len(), 1);

        let batch = &batches[0];
        let mask_idx = batch.schema().index_of(FILTER_MASK_COLUMN).unwrap();
        let mask = batch.column(mask_idx).as_any().downcast_ref::<BooleanArray>().unwrap();

        // a > 4: [1=false, 5=true, 10=true, 3=false, 8=true]
        assert_eq!(mask.value(0), false);
        assert_eq!(mask.value(1), true);
        assert_eq!(mask.value(2), true);
        assert_eq!(mask.value(3), false);
        assert_eq!(mask.value(4), true);
    }

    // Verifies that multiple batches can be pushed through the filter.
    #[test]
    fn test_filter_pushes_multiple_batches() {
        let predicate = Expr::BinaryExpr {
            left: Box::new(Expr::Column("a".to_string())),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(Value::Int64(0))),
        };

        let collected = Rc::new(RefCell::new(vec![]));
        let mock = Box::new(MockParent(Rc::clone(&collected)));
        let mut filter = FilterExec::new(predicate, mock);

        filter.execute(make_batch()).unwrap();
        filter.execute(make_batch()).unwrap();

        assert_eq!(collected.borrow().len(), 2);
    }
}
