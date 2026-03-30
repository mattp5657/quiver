pub mod intermediate;
pub mod output;

use std::sync::Arc;

use arrow::array::BooleanArray;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::physical_plan::filter::FILTER_MASK_COLUMN;
use crate::physical_plan::PhysicalOperator;

pub use intermediate::IntermediateProjectionExec;
pub use output::OutputProjectionExec;

pub enum ProjectionExec {
    Intermediate(IntermediateProjectionExec),
    Output(OutputProjectionExec),
}

impl PhysicalOperator for ProjectionExec {
    fn execute(&mut self, batch: RecordBatch) -> Result<(), Box<dyn std::error::Error>>{
        match self {
            ProjectionExec::Intermediate(p) => p.execute(batch),
            ProjectionExec::Output(p) => p.execute(batch),
        }
    }
}

pub(super) fn apply_mask(batch: RecordBatch) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let schema = batch.schema();
    if let Ok(mask_idx) = schema.index_of(FILTER_MASK_COLUMN) {
        let mask = batch
            .column(mask_idx)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or("__filter_mask__ is not a BooleanArray")?;
        Ok(arrow::compute::filter_record_batch(&batch, mask)?)
    } else {
        Ok(batch)
    }
}

pub(super) fn project_columns(
    batch: RecordBatch,
    columns: &[String],
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let schema = batch.schema();

    let fields = columns
        .iter()
        .map(|col| schema.field_with_name(col).cloned().map_err(|e| Box::new(e) as Box<dyn std::error::Error>))
        .collect::<Result<Vec<_>, _>>()?;

    let arrays = columns
        .iter()
        .map(|col| schema.index_of(col).map(|idx| batch.column(idx).clone()).map_err(|e| Box::new(e) as Box<dyn std::error::Error>))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow::array::{BooleanArray, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    fn make_batch_with_mask(a_vals: Vec<i64>, mask_vals: Vec<bool>) -> RecordBatch {
        let b_vals: Vec<i64> = a_vals.iter().map(|v| v * 2).collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new(FILTER_MASK_COLUMN, DataType::Boolean, false),
        ]));
        RecordBatch::try_new(schema, vec![
            Arc::new(Int64Array::from(a_vals)),
            Arc::new(Int64Array::from(b_vals)),
            Arc::new(BooleanArray::from(mask_vals)),
        ]).unwrap()
    }

    fn make_batch_no_mask(a_vals: Vec<i64>, b_vals: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        RecordBatch::try_new(schema, vec![
            Arc::new(Int64Array::from(a_vals)),
            Arc::new(Int64Array::from(b_vals)),
        ]).unwrap()
    }

    // --- apply_mask ---

    #[test]
    fn test_apply_mask_filters_rows() {
        let batch = make_batch_with_mask(vec![1, 2, 3, 4, 5], vec![true, false, true, false, true]);
        let result = apply_mask(batch).unwrap();
        let a = result.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(result.num_rows(), 3);
        assert_eq!(a.value(0), 1);
        assert_eq!(a.value(1), 3);
        assert_eq!(a.value(2), 5);
    }

    #[test]
    fn test_apply_mask_passthrough_when_no_mask() {
        let batch = make_batch_no_mask(vec![1, 2, 3], vec![10, 20, 30]);
        let result = apply_mask(batch).unwrap();
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.num_columns(), 2);
    }

    #[test]
    fn test_apply_mask_all_false_returns_empty() {
        let batch = make_batch_with_mask(vec![1, 2, 3], vec![false, false, false]);
        let result = apply_mask(batch).unwrap();
        assert_eq!(result.num_rows(), 0);
    }

    #[test]
    fn test_apply_mask_all_true_returns_all() {
        let batch = make_batch_with_mask(vec![1, 2, 3], vec![true, true, true]);
        let result = apply_mask(batch).unwrap();
        assert_eq!(result.num_rows(), 3);
    }

    // --- project_columns ---

        #[test]
    fn test_project_columns_selects_correct() {
        let batch = make_batch_no_mask(vec![1, 2, 3], vec![10, 20, 30]);
        let result = project_columns(batch, &["a".to_string()]).unwrap();
        assert_eq!(result.num_columns(), 1);
        assert_eq!(result.schema().field(0).name(), "a");
    }

    #[test]
    fn test_project_columns_preserves_order() {
        let batch = make_batch_no_mask(vec![1, 2, 3], vec![10, 20, 30]);
        let result = project_columns(batch, &["b".to_string(), "a".to_string()]).unwrap();
        assert_eq!(result.schema().field(0).name(), "b");
        assert_eq!(result.schema().field(1).name(), "a");
    }

    #[test]
    fn test_project_columns_unknown_column_errors() {
        let batch = make_batch_no_mask(vec![1, 2, 3], vec![10, 20, 30]);
        assert!(project_columns(batch, &["z".to_string()]).is_err());
    }

    #[test]
    fn test_project_columns_drops_mask() {
        let batch = make_batch_with_mask(vec![1, 2, 3], vec![true, false, true]);
        let filtered = apply_mask(batch).unwrap();
        let result = project_columns(filtered, &["a".to_string()]).unwrap();
        assert!(result.schema().index_of(FILTER_MASK_COLUMN).is_err());
        assert_eq!(result.num_rows(), 2);
    }
}