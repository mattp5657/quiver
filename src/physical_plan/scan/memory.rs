use arrow::record_batch::RecordBatch;

use crate::physical_plan::PhysicalOperator;

pub struct MemoryScanExec {
    data: RecordBatch,
    exhausted: bool,
    pub parent: Box<dyn PhysicalOperator>,
}

impl MemoryScanExec {
    pub fn new(data: RecordBatch, parent: Box<dyn PhysicalOperator>) -> Self {
        MemoryScanExec { data, exhausted: false, parent }
    }

    // Single batch — push it once then mark exhausted.
    pub fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.exhausted {
            self.exhausted = true;
            let batch = self.data.clone();
            self.parent.execute(batch)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::rc::Rc;
    use std::cell::RefCell;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
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
            Field::new("a", DataType::Int32, false),
        ]));
        let array = Arc::new(Int32Array::from(vec![1, 2, 3]));
        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    // Verifies run() pushes exactly one batch to the parent.
    #[test]
    fn test_memory_scan_pushes_batch() {
        let collected = Rc::new(RefCell::new(vec![]));
        let mock = Box::new(MockParent(Rc::clone(&collected)));
        let mut scan = MemoryScanExec::new(make_batch(), mock);
        scan.run().unwrap();
        let batches = collected.borrow();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "a");
    }

    // Verifies run() is a no-op after the scan is exhausted.
    #[test]
    fn test_memory_scan_exhausts() {
        let collected = Rc::new(RefCell::new(vec![]));
        let mock = Box::new(MockParent(Rc::clone(&collected)));
        let mut scan = MemoryScanExec::new(make_batch(), mock);
        scan.run().unwrap();
        scan.run().unwrap();
        assert_eq!(collected.borrow().len(), 1);
    }
}
