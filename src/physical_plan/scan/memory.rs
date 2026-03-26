use arrow::record_batch::RecordBatch;

use crate::physical_plan::PhysicalOperator;

pub struct MemoryScanExec {
    data: RecordBatch,
    exhausted: bool,
}

impl MemoryScanExec {
    pub fn new(data: RecordBatch) -> Self {
        MemoryScanExec { data, exhausted: false }
    }
}

impl PhysicalOperator for MemoryScanExec {
    fn execute(&mut self) -> Option<Result<RecordBatch, Box<dyn std::error::Error>>> {
        if self.exhausted {
            None  // no more data
        } else {
            self.exhausted = true;
            Some(Ok(self.data.clone()))  // return data once
        }
    }
}

mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
        ]));
        let array = Arc::new(Int32Array::from(vec![1, 2, 3]));
        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    // Verifies that the first execute() call returns the batch passed at construction.
    #[test]
    fn test_memory_scan_returns_batch() {
        let batch = make_batch();
        let mut scan = MemoryScanExec::new(batch.clone());

        let result = scan.execute().unwrap().unwrap();
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.num_columns(), 1);
        assert_eq!(result.schema().field(0).name(), "a");
    }

    // Verifies that execute() returns None on the second call — the scan is single-pass.
    #[test]
    fn test_memory_scan_exhausts() {
        let mut scan = MemoryScanExec::new(make_batch());

        let _ = scan.execute();
        assert!(scan.execute().is_none());
    }
}
