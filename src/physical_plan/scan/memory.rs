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