use arrow::record_batch::RecordBatch;

use crate::physical_plan::PhysicalOperator;
use super::{apply_mask, project_columns};

pub struct IntermediateProjectionExec {
    pub columns: Vec<String>,
    pub child: Box<dyn PhysicalOperator>,
}

impl IntermediateProjectionExec {
    pub fn new(columns: Vec<String>, child: Box<dyn PhysicalOperator>) -> Self {
        Self { columns, child }
    }
}

impl PhysicalOperator for IntermediateProjectionExec {
    fn execute(&mut self) -> Option<Result<RecordBatch, Box<dyn std::error::Error>>> {
        let result = self.child.execute()?;
        Some(result.and_then(|batch| {
            let batch = apply_mask(batch)?;
            let batch = project_columns(batch, &self.columns)?;

            // TODO: cache as raw Arrow IPC — no compression
            // if batch exceeds size threshold, compress and flag accordingly

            Ok(batch)
        }))
    }
}
