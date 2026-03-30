use arrow::record_batch::RecordBatch;

use crate::physical_plan::PhysicalOperator;
use super::{apply_mask, project_columns};

pub struct IntermediateProjectionExec {
    pub columns: Vec<String>,
    pub parent: Box<dyn PhysicalOperator>,
}

impl IntermediateProjectionExec {
    pub fn new(columns: Vec<String>, parent: Box<dyn PhysicalOperator>) -> Self {
        Self { columns, parent }
    }
}

impl PhysicalOperator for IntermediateProjectionExec {
    fn execute(&mut self, batch: RecordBatch) -> Result<(), Box<dyn std::error::Error>> {
        let batch = apply_mask(batch)?;
        let batch = project_columns(batch, &self.columns)?;

        // TODO: cache as raw Arrow IPC — no compression
        // if batch exceeds size threshold, compress and flag accordingly

        self.parent.execute(batch)
    }
}