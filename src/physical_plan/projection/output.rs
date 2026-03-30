use arrow::record_batch::RecordBatch;

use crate::physical_plan::PhysicalOperator;
use super::{apply_mask, project_columns};

pub struct OutputProjectionExec {
    pub columns: Vec<String>,
    pub results: Vec<RecordBatch>,
}

impl OutputProjectionExec {
    pub fn new(columns: Vec<String>) -> Self {
        Self { columns, results: vec![] }
    }
}

impl PhysicalOperator for OutputProjectionExec {
    fn execute(&mut self, batch: RecordBatch) -> Result<(), Box<dyn std::error::Error>> {
        let batch = apply_mask(batch)?;
        let batch = project_columns(batch, &self.columns)?;
        println!("{:#?}", batch);

        // TODO: cache with compression — CPU cycles acceptable here

        self.results.push(batch);
        Ok(())
    }
}
