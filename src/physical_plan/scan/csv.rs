use std::fs::File;
use arrow::csv::Reader;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::Schema;
use std::sync::Arc;

use crate::physical_plan::PhysicalOperator;

pub struct CsvScanExec {
    path: String,
    batch_size: usize,
    reader: Reader<File>,
    pub parent: Box<dyn PhysicalOperator>,
}

impl CsvScanExec {
    pub fn new(
        path: impl Into<String>,
        batch_size: usize,
        parent: Box<dyn PhysicalOperator>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let file_path = path.into();
        let file = File::open(&file_path)?;
        let reader = arrow::csv::ReaderBuilder::new(Arc::new(Schema::empty()))
            .with_batch_size(batch_size)
            .build(file)?;

        Ok(CsvScanExec { path: file_path, batch_size, reader, parent })
    }

    // Loops through all rows in the CSV, pushing each batch to the parent.
    pub fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        while let Some(result) = self.reader.next() {
            let batch = result?;
            self.parent.execute(batch)?;
        }
        Ok(())
    }
}
