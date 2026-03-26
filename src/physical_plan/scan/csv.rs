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
}

impl CsvScanExec {
    pub fn new(path: impl Into<String>, batch_size: usize) -> Result<Self, Box<dyn std::error::Error>> {
        let file_path = path.into();
        let file = File::open(&file_path)?;
        let reader = arrow::csv::ReaderBuilder::new(Arc::new(Schema::empty()))
            .with_batch_size(batch_size)
            .build(file)?;

        Ok(CsvScanExec {
            path: file_path,
            batch_size,
            reader,
        })
    }
}

impl PhysicalOperator for CsvScanExec {
    fn execute(&mut self) -> Option<Result<RecordBatch, Box<dyn std::error::Error>>> {
        self.reader.next().map(|r| r.map_err(|e| e.into()))
    }
}