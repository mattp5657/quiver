use std::fs::File;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use arrow::record_batch::RecordBatch;

use crate::physical_plan::PhysicalOperator;

pub struct ParquetScanExec {
    path: String,
    batch_size: usize,
    reader: ParquetRecordBatchReader,
}

impl ParquetScanExec {

    pub fn new(path: impl Into<String>, batch_size: usize) -> Result<Self, Box<dyn std::error::Error>> {
        let file_path = path.into();
        let file = File::open(&file_path)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
            .with_batch_size(batch_size)
            .build()?;
        
        Ok(ParquetScanExec {
            path: file_path,
            batch_size,
            reader,
        })
    }
}

impl PhysicalOperator for ParquetScanExec {
    fn execute(&mut self) -> Option<Result<RecordBatch, Box<dyn std::error::Error>>> {
        self.reader.next().map(|r| r.map_err(|e| e.into()))
    }
}