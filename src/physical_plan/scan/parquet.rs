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


mod tests {
    use super::*;

    // Verifies that ParquetScanExec reads batches from a real parquet file.
    // Asserts the schema is correct and at least one batch is returned with the expected columns.
    #[test]
    fn test_parquet_scan_reads_batches() {
        let mut scan = ParquetScanExec::new("tests/data/data.parquet", 1024).unwrap();

        let batch = scan.execute().unwrap().unwrap();

        assert_eq!(batch.num_columns(), 13);
        assert_eq!(batch.schema().field(0).name(), "Date");
        assert_eq!(batch.schema().field(1).name(), "Adj Close");
        assert_eq!(batch.schema().field(6).name(), "Volume");
    }

    // Verifies that execute() returns None after all batches are exhausted.
    #[test]
    fn test_parquet_scan_exhausts() {
        let mut scan = ParquetScanExec::new("tests/data/data.parquet", 10_000).unwrap();

        let first = scan.execute();
        assert!(first.is_some());

        let second = scan.execute();
        assert!(second.is_none());
    }
}