use std::fs::File;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use arrow::record_batch::RecordBatch;

use crate::physical_plan::PhysicalOperator;

pub struct ParquetScanExec {
    path: String,
    batch_size: usize,
    reader: ParquetRecordBatchReader,
    pub parent: Box<dyn PhysicalOperator>,
}

impl ParquetScanExec {
    pub fn new(
        path: impl Into<String>,
        batch_size: usize,
        parent: Box<dyn PhysicalOperator>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let file_path = path.into();
        let file = File::open(&file_path)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
            .with_batch_size(batch_size)
            .build()?;

        Ok(ParquetScanExec { path: file_path, batch_size, reader, parent })
    }

    // Loops through all batches in the file, pushing each one to the parent.
    pub fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        while let Some(result) = self.reader.next() {
            let batch = result?;
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

    // Collects pushed batches for test assertions.
    struct MockParent(Rc<RefCell<Vec<RecordBatch>>>);

    impl PhysicalOperator for MockParent {
        fn execute(&mut self, batch: RecordBatch) -> Result<(), Box<dyn std::error::Error>> {
            self.0.borrow_mut().push(batch);
            Ok(())
        }
    }

    // Verifies all batches from the parquet file are pushed to the parent.
    // Expected: 3 batches, 2524 total rows, correct schema.
    #[test]
    fn test_parquet_scan_pushes_all_batches() {
        let collected = Rc::new(RefCell::new(vec![]));
        let mock = Box::new(MockParent(Rc::clone(&collected)));
        let mut scan = ParquetScanExec::new("tests/data/data.parquet", 1024, mock).unwrap();
        scan.run().unwrap();
        let batches = collected.borrow();
        assert_eq!(batches.len(), 3);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2524);
    }

    // Verifies schema of the first pushed batch.
    #[test]
    fn test_parquet_scan_schema() {
        let collected = Rc::new(RefCell::new(vec![]));
        let mock = Box::new(MockParent(Rc::clone(&collected)));
        let mut scan = ParquetScanExec::new("tests/data/data.parquet", 1024, mock).unwrap();
        scan.run().unwrap();
        let batches = collected.borrow();
        assert_eq!(batches[0].num_columns(), 13);
        assert_eq!(batches[0].schema().field(0).name(), "Date");
        assert_eq!(batches[0].schema().field(1).name(), "Adj Close");
        assert_eq!(batches[0].schema().field(6).name(), "Volume");
    }
}
