pub mod memory;
pub mod parquet;
pub mod csv;

use arrow::record_batch::RecordBatch;
use crate::physical_plan::PhysicalOperator;

pub use memory::MemoryScanExec;
pub use parquet::ParquetScanExec;
pub use csv::CsvScanExec;

pub enum ScanExec {
    Memory(MemoryScanExec),
    Parquet(ParquetScanExec),
    Csv(CsvScanExec),
}

impl PhysicalOperator for ScanExec {
    fn execute(&mut self) ->  Option<Result<RecordBatch, Box<dyn std::error::Error>>>  {
        match self {
            ScanExec::Memory(s) => s.execute(),
            ScanExec::Parquet(s) => s.execute(),
            ScanExec::Csv(s) => s.execute(),
        }
    }
}