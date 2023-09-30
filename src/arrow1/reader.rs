use crate::arrow1::error::Result;
use arrow_wasm::arrow1::Table;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

/// Internal function to read a buffer with Parquet data into a buffer with Arrow IPC Stream data
/// using the arrow and parquet crates
pub fn read_parquet(parquet_file: Vec<u8>) -> Result<Table> {
    // Create Parquet reader
    let cursor: Bytes = parquet_file.into();
    let builder = ParquetRecordBatchReaderBuilder::try_new(cursor).unwrap();

    // Create Arrow reader
    let reader = builder.build().unwrap();

    let mut batches = vec![];

    for maybe_chunk in reader {
        batches.push(maybe_chunk?)
    }

    Ok(Table::new(batches))
}
