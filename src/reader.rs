use crate::error::Result;
use crate::read_options::JsReaderOptions;
use arrow_wasm::{Schema, Table};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

/// Internal function to read a buffer with Parquet data into a buffer with Arrow IPC Stream data
pub fn read_parquet(parquet_file: Vec<u8>, options: JsReaderOptions) -> Result<Table> {
    // Create Parquet reader
    let cursor: Bytes = parquet_file.into();
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(cursor)?;
    let schema = builder.schema().clone();

    if let Some(batch_size) = options.batch_size {
        builder = builder.with_batch_size(batch_size);
    }

    if let Some(row_groups) = options.row_groups {
        builder = builder.with_row_groups(row_groups);
    }

    if let Some(limit) = options.limit {
        builder = builder.with_limit(limit);
    }

    if let Some(offset) = options.offset {
        builder = builder.with_offset(offset);
    }

    // Create Arrow reader
    let reader = builder.build()?;

    let mut batches = vec![];

    for maybe_chunk in reader {
        batches.push(maybe_chunk?)
    }

    Ok(Table::new(schema, batches))
}

/// Internal function to read a buffer with Parquet data into an Arrow schema
pub fn read_schema(parquet_file: Vec<u8>) -> Result<Schema> {
    // Create Parquet reader
    let cursor: Bytes = parquet_file.into();
    let builder = ParquetRecordBatchReaderBuilder::try_new(cursor)?;
    let schema = builder.schema().clone();
    Ok(schema.into())
}
