use crate::arrow2::error::Result;
use arrow2::io::parquet::read::{
    infer_schema, read_metadata as parquet_read_metadata, FileReader as ParquetFileReader,
};
use arrow_wasm::arrow2::{RecordBatch, Table};
use parquet2::metadata::{FileMetaData, RowGroupMetaData};
use std::io::Cursor;

/// Internal function to read a buffer with Parquet data into a buffer with Arrow IPC Stream data
/// using the arrow2 and parquet2 crates
pub fn read_parquet(parquet_file: &[u8]) -> Result<Table> {
    // Create Parquet reader
    let mut input_file = Cursor::new(parquet_file);

    let metadata = parquet_read_metadata(&mut input_file)?;
    let schema = infer_schema(&metadata)?;

    let file_reader = ParquetFileReader::new(
        input_file,
        metadata.row_groups,
        schema.clone(),
        None,
        None,
        None,
    );

    let mut batches = vec![];

    for maybe_chunk in file_reader {
        batches.push(maybe_chunk?)
    }

    Ok(Table::new(schema, batches))
}

/// Read metadata from parquet buffer
pub fn read_metadata(parquet_file: &[u8]) -> Result<FileMetaData> {
    let mut input_file = Cursor::new(parquet_file);
    Ok(parquet_read_metadata(&mut input_file)?)
}

/// Read single row group
pub fn read_row_group(
    parquet_file: &[u8],
    schema: arrow2::datatypes::Schema,
    row_group: RowGroupMetaData,
) -> Result<RecordBatch> {
    let input_file = Cursor::new(parquet_file);
    let file_reader = ParquetFileReader::new(
        input_file,
        vec![row_group],
        schema.clone(),
        None,
        None,
        None,
    );

    let chunk = {
        let mut chunks = Vec::with_capacity(1);

        for maybe_chunk in file_reader {
            chunks.push(maybe_chunk?);
        }

        // Should be 1 because only reading one row group
        assert_eq!(chunks.len(), 1);
        chunks.pop().unwrap()
    };

    Ok(RecordBatch::new(schema, chunk))
}
