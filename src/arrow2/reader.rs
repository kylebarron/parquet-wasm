use crate::arrow2::error::Result;
use crate::arrow2::ffi::{FFIArrowRecordBatch, FFIArrowTable};
use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::io::ipc::write::{StreamWriter as IPCStreamWriter, WriteOptions as IPCWriteOptions};
use arrow2::io::parquet::read::{
    infer_schema, read_metadata as parquet_read_metadata, FileReader as ParquetFileReader,
};
use parquet2::metadata::{FileMetaData, RowGroupMetaData};
use std::io::Cursor;

/// Internal function to read a buffer with Parquet data into a buffer with Arrow IPC Stream data
/// using the arrow2 and parquet2 crates
pub fn read_parquet(
    parquet_file: &[u8],
    chunk_fn: impl Fn(Chunk<Box<dyn Array>>) -> Chunk<Box<dyn Array>>,
) -> Result<Vec<u8>> {
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

    // Create IPC writer
    let mut output_file = Vec::new();
    let options = IPCWriteOptions { compression: None };
    let mut writer = IPCStreamWriter::new(&mut output_file, options);
    writer.start(&schema, None)?;

    // Iterate over reader chunks, writing each into the IPC writer
    for maybe_chunk in file_reader {
        let chunk = chunk_fn(maybe_chunk?);
        writer.write(&chunk, None)?;
    }

    writer.finish()?;
    Ok(output_file)
}

pub fn read_parquet_ffi(
    parquet_file: &[u8],
    chunk_fn: impl Fn(Chunk<Box<dyn Array>>) -> Chunk<Box<dyn Array>>,
) -> Result<FFIArrowTable> {
    // Create Parquet reader
    let mut input_file = Cursor::new(parquet_file);
    let metadata = parquet_read_metadata(&mut input_file)?;
    let schema = infer_schema(&metadata)?;

    let num_row_groups = metadata.row_groups.len();
    let file_reader = ParquetFileReader::new(
        input_file,
        metadata.row_groups,
        schema.clone(),
        None,
        None,
        None,
    );

    let mut ffi_record_batches = Vec::with_capacity(num_row_groups);

    // Iterate over reader chunks, storing each in memory to be used for FFI
    for maybe_chunk in file_reader {
        let chunk = chunk_fn(maybe_chunk?);
        let ffi_record_batch = FFIArrowRecordBatch::from_chunk(chunk, schema.clone());
        ffi_record_batches.push(ffi_record_batch);
    }

    Ok(FFIArrowTable::new(ffi_record_batches))
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
    chunk_fn: impl Fn(Chunk<Box<dyn Array>>) -> Chunk<Box<dyn Array>>,
) -> Result<Vec<u8>> {
    let input_file = Cursor::new(parquet_file);
    let file_reader = ParquetFileReader::new(
        input_file,
        vec![row_group],
        schema.clone(),
        None,
        None,
        None,
    );

    // Create IPC writer
    let mut output_file = Vec::new();
    let options = IPCWriteOptions { compression: None };
    let mut writer = IPCStreamWriter::new(&mut output_file, options);
    writer.start(&schema, None)?;

    // Iterate over reader chunks, writing each into the IPC writer
    for maybe_chunk in file_reader {
        let chunk = chunk_fn(maybe_chunk?);
        writer.write(&chunk, None)?;
    }

    writer.finish()?;
    Ok(output_file)
}
