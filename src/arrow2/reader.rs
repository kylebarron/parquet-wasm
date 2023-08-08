use crate::arrow2::error::Result;
use crate::arrow2::ffi::{FFIArrowChunk, FFIArrowSchema, FFIArrowTable};
use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;
use arrow2::io::ipc::write::{StreamWriter as IPCStreamWriter, WriteOptions as IPCWriteOptions};
use arrow2::io::parquet::read::{
    infer_schema, read_metadata as parquet_read_metadata, FileReader as ParquetFileReader,
};
use parquet2::metadata::{FileMetaData, RowGroupMetaData};
use std::io::Cursor;

pub fn read_parquet_metadata(parquet_file: &[u8]) -> Result<FileMetaData> {
    // Create Parquet reader
    let mut input_file = Cursor::new(parquet_file);

    Ok(parquet_read_metadata(&mut input_file)?)
}

/// Internal function to read a buffer with Parquet data into a buffer with Arrow IPC Stream data
/// using the arrow2 and parquet2 crates
pub fn read_parquet(
    parquet_file: &[u8],
    schema_fn: impl Fn(Schema) -> Schema,
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
    writer.start(&schema_fn(schema), None)?;

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

    let file_reader = ParquetFileReader::new(
        input_file,
        metadata.row_groups,
        schema.clone(),
        None,
        None,
        None,
    );

    let ffi_schema: FFIArrowSchema = (&schema).into();
    let mut ffi_chunks: Vec<FFIArrowChunk> = vec![];

    // Iterate over reader chunks, storing each in memory to be used for FFI
    for maybe_chunk in file_reader {
        let chunk = chunk_fn(maybe_chunk?);
        ffi_chunks.push(chunk.into());
    }

    Ok((ffi_schema, ffi_chunks).into())
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
