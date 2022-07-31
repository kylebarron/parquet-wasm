use crate::arrow2::error::Result;
use arrow2::io::ipc::write::{StreamWriter as IPCStreamWriter, WriteOptions as IPCWriteOptions};
use arrow2::io::parquet::read::{
    infer_schema, read_columns_many, FileReader as ParquetFileReader, RowGroupDeserializer,
};
use parquet2::metadata::FileMetaData;
use std::io::Cursor;

/// Internal function to read a buffer with Parquet data into a buffer with Arrow IPC Stream data
/// using the arrow2 and parquet2 crates
pub fn read_parquet(parquet_file: &[u8]) -> Result<Vec<u8>> {
    // Create Parquet reader
    let input_file = Cursor::new(parquet_file);
    let file_reader = ParquetFileReader::try_new(input_file, None, None, None, None)?;
    let schema = file_reader.schema().clone();

    // Create IPC writer
    let mut output_file = Vec::new();
    let options = IPCWriteOptions { compression: None };
    let mut writer = IPCStreamWriter::new(&mut output_file, options);
    writer.start(&schema, None)?;

    // Iterate over reader chunks, writing each into the IPC writer
    for maybe_chunk in file_reader {
        let chunk = maybe_chunk?;
        writer.write(&chunk, None)?;
    }

    writer.finish()?;
    Ok(output_file)
}

/// Read metadata from parquet buffer
pub fn read_metadata(parquet_file: &[u8]) -> Result<FileMetaData> {
    let input_file = Cursor::new(parquet_file);
    let file_reader = ParquetFileReader::try_new(input_file, None, None, None, None)?;
    Ok(file_reader.metadata().clone())
}

/// Read single row group
pub fn read_row_group(parquet_file: &[u8], meta: &FileMetaData, i: usize) -> Result<Vec<u8>> {
    let mut reader = Cursor::new(parquet_file);
    let arrow_schema = infer_schema(meta)?;

    let row_group_meta = &meta.row_groups[i];
    let column_chunks = read_columns_many(
        &mut reader,
        row_group_meta,
        arrow_schema.fields.clone(),
        None,
        None,
    )?;

    let result = RowGroupDeserializer::new(column_chunks, row_group_meta.num_rows() as usize, None);

    let mut output_file = Vec::new();
    let options = IPCWriteOptions { compression: None };
    let mut writer = IPCStreamWriter::new(&mut output_file, options);
    writer.start(&arrow_schema, None)?;

    for maybe_chunk in result {
        let chunk = maybe_chunk?;
        writer.write(&chunk, None)?;
    }

    writer.finish()?;
    Ok(output_file)
}
