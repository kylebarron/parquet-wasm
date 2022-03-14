use arrow::ipc::reader::StreamReader;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::errors::ParquetError;
use parquet::file::writer::InMemoryWriteableCursor;
use std::io::Cursor;

pub fn write_parquet(
    arrow_file: &[u8],
    writer_properties: crate::arrow1::writer_properties::WriterProperties,
) -> Result<Vec<u8>, ParquetError> {
    // Create IPC reader
    let input_file = Cursor::new(arrow_file);
    let arrow_ipc_reader = StreamReader::try_new(input_file)?;
    let arrow_schema = arrow_ipc_reader.schema();

    // Create Parquet writer
    let cursor = InMemoryWriteableCursor::default();
    let props = writer_properties.to_upstream();
    let mut writer = ArrowWriter::try_new(cursor.clone(), arrow_schema, Some(props))?;

    // Iterate over IPC chunks, writing each batch to Parquet
    for maybe_record_batch in arrow_ipc_reader {
        let record_batch = maybe_record_batch?;
        writer.write(&record_batch)?;
    }

    writer.close()?;

    return Ok(cursor.data());
}
