use crate::error::Result;
use arrow::ipc::reader::StreamReader;
use parquet::arrow::arrow_writer::ArrowWriter;
use std::io::Cursor;

/// Internal function to write a buffer of data in Arrow IPC Stream format to a Parquet file using
/// the arrow and parquet crates
pub fn write_parquet(
    arrow_file: &[u8],
    writer_properties: crate::writer_properties::WriterProperties,
) -> Result<Vec<u8>> {
    // Create IPC reader
    let input_file = Cursor::new(arrow_file);
    let arrow_ipc_reader = StreamReader::try_new(input_file, None)?;
    let arrow_schema = arrow_ipc_reader.schema();

    // Create Parquet writer
    let mut output_file: Vec<u8> = vec![];
    let mut writer = ArrowWriter::try_new(
        &mut output_file,
        arrow_schema,
        Some(writer_properties.into()),
    )?;

    // Iterate over IPC chunks, writing each batch to Parquet
    for maybe_record_batch in arrow_ipc_reader {
        let record_batch = maybe_record_batch?;
        writer.write(&record_batch)?;
    }

    writer.close()?;

    Ok(output_file)
}
