use crate::arrow1::error::Result;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;

/// Internal function to write a buffer of data in Arrow IPC Stream format to a Parquet file using
/// the arrow and parquet crates
pub fn write_parquet(
    batches: impl Iterator<Item = RecordBatch>,
    schema: SchemaRef,
    writer_properties: crate::arrow1::writer_properties::WriterProperties,
) -> Result<Vec<u8>> {
    // Create Parquet writer
    let mut output_file: Vec<u8> = vec![];
    let mut writer =
        ArrowWriter::try_new(&mut output_file, schema, Some(writer_properties.into()))?;

    // Iterate over IPC chunks, writing each batch to Parquet
    for record_batch in batches {
        writer.write(&record_batch)?;
    }

    writer.close()?;

    Ok(output_file)
}
