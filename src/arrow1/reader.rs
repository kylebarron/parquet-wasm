use arrow::ipc::writer::StreamWriter;
use bytes::Bytes;
use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use parquet::errors::ParquetError;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::sync::Arc;

/// Internal function to read a buffer with Parquet data into a buffer with Arrow IPC Stream data
/// using the arrow and parquet crates
pub fn read_parquet(parquet_file: &[u8]) -> Result<Vec<u8>, ParquetError> {
    // Create Parquet reader
    let cursor = Bytes::copy_from_slice(parquet_file);
    let parquet_reader = SerializedFileReader::new(cursor)?;
    let parquet_metadata = parquet_reader.metadata();
    // TODO check that there exists at least one row group
    let first_row_group_metadata = parquet_metadata.row_group(0);
    let row_group_count = first_row_group_metadata.num_rows() as usize;

    // Create Arrow reader from Parquet reader
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(parquet_reader));
    let record_batch_reader = arrow_reader.get_record_reader(row_group_count)?;
    let arrow_schema = arrow_reader.get_schema()?;

    // Create IPC Writer
    let mut output_file = Vec::new();
    let mut writer = StreamWriter::try_new(&mut output_file, &arrow_schema)?;

    // Iterate over record batches, writing them to IPC stream
    for maybe_record_batch in record_batch_reader {
        let record_batch = maybe_record_batch?;
        writer.write(&record_batch)?;
    }
    writer.finish()?;

    let writer_buffer = writer.into_inner()?;
    Ok(writer_buffer.to_vec())
}
