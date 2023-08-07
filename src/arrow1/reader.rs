use crate::arrow1::error::Result;
use arrow::ipc::writer::StreamWriter;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

/// Internal function to read a buffer with Parquet data into a buffer with Arrow IPC Stream data
/// using the arrow and parquet crates
pub fn read_parquet(parquet_file: Vec<u8>) -> Result<Vec<u8>> {
    // Create Parquet reader
    let cursor: Bytes = parquet_file.into();
    let builder = ParquetRecordBatchReaderBuilder::try_new(cursor).unwrap();
    let arrow_schema = builder.schema().clone();

    // Create Arrow reader
    let reader = builder.build().unwrap();

    // Create IPC Writer
    let mut output_file = Vec::new();

    {
        let mut writer = StreamWriter::try_new(&mut output_file, &arrow_schema)?;

        // Iterate over record batches, writing them to IPC stream
        for maybe_record_batch in reader {
            let record_batch = maybe_record_batch?;
            writer.write(&record_batch)?;
        }
        writer.finish()?;
    }

    // Note that this returns output_file directly instead of using writer.into_inner().to_vec() as
    // the latter seems likely to incur an extra copy of the vec
    Ok(output_file)
}
