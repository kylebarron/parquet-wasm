use arrow2::error::ArrowError;
use arrow2::io::ipc::write::{StreamWriter as IPCStreamWriter, WriteOptions as IPCWriteOptions};
// NOTE: It's FileReader on latest main but RecordReader in 0.9.2
use arrow2::io::parquet::read::FileReader as ParquetFileReader;
use std::io::Cursor;

/// Internal function to read a buffer with Parquet data into a buffer with Arrow IPC Stream data
/// using the arrow2 and parquet2 crates
pub fn read_parquet(parquet_file: &[u8]) -> Result<Vec<u8>, ArrowError> {
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
    return Ok(output_file);
}
