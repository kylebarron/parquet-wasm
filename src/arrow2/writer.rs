use arrow2::error::ArrowError;
use arrow2::io::ipc::read::{read_file_metadata, FileReader as IPCFileReader};
use arrow2::io::parquet::write::{FileWriter as ParquetFileWriter, RowGroupIterator};
use std::io::Cursor;

/// Internal function to write a buffer of data in Arrow IPC File format to a Parquet file using
/// the arrow2 and parquet2 crates
pub fn write_parquet(
    arrow_file: &[u8],
    writer_properties: crate::arrow2::writer_properties::WriterProperties,
) -> Result<Vec<u8>, ArrowError> {
    // Create IPC reader
    let mut input_file = Cursor::new(arrow_file);
    let stream_metadata = read_file_metadata(&mut input_file)?;
    let arrow_ipc_reader = IPCFileReader::new(input_file, stream_metadata.clone(), None);

    // Create Parquet writer
    let mut output_file: Vec<u8> = vec![];
    let options = writer_properties.get_write_options();
    let encoding = writer_properties.get_encoding();

    let schema = stream_metadata.schema.clone();
    let mut parquet_writer = ParquetFileWriter::try_new(&mut output_file, schema, options)?;
    parquet_writer.start()?;

    for maybe_chunk in arrow_ipc_reader {
        let chunk = maybe_chunk?;

        let iter = vec![Ok(chunk)];

        // Need to create an encoding for each column
        let mut encodings = vec![];
        for _ in &stream_metadata.schema.fields {
            encodings.push(encoding);
        }

        let row_groups = RowGroupIterator::try_new(
            iter.into_iter(),
            &stream_metadata.schema,
            options,
            encodings,
        );

        // TODO: from clippy:
        // for loop over `row_groups`, which is a `Result`. This is more readably written as an `if let` statement
        for group in row_groups {
            for maybe_column in group {
                let column = maybe_column?;
                let (group, len) = column;
                parquet_writer.write(group, len)?;
            }
        }
    }
    let _size = parquet_writer.end(None)?;
    return Ok(output_file);
}
