#[cfg(feature = "arrow2")]
use {
    arrow2::error::ArrowError,
    arrow2::io::ipc::read::{read_file_metadata, FileReader as IPCFileReader},
    arrow2::io::ipc::write::{StreamWriter as IPCStreamWriter, WriteOptions as IPCWriteOptions},
    arrow2::io::parquet::read::FileReader as ParquetFileReader,
    // NOTE: It's FileReader on latest main but RecordReader in 0.9.2
    arrow2::io::parquet::write::{
        Compression, Encoding, FileWriter as ParquetFileWriter, RowGroupIterator, Version,
        WriteOptions as ParquetWriteOptions,
    },
    std::io::Cursor,
};

#[cfg(feature = "arrow2")]
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

#[cfg(feature = "arrow2")]
pub fn write_parquet(arrow_file: &[u8]) -> Result<Vec<u8>, ArrowError> {
    // Create IPC reader
    let mut input_file = Cursor::new(arrow_file);
    let stream_metadata = read_file_metadata(&mut input_file)?;
    let arrow_ipc_reader = IPCFileReader::new(input_file, stream_metadata.clone(), None);

    // Create Parquet writer
    let mut output_file: Vec<u8> = vec![];
    let options = ParquetWriteOptions {
        write_statistics: true,
        compression: Compression::Snappy,
        version: Version::V2,
    };

    let schema = stream_metadata.schema.clone();
    let mut parquet_writer = ParquetFileWriter::try_new(&mut output_file, schema, options)?;
    parquet_writer.start()?;

    for maybe_chunk in arrow_ipc_reader {
        let chunk = maybe_chunk?;

        let iter = vec![Ok(chunk)];

        // Need to create an encoding for each column
        let mut encodings: Vec<Encoding> = vec![];
        for _ in &stream_metadata.schema.fields {
            encodings.push(Encoding::Plain);
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
