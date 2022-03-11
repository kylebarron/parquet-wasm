use parquet::errors::ParquetError;

// A macro to provide `println!(..)`-style syntax for `console.log` logging.
#[cfg(target_arch = "wasm32")]
macro_rules! log {
    ( $( $t:tt )* ) => {
        web_sys::console::log_1(&format!( $( $t )* ).into());
    }
}

#[cfg(not(target_arch = "wasm32"))]
macro_rules! log {
    ( $( $t:tt )* ) => {
        println!("LOG - {}", format!( $( $t )* ));
    }
}

#[cfg(feature = "arrow1")]
pub fn read_parquet(parquet_file: &[u8]) -> Result<Vec<u8>, ParquetError> {
    use arrow::ipc::writer::StreamWriter;
    use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::file::serialized_reader::SliceableCursor;
    use std::sync::Arc;

    // Create Parquet reader
    let sliceable_cursor = SliceableCursor::new(Arc::new(parquet_file.to_vec()));
    let parquet_reader = SerializedFileReader::new(sliceable_cursor)?;
    let parquet_metadata = parquet_reader.metadata();
    let parquet_file_metadata = parquet_metadata.file_metadata();
    let row_count = parquet_file_metadata.num_rows() as usize;

    // Create Arrow reader from Parquet reader
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(parquet_reader));
    // TODO: use Parquet column group row count for arrow record reader row count (i.e. don't read
    // entire file into one IPC batch)
    let record_batch_reader = arrow_reader.get_record_reader(row_count)?;
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
    return Ok(writer_buffer.to_vec());
}

#[cfg(feature = "arrow1")]
pub fn write_parquet(arrow_file: &[u8]) -> Result<Vec<u8>, ParquetError> {
    use arrow::ipc::reader::StreamReader;
    use parquet::arrow::arrow_writer::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::InMemoryWriteableCursor;
    use std::io::Cursor;

    // Create IPC reader
    let input_file = Cursor::new(arrow_file);
    let arrow_ipc_reader = StreamReader::try_new(input_file)?;
    let arrow_schema = arrow_ipc_reader.schema();

    // Create Parquet writer
    let cursor = InMemoryWriteableCursor::default();
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(cursor.clone(), arrow_schema, Some(props))?;

    // Iterate over IPC chunks, writing each batch to Parquet
    for maybe_record_batch in arrow_ipc_reader {
        let record_batch = maybe_record_batch?;
        writer.write(&record_batch)?;
    }

    writer.close()?;

    return Ok(cursor.data());
}
