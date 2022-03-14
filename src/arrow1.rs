#[cfg(feature = "arrow1")]
use {
    arrow::ipc::reader::StreamReader,
    arrow::ipc::writer::StreamWriter,
    parquet::arrow::arrow_writer::ArrowWriter,
    parquet::arrow::{ArrowReader, ParquetFileArrowReader},
    parquet::errors::ParquetError,
    parquet::file::reader::{FileReader, SerializedFileReader},
    parquet::file::serialized_reader::SliceableCursor,
    parquet::file::writer::InMemoryWriteableCursor,
    std::io::Cursor,
    std::sync::Arc,
};

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
    // Create Parquet reader
    let sliceable_cursor = SliceableCursor::new(Arc::new(parquet_file.to_vec()));
    let parquet_reader = SerializedFileReader::new(sliceable_cursor)?;
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
    return Ok(writer_buffer.to_vec());
}

#[cfg(feature = "arrow1")]
pub fn write_parquet(
    arrow_file: &[u8],
    writer_properties: crate::writer_properties1::WriterProperties,
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
