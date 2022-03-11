use js_sys::Uint8Array;
use wasm_bindgen::prelude::*;

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
#[wasm_bindgen(js_name = readParquet1)]
pub fn read_parquet(parquet_file: &[u8]) -> Result<Uint8Array, JsValue> {
    use js_sys::Uint8Array;

    use arrow::ipc::writer::StreamWriter;
    use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::file::serialized_reader::SliceableCursor;
    use std::sync::Arc;

    // Create Parquet reader
    let sliceable_cursor = SliceableCursor::new(Arc::new(parquet_file.to_vec()));
    let parquet_reader = match SerializedFileReader::new(sliceable_cursor) {
        Ok(parquet_reader) => parquet_reader,
        Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
    };
    let parquet_metadata = parquet_reader.metadata();
    let parquet_file_metadata = parquet_metadata.file_metadata();
    let row_count = parquet_file_metadata.num_rows() as usize;

    // Create Arrow reader from Parquet reader
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(parquet_reader));
    // TODO: use Parquet column group row count for arrow record reader row count (i.e. don't read
    // entire file into one IPC batch)
    let record_batch_reader = match arrow_reader.get_record_reader(row_count) {
        Ok(record_batch_reader) => record_batch_reader,
        Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
    };
    let arrow_schema = match arrow_reader.get_schema() {
        Ok(arrow_schema) => arrow_schema,
        Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
    };

    // Create IPC Writer
    let mut output_file = Vec::new();
    let mut writer = match StreamWriter::try_new(&mut output_file, &arrow_schema) {
        Ok(writer) => writer,
        Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
    };

    for maybe_record_batch in record_batch_reader {
        let record_batch = match maybe_record_batch {
            Ok(record_batch) => record_batch,
            Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
        };
        match writer.write(&record_batch) {
            Ok(_) => {}
            Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
        };
    }
    match writer.finish() {
        Ok(_) => {}
        Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
    };

    let writer_buffer = match writer.into_inner() {
        Ok(writer_buffer) => writer_buffer,
        Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
    };
    let return_len = match (writer_buffer.len() as usize).try_into() {
        Ok(return_len) => return_len,
        Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
    };
    let return_vec = Uint8Array::new_with_length(return_len);
    return_vec.copy_from(&writer_buffer);
    return Ok(return_vec);
}

#[cfg(feature = "arrow1")]
#[wasm_bindgen(js_name = writeParquet1)]
pub fn write_parquet(arrow_file: &[u8]) -> Result<Uint8Array, JsValue> {
    use arrow::ipc::reader::StreamReader;
    use parquet::arrow::arrow_writer::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::InMemoryWriteableCursor;
    use std::io::Cursor;

    // Create IPC reader
    let input_file = Cursor::new(arrow_file);
    let arrow_ipc_reader = match StreamReader::try_new(input_file) {
        Ok(arrow_ipc_reader) => arrow_ipc_reader,
        Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
    };
    let arrow_schema = arrow_ipc_reader.schema();

    // Create Parquet writer
    let cursor = InMemoryWriteableCursor::default();
    let props = WriterProperties::builder().build();
    let mut writer = match ArrowWriter::try_new(cursor.clone(), arrow_schema, Some(props)) {
        Ok(writer) => writer,
        Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
    };

    // Iterate over IPC chunks, writing each batch to Parquet
    for maybe_record_batch in arrow_ipc_reader {
        let record_batch = match maybe_record_batch {
            Ok(record_batch) => record_batch,
            Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
        };

        match writer.write(&record_batch) {
            Ok(_) => {}
            Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
        };
    }
    match writer.close() {
        Ok(_) => {}
        Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
    };

    let return_buffer = cursor.data();
    let return_len = match (return_buffer.len() as usize).try_into() {
        Ok(return_len) => return_len,
        Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
    };
    let return_vec = Uint8Array::new_with_length(return_len);
    return_vec.copy_from(&return_buffer);
    return Ok(return_vec);
}
