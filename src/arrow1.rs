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

    use arrow::error::ArrowError;
    use arrow::ipc::writer::FileWriter;
    use arrow::record_batch::RecordBatch;
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
    // entire file)
    let record_batch_reader = match arrow_reader.get_record_reader(row_count) {
        Ok(record_batch_reader) => record_batch_reader,
        Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
    };
    let arrow_schema = arrow_reader.get_schema().unwrap();

    // Create IPC Writer
    let mut output_file = Vec::new();
    let mut writer = FileWriter::try_new(&mut output_file, &arrow_schema).unwrap();

    for maybe_record_batch in record_batch_reader {
        let record_batch = match maybe_record_batch {
            Ok(record_batch) => record_batch,
            Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
        };
        writer.write(&record_batch).unwrap();
    }
    writer.finish().unwrap();
    let buf = writer.into_inner().unwrap();

    let return_len = (buf.len() as usize).try_into().unwrap();
    let return_vec = Uint8Array::new_with_length(return_len);
    return_vec.copy_from(&buf);
    return Ok(return_vec);
}
