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
pub fn read_parquet1(parquet_file: &[u8]) -> Result<Uint8Array, JsValue> {
    use js_sys::Uint8Array;

    use arrow::error::ArrowError;
    use arrow::ipc::writer::FileWriter;
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::file::serialized_reader::SliceableCursor;
    use std::sync::Arc;

    let parquet_bytes_as_vec = parquet_file.to_vec();
    let parquet_vec_arc = Arc::new(parquet_bytes_as_vec);
    let sliceable_cursor = SliceableCursor::new(parquet_vec_arc);

    let parquet_reader = SerializedFileReader::new(sliceable_cursor).unwrap();
    let parquet_metadata = parquet_reader.metadata();
    let parquet_file_metadata = parquet_metadata.file_metadata();
    let row_count = parquet_file_metadata.num_rows() as usize;

    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(parquet_reader));
    let record_batch_reader = arrow_reader.get_record_reader(row_count).unwrap();
    let arrow_schema = arrow_reader.get_schema().unwrap();

    let mut record_batches: Vec<RecordBatch> = Vec::new();
    for maybe_record_batch in record_batch_reader {
        let record_batch = maybe_record_batch.unwrap();
        record_batches.push(record_batch);
    }

    let mut file = Vec::new();
    {
        let mut writer = FileWriter::try_new(&mut file, &arrow_schema).unwrap();

        let result: Result<Vec<()>, ArrowError> = record_batches
            .iter()
            .map(|batch| writer.write(batch))
            .collect();
        result.unwrap();
        writer.finish().unwrap();
    }

    log!("{:?}", &file);
    let return_vec = Uint8Array::new_with_length((file.len() as usize).try_into().unwrap());
    return_vec.copy_from(&file);

    return Ok(return_vec);
}
