extern crate web_sys;

mod utils;

use arrow::error::ArrowError;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;

use js_sys::Uint8Array;

use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use parquet::basic::Compression;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::{SerializedFileReader, SliceableCursor};

use std::sync::Arc;

use wasm_bindgen;
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

// When the `wee_alloc` feature is enabled, use `wee_alloc` as the global
// allocator.
/*#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;*/

#[wasm_bindgen]
pub fn read_parquet(parquet_file_bytes: &[u8]) -> Result<Uint8Array, JsValue> {
    log!(
        "In rust, parquet bytes array has size: {}",
        parquet_file_bytes.len()
    );

    let supported_compressions: Vec<Compression> =
        vec![Compression::SNAPPY, Compression::UNCOMPRESSED];

    let parquet_bytes_as_vec = parquet_file_bytes.to_vec();
    let parquet_vec_arc = Arc::new(parquet_bytes_as_vec);
    let sliceable_cursor = SliceableCursor::new(parquet_vec_arc);

    let parquet_reader_result = SerializedFileReader::new(sliceable_cursor);

    match parquet_reader_result {
        Ok(parquet_reader) => {
            let pq_metadata = parquet_reader.metadata();

            // Check if any column chunk has an unsupported compression type
            for row_group_metadata in pq_metadata.row_groups() {
                for column_chunk_metadata in row_group_metadata.columns() {
                    let column_chunk_compression = &column_chunk_metadata.compression();
                    if !supported_compressions.contains(column_chunk_compression) {
                        return Err(JsValue::from_str(
                            format!("Unsupported compression {}", column_chunk_compression)
                                .as_str(),
                        ));
                    }
                }
            }

            let pq_file_metadata = pq_metadata.file_metadata();

            let pq_row_count = pq_file_metadata.num_rows() as usize;
            log!("got parquet metadata: {:?}", pq_file_metadata);

            let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(parquet_reader));

            log!("Got an arrow reader.");

            let record_batch_reader_result = arrow_reader.get_record_reader(pq_row_count);
            let arrow_schema = arrow_reader.get_schema().unwrap();

            log!("Got an arrow batch reader.");

            let mut record_batch_vector: Vec<RecordBatch> = Vec::new();
            log!("Initialized record batch vector");

            match record_batch_reader_result {
                Ok(record_batch_reader) => {
                    log!("Got an arrow record batch reader.");

                    for maybe_record_batch in record_batch_reader {
                        let record_batch = maybe_record_batch.expect("why not read batch");
                        log!(
                            "Read {} records from record batch.",
                            &record_batch.num_rows()
                        );
                        record_batch_vector.push(record_batch);
                    }
                }
                Err(batch_err) => {
                    log!("Failed to get an arrow record batch reader.");
                    return Err(JsValue::from_str(format!("{}", batch_err).as_str()));
                }
            }

            log!(
                "Number of elements in record_batch_vector: {}",
                record_batch_vector.len()
            );

            log!("Record batch schema: {}", &record_batch_vector[0].schema());

            // Cleaner writing to file than I had previously
            // From https://github.com/domoritz/arrow-wasm/blob/159bb145fd93bf7746db3c7d66986468cffd3fdd/src/table.rs#L57-L76
            let mut file = Vec::new();
            {
                let mut writer = FileWriter::try_new(&mut file, &arrow_schema).unwrap();
                let result: Result<Vec<()>, ArrowError> = record_batch_vector
                    .iter()
                    .map(|batch| writer.write(batch))
                    .collect();
                if let Err(error) = result {
                    let err_str = format!("{}", error);
                    return Err(JsValue::from_str(err_str.as_str()));
                }

                if let Err(error) = writer.finish() {
                    let err_str = format!("{}", error);
                    return Err(JsValue::from_str(err_str.as_str()));
                }
            };
            return Ok(unsafe { Uint8Array::view(&file) });
        }
        Err(parquet_reader_err) => {
            log!("Failed to create parquet reader: {}", parquet_reader_err);
            Err(JsValue::from_str(
                format!("{}", parquet_reader_err).as_str(),
            ))
        }
    }
}

#[wasm_bindgen]
pub fn init() {
    log!("init - start...");

    utils::set_panic_hook();

    log!("init - complete.");
}
