extern crate web_sys;

mod utils;

use arrow::ipc::writer::StreamWriter;
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

    log!("created sliceable cursor");

    let parquet_reader_result = SerializedFileReader::new(sliceable_cursor);

    match parquet_reader_result {
        Ok(parquet_reader) => {
            log!("created parquet reader mk2");
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

            let result_buf: Vec<u8> = Vec::new();
            log!("Initialized output data vector");

            log!("Record batch schema: {}", &record_batch_vector[0].schema());

            let arrow_stream_writer_result =
                StreamWriter::try_new(result_buf, &record_batch_vector[0].schema());

            match arrow_stream_writer_result {
                Ok(mut arrow_stream_writer) => {
                    for record_batch in &record_batch_vector {
                        let rec_batch_write_result = arrow_stream_writer.write(&record_batch);
                        match rec_batch_write_result {
                            Ok(_0) => {}
                            Err(rec_batch_write_err) => {
                                let err_str = format!(
                                    "Failed to write rec batch into stream reader: {}",
                                    rec_batch_write_err
                                );
                                log!("{}", err_str);
                                return Err(JsValue::from_str(err_str.as_str()));
                            }
                        }
                    }

                    let finish_write_result = arrow_stream_writer.finish();
                    match finish_write_result {
                        Ok(_0) => {
                            let completed_result = arrow_stream_writer.into_inner();
                            match completed_result {
                                Ok(stream_data) => {
                                    log!(
                                        "In rust, arrow bytes array has size: {}",
                                        stream_data.len()
                                    );

                                    return Ok(unsafe {
                                        Uint8Array::view(&Arc::new(stream_data).clone())
                                    });
                                }
                                Err(completed_err) => {
                                    let err_str =
                                        format!("Completing write failed: {}", completed_err);
                                    log!("{}", err_str);
                                    return Err(JsValue::from_str(err_str.as_str()));
                                }
                            }
                        }
                        Err(finsh_batch_write_err) => {
                            let err_str = format!(
                                "Failed to finish record batch write: {}",
                                finsh_batch_write_err
                            );
                            log!("{}", err_str);
                            return Err(JsValue::from_str(err_str.as_str()));
                        }
                    }
                }
                Err(create_stream_writer_err) => {
                    let err_str = format!(
                        "Failed to create arrow stream reader: {}",
                        create_stream_writer_err
                    );
                    log!("{}", err_str);
                    return Err(JsValue::from_str(err_str.as_str()));
                }
            }

            log!("Finished reading records into arrow.");
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
