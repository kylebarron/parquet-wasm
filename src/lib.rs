extern crate web_sys;

mod utils;

use js_sys::Uint8Array;

// use arrow2::io::ipc::read::{read_file_metadata, FileReader as IPCFileReader};
// use arrow2::io::ipc::write::{StreamWriter as IPCStreamWriter, WriteOptions as IPCWriteOptions};
// // NOTE: It's FileReader on latest main but RecordReader in 0.9.2
// use arrow2::io::parquet::read::FileReader as ParquetFileReader;
// use arrow2::io::parquet::write::{
//     Compression, Encoding, FileWriter as ParquetFileWriter, RowGroupIterator, Version,
//     WriteOptions as ParquetWriteOptions,
// };
use std::io::Cursor;

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

use arrow::error::ArrowError;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::serialized_reader::SliceableCursor;
use std::sync::Arc;

#[wasm_bindgen(js_name = readParquet1)]
pub fn read_parquet1(parquet_file: &[u8]) -> Result<Uint8Array, JsValue> {
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
    let mut writer = FileWriter::try_new(&mut file, &arrow_schema).unwrap();

    let result: Result<Vec<()>, ArrowError> = record_batches
        .iter()
        .map(|batch| writer.write(batch))
        .collect();
    result.unwrap();
    writer.finish().unwrap();
    let new_vec = writer.into_inner().unwrap();

    log!("{:?}", &new_vec);
    return Ok(unsafe { Uint8Array::view(&new_vec) });

    // let new_vec.len()
    let return_vec = Uint8Array::new_with_length((new_vec.len() as usize).try_into().unwrap());
    return_vec.copy_from(new_vec);

    return Ok(return_vec);
}
// #[wasm_bindgen(js_name = readParquet)]
// pub fn read_parquet(parquet_file: &[u8]) -> Result<Uint8Array, JsValue> {
//     // Create Parquet reader
//     let input_file = Cursor::new(parquet_file);
//     let file_reader = match ParquetFileReader::try_new(input_file, None, None, None, None) {
//         Ok(file_reader) => file_reader,
//         Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
//     };
//     let schema = file_reader.schema().clone();

//     // Create IPC writer
//     let mut output_file = Vec::new();
//     let options = IPCWriteOptions { compression: None };
//     let mut writer = IPCStreamWriter::new(&mut output_file, options);
//     match writer.start(&schema, None) {
//         Ok(_) => {}
//         Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
//     }

//     // Iterate over reader chunks, writing each into the IPC writer
//     for maybe_chunk in file_reader {
//         let chunk = match maybe_chunk {
//             Ok(chunk) => chunk,
//             Err(error) => {
//                 return Err(JsValue::from_str(format!("{}", error).as_str()));
//             }
//         };

//         match writer.write(&chunk, None) {
//             Ok(_) => {}
//             Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
//         };
//     }

//     match writer.finish() {
//         Ok(_) => {}
//         Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
//     };

//     Ok(unsafe { Uint8Array::view(&output_file) })
// }

// #[wasm_bindgen(js_name = writeParquet)]
// pub fn write_parquet(arrow_file: &[u8]) -> Result<Uint8Array, JsValue> {
//     // Create IPC reader
//     let mut input_file = Cursor::new(arrow_file);

//     let stream_metadata = match read_file_metadata(&mut input_file) {
//         Ok(stream_metadata) => stream_metadata,
//         Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
//     };

//     let arrow_ipc_reader = IPCFileReader::new(input_file, stream_metadata.clone(), None);

//     // Create Parquet writer
//     let mut output_file: Vec<u8> = vec![];
//     let options = ParquetWriteOptions {
//         write_statistics: true,
//         compression: Compression::Snappy,
//         version: Version::V2,
//     };

//     let schema = stream_metadata.schema.clone();
//     let mut parquet_writer = match ParquetFileWriter::try_new(&mut output_file, schema, options) {
//         Ok(parquet_writer) => parquet_writer,
//         Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
//     };

//     match parquet_writer.start() {
//         Ok(_) => {}
//         Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
//     };

//     for maybe_chunk in arrow_ipc_reader {
//         let chunk = match maybe_chunk {
//             Ok(chunk) => chunk,
//             Err(error) => {
//                 return Err(JsValue::from_str(format!("{}", error).as_str()));
//             }
//         };

//         let iter = vec![Ok(chunk)];

//         // Need to create an encoding for each column
//         let mut encodings: Vec<Encoding> = vec![];
//         for _ in &stream_metadata.schema.fields {
//             encodings.push(Encoding::Plain);
//         }

//         let row_groups = RowGroupIterator::try_new(
//             iter.into_iter(),
//             &stream_metadata.schema,
//             options,
//             encodings,
//         );

//         // TODO: from clippy:
//         // for loop over `row_groups`, which is a `Result`. This is more readably written as an `if let` statement
//         for group in row_groups {
//             for maybe_column in group {
//                 let column = match maybe_column {
//                     Ok(column) => column,
//                     Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
//                 };

//                 let (group, len) = column;
//                 match parquet_writer.write(group, len) {
//                     Ok(_) => {}
//                     Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
//                 };
//             }
//         }
//     }
//     let _size = parquet_writer.end(None);

//     Ok(unsafe { Uint8Array::view(&output_file) })
// }

#[wasm_bindgen(js_name = setPanicHook)]
pub fn set_panic_hook() {
    utils::set_panic_hook();
}
