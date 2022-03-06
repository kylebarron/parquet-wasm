extern crate web_sys;

mod utils;

use js_sys::Uint8Array;

use arrow2::io::ipc::read::{
    read_file_metadata, FileReader as IPCFileReader,
};
use arrow2::io::ipc::write::{StreamWriter, WriteOptions as IPCWriteOptions};
// NOTE: It's FileReader on latest main but RecordReader in 0.9.2
use arrow2::io::parquet::read::FileReader;
use arrow2::io::parquet::write::{
    Compression, Encoding, FileWriter, RowGroupIterator, Version,
    WriteOptions as ParquetWriteOptions,
};
use std::io::Cursor;

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
pub fn read_parquet(parquet_file: &[u8]) -> Result<Uint8Array, JsValue> {
    // Create Parquet reader
    let input_file = Cursor::new(parquet_file);
    let file_reader = match FileReader::try_new(input_file, None, None, None, None) {
        Ok(file_reader) => file_reader,
        Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
    };
    let schema = file_reader.schema().clone();

    // Create IPC writer
    let mut output_file = Vec::new();
    let options = IPCWriteOptions { compression: None };
    let mut writer = StreamWriter::new(&mut output_file, options);
    match writer.start(&schema, None) {
        Ok(_) => {}
        Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
    }

    // Iterate over reader chunks, writing each into the IPC writer
    for maybe_chunk in file_reader {
        let chunk = match maybe_chunk {
            Ok(chunk) => chunk,
            Err(error) => {
                return Err(JsValue::from_str(format!("{}", error).as_str()));
            }
        };

        match writer.write(&chunk, None) {
            Ok(_) => {}
            Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
        };
    }

    match writer.finish() {
        Ok(_) => {}
        Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
    };

    return Ok(unsafe { Uint8Array::view(&output_file) });
}

#[wasm_bindgen]
pub fn write_parquet(arrow_stream: &[u8]) -> Result<Uint8Array, JsValue> {
    // Create IPC reader
    let mut input_file = Cursor::new(arrow_stream);
    let stream_metadata = read_file_metadata(&mut input_file).unwrap().clone();
    let stream_reader = IPCFileReader::new(input_file, stream_metadata.clone(), None);

    log!("Created IPC Reader");

    // Create Parquet writer
    let mut output_file: Vec<u8> = vec![];
    let options = ParquetWriteOptions {
        write_statistics: true,
        compression: Compression::Snappy,
        version: Version::V2,
    };
    let mut parquet_writer =
        FileWriter::try_new(&mut output_file, stream_metadata.schema.clone(), options).unwrap();
    log!("Created Parquet writer");

    parquet_writer.start().unwrap();
    log!("Started Parquet writer");

    for maybe_chunk in stream_reader {
        let chunk = match maybe_chunk {
            Ok(chunk) => chunk,
            Err(error) => {
                return Err(JsValue::from_str(format!("{}", error).as_str()));
            }
        };
        log!("Read chunk");

        let iter = vec![Ok(chunk)];
        log!("Created chunk iter");

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
        log!("Created row group iter");

        for group in row_groups {
            log!("Read group");
            for test in group {
                log!("Column?");
                let test2 = test.unwrap();
                let (group, len) = test2;
                parquet_writer.write(group, len).unwrap();
                log!("Write column");
            }
        }
    }
    let _size = parquet_writer.end(None);
    log!("End parquet");

    return Ok(unsafe { Uint8Array::view(&output_file) });
}

#[wasm_bindgen]
pub fn init() {
    utils::set_panic_hook();
}
