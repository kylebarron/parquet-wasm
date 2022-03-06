extern crate web_sys;

mod utils;

use js_sys::Uint8Array;

use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::io::ipc::write;
// NOTE: It's FileReader on latest main but RecordReader in 0.9.2
use arrow2::io::parquet::read::FileReader;
use std::io::Cursor;
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
pub fn read_parquet2(parquet_file_bytes: &[u8]) -> Result<Uint8Array, JsValue> {
    log!(
        "In rust, parquet bytes array has size: {}",
        parquet_file_bytes.len()
    );

    let mut file = Cursor::new(parquet_file_bytes);
    let mut file_reader = FileReader::try_new(&mut file, None, None, None, None).unwrap();

    let mut chunk_vector: Vec<Chunk<Arc<dyn Array>>> = Vec::new();
    for maybe_chunk in &mut file_reader {
        match maybe_chunk {
            Ok(chunk) => {
                chunk_vector.push(chunk);
            }
            Err(chunk_err) => {
                log!("Failed to read chunk: {}", chunk_err);
            }
        }
    }

    // No idea why but this needs to be after the above block?
    // file_reader.schema() is an immutable borrow
    let schema = &mut file_reader.schema();

    let mut output_file = Vec::new();
    let options = write::WriteOptions { compression: None };
    let mut writer = write::FileWriter::try_new(&mut output_file, &schema, None, options).unwrap();

    for chunk in chunk_vector {
        writer.write(&chunk, None).unwrap();
    }

    writer.finish().unwrap();
    return Ok(unsafe { Uint8Array::view(&output_file) });
}

#[wasm_bindgen]
pub fn init() {
    log!("init - start...");

    utils::set_panic_hook();

    log!("init - complete.");
}
