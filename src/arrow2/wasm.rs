use crate::utils::copy_vec_to_uint8_array;
use js_sys::Uint8Array;
use wasm_bindgen::prelude::*;

/// Read a Parquet file into Arrow data using the [`arrow2`](https://crates.io/crates/arrow2) and
/// [`parquet2`](https://crates.io/crates/parquet2) Rust crates.
///
/// @param parquet_file Uint8Array containing Parquet data
/// @returns Uint8Array containing Arrow data in IPC Stream format
#[wasm_bindgen(js_name = readParquet2)]
#[cfg(feature = "reader")]
pub fn read_parquet2(parquet_file: &[u8]) -> Result<Uint8Array, JsValue> {
    match crate::arrow2::reader::read_parquet(parquet_file) {
        Ok(buffer) => copy_vec_to_uint8_array(buffer),
        Err(error) => Err(JsValue::from_str(format!("{}", error).as_str())),
    }
}


/// Write Arrow data to a Parquet file using the [`arrow2`](https://crates.io/crates/arrow2) and
/// [`parquet2`](https://crates.io/crates/parquet2) Rust crates.
///
/// @param arrow_file Uint8Array containing Arrow data in IPC **File** format
/// @param writer_properties Configuration for writing to Parquet. Use the {@linkcode WriterPropertiesBuilder} to build a writing configuration, then call `.build()` to create an immutable writer properties to pass in here.
/// @returns Uint8Array containing written Parquet data.
#[wasm_bindgen(js_name = writeParquet2)]
#[cfg(feature = "writer")]
pub fn write_parquet2(
    arrow_file: &[u8],
    // TODO: make this param optional?
    writer_properties: crate::arrow2::writer_properties::WriterProperties,
) -> Result<Uint8Array, JsValue> {
    match crate::arrow2::writer::write_parquet(arrow_file, writer_properties) {
        Ok(buffer) => copy_vec_to_uint8_array(buffer),
        Err(error) => Err(JsValue::from_str(format!("{}", error).as_str())),
    }
}
