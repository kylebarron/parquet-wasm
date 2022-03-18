use crate::utils::copy_vec_to_uint8_array;
use js_sys::Uint8Array;
use wasm_bindgen::prelude::*;

/// Read a Parquet file into Arrow data using the [`arrow`](https://crates.io/crates/arrow) and
/// [`parquet`](https://crates.io/crates/parquet) Rust crates.
///
/// @param parquet_file Uint8Array containing Parquet data
/// @returns Uint8Array containing Arrow data in IPC Stream format
#[wasm_bindgen(js_name = readParquet)]
#[cfg(feature = "reader")]
pub fn read_parquet(parquet_file: &[u8]) -> Result<Uint8Array, JsValue> {
    match crate::arrow1::reader::read_parquet(parquet_file) {
        Ok(buffer) => copy_vec_to_uint8_array(buffer),
        Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
    }
}

/// Write Arrow data to a Parquet file using the [`arrow`](https://crates.io/crates/arrow) and
/// [`parquet`](https://crates.io/crates/parquet) Rust crates.
///
/// @param arrow_file Uint8Array containing Arrow data in IPC Stream format
/// @param writer_properties Configuration for writing to Parquet. Use the {@linkcode WriterPropertiesBuilder} to build a writing configuration, then call `.build()` to create an immutable writer properties to pass in here.
/// @returns Uint8Array containing written Parquet data.
#[wasm_bindgen(js_name = writeParquet)]
#[cfg(feature = "writer")]
pub fn write_parquet(
    arrow_file: &[u8],
    // TODO: make this param optional?
    writer_properties: crate::arrow1::writer_properties::WriterProperties,
) -> Result<Uint8Array, JsValue> {
    match crate::arrow1::writer::write_parquet(arrow_file, writer_properties) {
        Ok(buffer) => copy_vec_to_uint8_array(buffer),
        Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
    }
}
