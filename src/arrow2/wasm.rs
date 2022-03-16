use crate::utils::copy_vec_to_uint8_array;
use js_sys::Uint8Array;
use wasm_bindgen::prelude::*;

/// Read a Parquet file into a buffer of Arrow data in IPC Stream format, using the arrow2 and
/// parquet2 Rust crates.
#[wasm_bindgen(js_name = readParquet2)]
#[cfg(feature = "reader")]
pub fn read_parquet2(parquet_file: &[u8]) -> Result<Uint8Array, JsValue> {
    match crate::arrow2::reader::read_parquet(parquet_file) {
        Ok(buffer) => copy_vec_to_uint8_array(buffer),
        Err(error) => Err(JsValue::from_str(format!("{}", error).as_str())),
    }
}

/// Write a Parquet file from a buffer of Arrow data in IPC **File** format, using the arrow and
/// parquet Rust crates. Requires a writer_properties argument that can be built from
/// WriterPropertiesBuilder.
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
