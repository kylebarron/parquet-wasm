use crate::utils::copy_vec_to_uint8_array;
use js_sys::Uint8Array;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(js_name = readParquet)]
#[cfg(feature = "reader")]
pub fn read_parquet(parquet_file: &[u8]) -> Result<Uint8Array, JsValue> {
    match crate::arrow1::reader::read_parquet(parquet_file) {
        Ok(buffer) => copy_vec_to_uint8_array(buffer),
        Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
    }
}

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
