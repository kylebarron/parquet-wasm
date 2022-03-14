use js_sys::Uint8Array;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(js_name = readParquet)]
pub fn read_parquet(parquet_file: &[u8]) -> Result<Uint8Array, JsValue> {
    let buffer = match crate::arrow1::reader::read_parquet(parquet_file) {
        // This function would return a rust vec that would be copied to a Uint8Array here
        Ok(buffer) => buffer,
        Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
    };

    let return_len = match (buffer.len() as usize).try_into() {
        Ok(return_len) => return_len,
        Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
    };
    let return_vec = Uint8Array::new_with_length(return_len);
    return_vec.copy_from(&buffer);
    return Ok(return_vec);
}

#[wasm_bindgen(js_name = writeParquet)]
pub fn write_parquet(
    arrow_file: &[u8],
    // TODO: make this param optional?
    writer_properties: crate::arrow1::writer_properties::WriterProperties,
) -> Result<Uint8Array, JsValue> {
    let buffer = match crate::arrow1::writer::write_parquet(arrow_file, writer_properties) {
        // This function would return a rust vec that would be copied to a Uint8Array here
        Ok(buffer) => buffer,
        Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
    };

    let return_len = match (buffer.len() as usize).try_into() {
        Ok(return_len) => return_len,
        Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
    };
    let return_vec = Uint8Array::new_with_length(return_len);
    return_vec.copy_from(&buffer);
    return Ok(return_vec);
}
