use js_sys::Uint8Array;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(js_name = readParquet2)]
pub fn read_parquet2(parquet_file: &[u8]) -> Result<Uint8Array, JsValue> {
    let buffer = match crate::arrow2::reader::read_parquet(parquet_file) {
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

#[wasm_bindgen(js_name = writeParquet2)]
pub fn write_parquet2(arrow_file: &[u8]) -> Result<Uint8Array, JsValue> {
    let buffer = match crate::arrow2::writer::write_parquet(arrow_file) {
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
