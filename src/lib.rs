extern crate web_sys;

mod arrow1;
mod arrow2;
mod utils;

use js_sys::Uint8Array;
use wasm_bindgen::prelude::*;

// When the `wee_alloc` feature is enabled, use `wee_alloc` as the global
// allocator.
/*#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;*/

#[cfg(feature = "arrow1")]
#[wasm_bindgen(js_name = readParquet1)]
pub fn read_parquet1(parquet_file: &[u8]) -> Result<Uint8Array, JsValue> {
    let buffer = match crate::arrow1::read_parquet(parquet_file) {
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

#[cfg(feature = "arrow1")]
#[wasm_bindgen(js_name = writeParquet1)]
pub fn write_parquet1(arrow_file: &[u8]) -> Result<Uint8Array, JsValue> {
    let buffer = match crate::arrow1::write_parquet(arrow_file) {
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

#[cfg(feature = "arrow2")]
#[wasm_bindgen(js_name = readParquet2)]
pub fn read_parquet2(parquet_file: &[u8]) -> Result<Uint8Array, JsValue> {
    let buffer = match crate::arrow2::read_parquet(parquet_file) {
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

#[cfg(feature = "arrow2")]
#[wasm_bindgen(js_name = writeParquet2)]
pub fn write_parquet2(arrow_file: &[u8]) -> Result<Uint8Array, JsValue> {
    let buffer = match crate::arrow2::write_parquet(arrow_file) {
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

#[wasm_bindgen(js_name = setPanicHook)]
pub fn set_panic_hook() {
    utils::set_panic_hook();
}
