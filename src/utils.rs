use js_sys::Uint8Array;
use wasm_bindgen::prelude::*;

/// Call this function at least once during initialization to get better error
// messages if the underlying Rust code ever panics (creates uncaught errors).
#[cfg(feature = "console_error_panic_hook")]
#[wasm_bindgen(js_name = setPanicHook)]
pub fn set_panic_hook() {
    // When the `console_error_panic_hook` feature is enabled, we can call the
    // `set_panic_hook` function at least once during initialization, and then
    // we will get better error messages if our code ever panics.
    //
    // For more details see
    // https://github.com/rustwasm/console_error_panic_hook#readme
    console_error_panic_hook::set_once();
}

// A macro to provide `println!(..)`-style syntax for `console.log` logging.
#[cfg(target_arch = "wasm32")]
#[macro_export]
macro_rules! log {
    ( $( $t:tt )* ) => {
        web_sys::console::log_1(&format!( $( $t )* ).into());
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[macro_export]
macro_rules! log {
    ( $( $t:tt )* ) => {
        println!("LOG - {}", format!( $( $t )* ));
    }
}

/// Copy Vec<u8> to a Uint8Array
pub fn copy_vec_to_uint8_array(buffer: Vec<u8>) -> Result<Uint8Array, JsError> {
    let return_len = match (buffer.len()).try_into() {
        Ok(return_len) => return_len,
        Err(error) => return Err(JsError::new(format!("{}", error).as_str())),
    };
    let return_vec = Uint8Array::new_with_length(return_len);
    return_vec.copy_from(&buffer);
    Ok(return_vec)
}

/// Raise an error if the input array is empty
pub fn assert_parquet_file_not_empty(parquet_file: &[u8]) -> Result<(), JsError> {
    if parquet_file.is_empty() {
        return Err(JsError::new("Empty input provided or not a Uint8Array."));
    }
    Ok(())
}
