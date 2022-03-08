use crate::enums::{Compression, Encoding};
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
#[derive(Copy, Clone, Debug)]
pub struct WriteOptions {
    pub compression: Compression,
    pub encoding: Encoding,
}

#[wasm_bindgen]
impl WriteOptions {
    #[wasm_bindgen(constructor)]
    pub fn new(compression: Compression, encoding: Encoding) -> Self {
        Self {
            compression: compression,
            encoding: encoding
        }
    }
}
