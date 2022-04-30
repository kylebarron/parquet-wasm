use crate::fetch::get_content_length;
use wasm_bindgen::prelude::*;

/// Asynchronous implementation of ParquetFile
#[wasm_bindgen]
pub struct AsyncParquetFile {
    url: String,
    content_length: usize,
}

#[wasm_bindgen]
impl AsyncParquetFile {
    #[wasm_bindgen(constructor)]
    pub async fn new(url: String) -> Result<AsyncParquetFile, JsValue> {
        let content_length = get_content_length(url.clone()).await?;

        Ok(Self {
            url,
            content_length,
        })
    }

    #[wasm_bindgen]
    pub fn url(&self) -> String {
        self.url.clone()
    }

    #[wasm_bindgen]
    pub fn content_length(&self) -> usize {
        self.content_length
    }
}
