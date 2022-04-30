use crate::arrow2::reader_async::read_parquet_metadata_async;
use crate::fetch::get_content_length;
use arrow2::io::parquet::read::FileMetaData;
use wasm_bindgen::prelude::*;

/// Asynchronous implementation of ParquetFile
#[wasm_bindgen]
pub struct AsyncParquetFile {
    url: String,
    content_length: usize,
    metadata: FileMetaData,
}

#[wasm_bindgen]
impl AsyncParquetFile {
    #[wasm_bindgen(constructor)]
    pub async fn new(url: String) -> Result<AsyncParquetFile, JsValue> {
        let content_length = get_content_length(url.clone()).await?;
        let metadata = read_parquet_metadata_async(url.clone(), content_length).await?;

        Ok(Self {
            url,
            content_length,
            metadata,
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

    #[wasm_bindgen]
    pub fn num_rows(&self) -> usize {
        self.metadata.num_rows
    }
}
