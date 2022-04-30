use crate::arrow2::ranged_reader::{RangeOutput, RangedAsyncReader};
use crate::arrow2::reader_async::{create_reader, read_parquet_metadata_async};
use crate::fetch::get_content_length;
use arrow2::datatypes::Schema;
use arrow2::io::parquet::read::FileMetaData;
use arrow2::io::parquet::read::{infer_schema, read_metadata_async};
use wasm_bindgen::prelude::*;

/// Asynchronous implementation of ParquetFile
#[wasm_bindgen]
pub struct AsyncParquetFile {
    url: String,
    content_length: usize,
    metadata: FileMetaData,
    reader: RangedAsyncReader,
    schema: Schema,
}

#[wasm_bindgen]
impl AsyncParquetFile {
    #[wasm_bindgen(constructor)]
    pub async fn new(url: String) -> Result<AsyncParquetFile, JsValue> {
        let content_length = get_content_length(url.clone()).await?;

        let mut reader = create_reader(url.clone(), content_length);
        let metadata = read_metadata_async(&mut reader).await.unwrap();
        // let metadata = read_parquet_metadata_async(url.clone(), content_length).await?;
        let schema = infer_schema(&metadata).unwrap();

        Ok(Self {
            url,
            content_length,
            metadata,
            reader,
            schema,
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

    #[wasm_bindgen]
    pub fn column_name(&self, field: usize) -> String {
        self.schema.fields[field].name.clone()
    }
}
