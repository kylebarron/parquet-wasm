
use crate::arrow1::error::WasmResult;
use crate::common::fetch::create_reader;
use crate::common::stream::AsyncReadableStreamSink;
use crate::utils::{assert_parquet_file_not_empty, copy_vec_to_uint8_array};
use arrow::ipc::writer::StreamWriter;
use async_compat::CompatExt;
use futures::{StreamExt, AsyncWriteExt};
use js_sys::{Uint8Array, Object};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use wasm_bindgen::prelude::*;

/// Read a Parquet file into Arrow data using the [`arrow`](https://crates.io/crates/arrow) and
/// [`parquet`](https://crates.io/crates/parquet) Rust crates.
///
/// Example:
///
/// ```js
/// import { tableFromIPC } from "apache-arrow";
/// // Edit the `parquet-wasm` import as necessary
/// import { readParquet } from "parquet-wasm/node";
///
/// const resp = await fetch("https://example.com/file.parquet");
/// const parquetUint8Array = new Uint8Array(await resp.arrayBuffer());
/// const arrowUint8Array = readParquet(parquetUint8Array);
/// const arrowTable = tableFromIPC(arrowUint8Array);
/// ```
///
/// @param parquet_file Uint8Array containing Parquet data
/// @returns Uint8Array containing Arrow data in [IPC Stream format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format). To parse this into an Arrow table, pass to `tableFromIPC` in the Arrow JS bindings.
#[wasm_bindgen(js_name = readParquet)]
#[cfg(feature = "reader")]
pub fn read_parquet(parquet_file: Vec<u8>) -> WasmResult<Uint8Array> {
    assert_parquet_file_not_empty(parquet_file.as_slice())?;

    let buffer = crate::arrow1::reader::read_parquet(parquet_file)?;
    copy_vec_to_uint8_array(buffer)
}

/// Write Arrow data to a Parquet file using the [`arrow`](https://crates.io/crates/arrow) and
/// [`parquet`](https://crates.io/crates/parquet) Rust crates.
///
/// For example, to create a Parquet file with Snappy compression:
///
/// ```js
/// import { tableToIPC } from "apache-arrow";
/// // Edit the `parquet-wasm` import as necessary
/// import { WriterPropertiesBuilder, Compression, writeParquet } from "parquet-wasm/node";
///
/// // Given an existing arrow table under `table`
/// const arrowUint8Array = tableToIPC(table, "file");
/// const writerProperties = new WriterPropertiesBuilder()
///   .setCompression(Compression.SNAPPY)
///   .build();
/// const parquetUint8Array = writeParquet(arrowUint8Array, writerProperties);
/// ```
///
/// If `writerProperties` is not provided or is `null`, the default writer properties will be used.
/// This is equivalent to `new WriterPropertiesBuilder().build()`.
///
/// @param arrow_file Uint8Array containing Arrow data in [IPC Stream format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format). If you have an Arrow table in JS, call `tableToIPC(table)` in the JS bindings and pass the result here.
/// @param writer_properties Configuration for writing to Parquet. Use the {@linkcode WriterPropertiesBuilder} to build a writing configuration, then call `.build()` to create an immutable writer properties to pass in here.
/// @returns Uint8Array containing written Parquet data.
#[wasm_bindgen(js_name = writeParquet)]
#[cfg(feature = "writer")]
pub fn write_parquet(
    arrow_file: &[u8],
    writer_properties: Option<crate::arrow1::writer_properties::WriterProperties>,
) -> WasmResult<Uint8Array> {
    let writer_props = writer_properties.unwrap_or_else(|| {
        crate::arrow1::writer_properties::WriterPropertiesBuilder::default().build()
    });

    let buffer = crate::arrow1::writer::write_parquet(arrow_file, writer_props)?;
    copy_vec_to_uint8_array(buffer)
}

#[wasm_bindgen(js_name = readParquetAsync)]
#[cfg(all(feature = "reader", feature = "async"))]
pub async fn read_parquet_async(url: String) -> WasmResult<Uint8Array> {
    let buffer = crate::arrow1::reader_async::read_parquet(url).await?;
    copy_vec_to_uint8_array(buffer)
}


#[wasm_bindgen(js_name = "ParquetReader")]
#[cfg(all(feature = "reader", feature = "async"))]
#[derive(Clone)]
pub struct JsParquetReader {
    url: String,
    content_length: Option<u32>,
}

#[wasm_bindgen(js_class = "ParquetReader")]
impl JsParquetReader {

    #[wasm_bindgen(constructor)]
    pub fn new(url: String, content_length: u32) -> Self {
        Self { url, content_length: Some(content_length)}
    }

    pub async fn pull(&mut self, _controller: web_sys::ReadableStreamDefaultController) {
    }
    pub async fn start(&mut self, _controller: web_sys::ReadableStreamDefaultController) -> WasmResult<bool> {
        let content_length = usize::try_from(self.content_length.unwrap()).unwrap();
        let reader = create_reader(self.url.clone(), content_length, None);
        let builder = ParquetRecordBatchStreamBuilder::new(reader.compat()).await?;
        let arrow_schema = builder.schema().clone();
        let parquet_reader = builder.build()?;
        let mut sink = AsyncReadableStreamSink::from(_controller);
        // flow: fetch -> parquet reader stream -> ipc writer sink -> ReadableStream sink
        // let mut writer = AsyncStreamWriter::new(&sink, &arrow_schema);
        // parquet_reader.forward(writer);
        // writer.forward(sink);
        let mut intermediate_stream = parquet_reader.map(|maybe_record_batch| {
            let record_batch = maybe_record_batch.unwrap();
            let mut intermediate_vec = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut intermediate_vec, &arrow_schema.clone()).unwrap();
                let _ = writer.write(&record_batch);
                // writer.close();

            }
            Ok::<Vec<u8>, std::io::Error>(intermediate_vec)
        });
        while let Some(maybe_chunk) = intermediate_stream.next().await {
            let chunk = maybe_chunk?;
            sink.write(&chunk).await?;
        }

        let _ = sink.close().await;
        Ok(true)
    }
    pub async fn cancel(&mut self, _controller: web_sys::ReadableStreamDefaultController) {

    }

    pub fn stream(&self) -> web_sys::ReadableStream {
        let wrapper: Object = Into::<JsValue>::into(self.clone()).into();
        let stream = web_sys::ReadableStream::new_with_underlying_source(&wrapper).unwrap();
        stream
    }
}