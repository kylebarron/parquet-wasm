use std::cell::RefCell;
use std::rc::Rc;

use crate::arrow1::error::WasmResult;
use crate::utils::{assert_parquet_file_not_empty, copy_vec_to_uint8_array};
use js_sys::{Object, Uint8Array};
use std::io::Write;
use wasm_bindgen::prelude::*;

use super::ffi::FFIArrowRecordBatch;

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
pub fn read_parquet(parquet_file: Vec<u8>) -> WasmResult<Vec<u8>> {
    assert_parquet_file_not_empty(parquet_file.as_slice())?;
    Ok(crate::arrow1::reader::read_parquet(parquet_file)?)
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
) -> WasmResult<Vec<u8>> {
    let writer_props = writer_properties.unwrap_or_else(|| {
        crate::arrow1::writer_properties::WriterPropertiesBuilder::default().build()
    });

    Ok(crate::arrow1::writer::write_parquet(
        arrow_file,
        writer_props,
    )?)
}

#[wasm_bindgen(js_name = readParquetAsync)]
#[cfg(all(feature = "reader", feature = "async"))]
pub async fn read_parquet_async(url: String) -> WasmResult<Uint8Array> {
    let buffer = crate::arrow1::reader_async::read_parquet(url).await?;
    copy_vec_to_uint8_array(&buffer)
}
#[wasm_bindgen(js_name = "ParquetReader")]
#[cfg(all(feature = "reader", feature = "async"))]
#[derive(Clone)]
pub struct JsParquetReader {
    url: String,
    content_length: Option<u32>,
    _stream: Option<Rc<futures::lock::Mutex<crate::arrow1::reader_async::BoxedVecStream>>>,
}

#[wasm_bindgen(js_class = "ParquetReader")]
#[cfg(all(feature = "reader", feature = "async"))]
impl JsParquetReader {
    #[wasm_bindgen(constructor)]
    pub fn new(url: String, content_length: u32) -> Self {
        Self {
            url,
            content_length: Some(content_length),
            _stream: None,
        }
    }

    pub async fn pull(
        &mut self,
        controller: web_sys::ReadableStreamDefaultController,
    ) -> WasmResult<bool> {
        use crate::common::stream::ReadableStreamSink;
        use futures::StreamExt;
        // store a mutable stream ref on this struct.
        // in theory, we could just dispense with polling at all and offload
        // the scheduling problem on the consuming context.
        // However, it is generally a good idea to enqueue as many items as the sink
        // is willing to accept.
        // Both approaches involve crossing the JS-WASM boundary, how many though?
        // Start push with backpressure: 1 per chunk (enqueue) + 1-2 per timer.
        // At least 1 timer per highwater mark.
        // Pull (with restraint): 1 per enqueue (which can be equal to the highwater mark) +
        // 1 per pull.
        let mut unwrapped_stream = self._stream.as_deref().unwrap().lock().await;
        let desired_count = controller.desired_size().unwrap() as u32;
        let mut wrapped_controller = ReadableStreamSink::from(controller);
        for _ in 0..desired_count {
            let chunk = unwrapped_stream.next().await;
            if let Some(Ok(chunk)) = chunk {
                let _ = wrapped_controller.write(&chunk);
            } else {
                wrapped_controller.close();
            }
        }
        Ok(true)
    }
    pub async fn start(
        &mut self,
        _controller: web_sys::ReadableStreamDefaultController,
    ) -> WasmResult<bool> {
        use crate::arrow1::reader_async::read_parquet_stream;
        let content_length = usize::try_from(self.content_length.unwrap()).unwrap();
        let intermediate_stream = read_parquet_stream(self.url.clone(), content_length).await?;
        self._stream = Some(Rc::new(futures::lock::Mutex::new(intermediate_stream)));
        Ok(true)
    }
    pub async fn cancel(&mut self, _controller: web_sys::ReadableStreamDefaultController) {}

    pub fn stream(&self) -> web_sys::ReadableStream {
        let wrapper: Object = Into::<JsValue>::into(self.clone()).into();

        web_sys::ReadableStream::new_with_underlying_source(&wrapper).unwrap()
    }
}
#[wasm_bindgen(js_name = "FFIStreamReader")]
#[cfg(all(feature = "reader", feature = "async"))]
#[derive(Clone)]
pub struct JsFFIStreamReader {
    url: String,
    content_length: Option<u32>,
    _stream: Option<Rc<futures::lock::Mutex<crate::arrow1::reader_async::BoxedFFIStream>>>,
}
#[wasm_bindgen(js_class = "FFIStreamReader")]
#[cfg(all(feature = "reader", feature = "async"))]
impl JsFFIStreamReader {
    #[wasm_bindgen(constructor)]
    pub fn new(url: String, content_length: u32) -> Self {
        Self {
            url,
            content_length: Some(content_length),
            _stream: None,
        }
    }

    pub async fn pull(
        &mut self,
        controller: web_sys::ReadableStreamDefaultController,
    ) -> WasmResult<bool> {
        use futures::StreamExt;
        let mut unwrapped_stream = self._stream.as_deref().unwrap().lock().await;
        let desired_count = controller.desired_size().unwrap() as u32;
        for _ in 0..desired_count {
            let chunk = unwrapped_stream.next().await;
            if let Some(chunk) = chunk {
                let _ = controller.enqueue_with_chunk(&chunk.into());
            } else {
                let _ = controller.close();
            }
        }
        Ok(true)
    }
    pub async fn start(
        &mut self,
        _controller: web_sys::ReadableStreamDefaultController,
    ) -> WasmResult<bool> {
        use crate::arrow1::reader_async::read_ffi_stream;
        let content_length = usize::try_from(self.content_length.unwrap()).unwrap();
        let intermediate_stream = read_ffi_stream(self.url.clone(), content_length).await?;
        self._stream = Some(Rc::new(futures::lock::Mutex::new(intermediate_stream)));
        Ok(true)
    }
    pub async fn cancel(&mut self, _controller: web_sys::ReadableStreamDefaultController) {}

    pub fn stream(&self) -> web_sys::ReadableStream {
        let wrapper: Object = Into::<JsValue>::into(self.clone()).into();

        web_sys::ReadableStream::new_with_underlying_source(&wrapper).unwrap()
    }
}