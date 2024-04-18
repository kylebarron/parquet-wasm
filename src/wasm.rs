use crate::error::WasmResult;
use crate::read_options::ReaderOptions;
use crate::utils::assert_parquet_file_not_empty;
use arrow_wasm::{RecordBatch, Schema, Table};
use wasm_bindgen::prelude::*;

/// Read a Parquet file into Arrow data.
///
/// This returns an Arrow table in WebAssembly memory. To transfer the Arrow table to JavaScript
/// memory you have two options:
///
/// - (Easier): Call {@linkcode Table.intoIPCStream} to construct a buffer that can be parsed with
///   Arrow JS's `tableFromIPC` function.
/// - (More performant but bleeding edge): Call {@linkcode Table.intoFFI} to construct a data
///   representation that can be parsed zero-copy from WebAssembly with
///   [arrow-js-ffi](https://github.com/kylebarron/arrow-js-ffi).
///
/// Example:
///
/// ```js
/// import { tableFromIPC } from "apache-arrow";
/// // Edit the `parquet-wasm` import as necessary
/// import { readParquet } from "parquet-wasm/node/arrow1";
///
/// const resp = await fetch("https://example.com/file.parquet");
/// const parquetUint8Array = new Uint8Array(await resp.arrayBuffer());
/// const arrowWasmTable = readParquet(parquetUint8Array);
/// const arrowTable = tableFromIPC(arrowWasmTable.intoIPCStream());
/// ```
///
/// @param parquet_file Uint8Array containing Parquet data
#[wasm_bindgen(js_name = readParquet)]
#[cfg(feature = "reader")]
pub fn read_parquet(parquet_file: Vec<u8>, options: Option<ReaderOptions>) -> WasmResult<Table> {
    assert_parquet_file_not_empty(parquet_file.as_slice())?;
    Ok(crate::reader::read_parquet(
        parquet_file,
        options
            .map(|x| x.try_into())
            .transpose()?
            .unwrap_or_default(),
    )?)
}

/// Read an Arrow schema from a Parquet file in memory.
#[wasm_bindgen(js_name = readSchema)]
#[cfg(feature = "reader")]
pub fn read_schema(parquet_file: Vec<u8>) -> WasmResult<Schema> {
    assert_parquet_file_not_empty(parquet_file.as_slice())?;
    Ok(crate::reader::read_schema(parquet_file)?)
}

/// Write Arrow data to a Parquet file.
///
/// For example, to create a Parquet file with Snappy compression:
///
/// ```js
/// import { tableToIPC } from "apache-arrow";
/// // Edit the `parquet-wasm` import as necessary
/// import {
///   Table,
///   WriterPropertiesBuilder,
///   Compression,
///   writeParquet,
/// } from "parquet-wasm/node/arrow1";
///
/// // Given an existing arrow JS table under `table`
/// const wasmTable = Table.fromIPCStream(tableToIPC(table, "stream"));
/// const writerProperties = new WriterPropertiesBuilder()
///   .setCompression(Compression.SNAPPY)
///   .build();
/// const parquetUint8Array = writeParquet(wasmTable, writerProperties);
/// ```
///
/// If `writerProperties` is not provided or is `null`, the default writer properties will be used.
/// This is equivalent to `new WriterPropertiesBuilder().build()`.
///
/// @param table A {@linkcode Table} representation in WebAssembly memory.
/// @param writer_properties (optional) Configuration for writing to Parquet. Use the {@linkcode
/// WriterPropertiesBuilder} to build a writing configuration, then call `.build()` to create an
/// immutable writer properties to pass in here.
/// @returns Uint8Array containing written Parquet data.
#[wasm_bindgen(js_name = writeParquet)]
#[cfg(feature = "writer")]
pub fn write_parquet(
    table: Table,
    writer_properties: Option<crate::writer_properties::WriterProperties>,
) -> WasmResult<Vec<u8>> {
    let (schema, batches) = table.into_inner();
    Ok(crate::writer::write_parquet(
        batches.into_iter(),
        schema,
        writer_properties.unwrap_or_default(),
    )?)
}

#[wasm_bindgen(js_name = readParquetStream)]
#[cfg(all(feature = "reader", feature = "async"))]
pub async fn read_parquet_stream(
    url: String,
    content_length: Option<usize>,
) -> WasmResult<wasm_streams::readable::sys::ReadableStream> {
    use futures::StreamExt;
    let parquet_stream = crate::reader_async::read_record_batch_stream(url, content_length).await?;
    let stream = parquet_stream.map(|maybe_record_batch| {
        let record_batch = maybe_record_batch.unwrap();
        Ok(RecordBatch::new(record_batch).into())
    });
    Ok(wasm_streams::ReadableStream::from_stream(stream).into_raw())
}
