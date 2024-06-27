use crate::error::WasmResult;
#[cfg(feature = "reader")]
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
///   [arrow-js-ffi](https://github.com/kylebarron/arrow-js-ffi) using `parseTable`.
///
/// Example with IPC stream:
///
/// ```js
/// import { tableFromIPC } from "apache-arrow";
/// import initWasm, {readParquet} from "parquet-wasm";
///
/// // Instantiate the WebAssembly context
/// await initWasm();
///
/// const resp = await fetch("https://example.com/file.parquet");
/// const parquetUint8Array = new Uint8Array(await resp.arrayBuffer());
/// const arrowWasmTable = readParquet(parquetUint8Array);
/// const arrowTable = tableFromIPC(arrowWasmTable.intoIPCStream());
/// ```
///
/// Example with `arrow-js-ffi`:
///
/// ```js
/// import { parseTable } from "arrow-js-ffi";
/// import initWasm, {readParquet, wasmMemory} from "parquet-wasm";
///
/// // Instantiate the WebAssembly context
/// await initWasm();
/// const WASM_MEMORY = wasmMemory();
///
/// const resp = await fetch("https://example.com/file.parquet");
/// const parquetUint8Array = new Uint8Array(await resp.arrayBuffer());
/// const arrowWasmTable = readParquet(parquetUint8Array);
/// const ffiTable = arrowWasmTable.intoFFI();
/// const arrowTable = parseTable(
///   WASM_MEMORY.buffer,
///   ffiTable.arrayAddrs(),
///   ffiTable.schemaAddr()
/// );
/// ```
///
/// @param parquet_file Uint8Array containing Parquet data
/// @param options
///
///    Options for reading Parquet data. Optional keys include:
///
///    - `batchSize`: The number of rows in each batch. If not provided, the upstream parquet
///           default is 1024.
///    - `rowGroups`: Only read data from the provided row group indexes.
///    - `limit`: Provide a limit to the number of rows to be read.
///    - `offset`: Provide an offset to skip over the given number of rows.
///    - `columns`: The column names from the file to read.
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
///
/// This returns an Arrow schema in WebAssembly memory. To transfer the Arrow schema to JavaScript
/// memory you have two options:
///
/// - (Easier): Call {@linkcode Schema.intoIPCStream} to construct a buffer that can be parsed with
///   Arrow JS's `tableFromIPC` function. This results in an Arrow JS Table with zero rows but a
///   valid schema.
/// - (More performant but bleeding edge): Call {@linkcode Schema.intoFFI} to construct a data
///   representation that can be parsed zero-copy from WebAssembly with
///   [arrow-js-ffi](https://github.com/kylebarron/arrow-js-ffi) using `parseSchema`.
///
/// Example with IPC Stream:
///
/// ```js
/// import { tableFromIPC } from "apache-arrow";
/// import initWasm, {readSchema} from "parquet-wasm";
///
/// // Instantiate the WebAssembly context
/// await initWasm();
///
/// const resp = await fetch("https://example.com/file.parquet");
/// const parquetUint8Array = new Uint8Array(await resp.arrayBuffer());
/// const arrowWasmSchema = readSchema(parquetUint8Array);
/// const arrowTable = tableFromIPC(arrowWasmSchema.intoIPCStream());
/// const arrowSchema = arrowTable.schema;
/// ```
///
/// Example with `arrow-js-ffi`:
///
/// ```js
/// import { parseSchema } from "arrow-js-ffi";
/// import initWasm, {readSchema, wasmMemory} from "parquet-wasm";
///
/// // Instantiate the WebAssembly context
/// await initWasm();
/// const WASM_MEMORY = wasmMemory();
///
/// const resp = await fetch("https://example.com/file.parquet");
/// const parquetUint8Array = new Uint8Array(await resp.arrayBuffer());
/// const arrowWasmSchema = readSchema(parquetUint8Array);
/// const ffiSchema = arrowWasmSchema.intoFFI();
/// const arrowTable = parseSchema(WASM_MEMORY.buffer, ffiSchema.addr());
/// const arrowSchema = arrowTable.schema;
/// ```
///
/// @param parquet_file Uint8Array containing Parquet data
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
/// import initWasm, {
///   Table,
///   WriterPropertiesBuilder,
///   Compression,
///   writeParquet,
/// } from "parquet-wasm";
///
/// // Instantiate the WebAssembly context
/// await initWasm();
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

/// Read a Parquet file into a stream of Arrow `RecordBatch`es.
///
/// This returns a ReadableStream containing RecordBatches in WebAssembly memory. To transfer the
/// Arrow table to JavaScript memory you have two options:
///
/// - (Easier): Call {@linkcode RecordBatch.intoIPCStream} to construct a buffer that can be parsed
///   with Arrow JS's `tableFromIPC` function. (The table will have a single internal record
///   batch).
/// - (More performant but bleeding edge): Call {@linkcode RecordBatch.intoFFI} to construct a data
///   representation that can be parsed zero-copy from WebAssembly with
///   [arrow-js-ffi](https://github.com/kylebarron/arrow-js-ffi) using `parseRecordBatch`.
///
/// Example with IPC stream:
///
/// ```js
/// import { tableFromIPC } from "apache-arrow";
/// import initWasm, {readParquetStream} from "parquet-wasm";
///
/// // Instantiate the WebAssembly context
/// await initWasm();
///
/// const stream = await wasm.readParquetStream(url);
///
/// const batches = [];
/// for await (const wasmRecordBatch of stream) {
///   const arrowTable = tableFromIPC(wasmRecordBatch.intoIPCStream());
///   batches.push(...arrowTable.batches);
/// }
/// const table = new arrow.Table(batches);
/// ```
///
/// Example with `arrow-js-ffi`:
///
/// ```js
/// import { parseRecordBatch } from "arrow-js-ffi";
/// import initWasm, {readParquetStream, wasmMemory} from "parquet-wasm";
///
/// // Instantiate the WebAssembly context
/// await initWasm();
/// const WASM_MEMORY = wasmMemory();
///
/// const stream = await wasm.readParquetStream(url);
///
/// const batches = [];
/// for await (const wasmRecordBatch of stream) {
///   const ffiRecordBatch = wasmRecordBatch.intoFFI();
///   const recordBatch = parseRecordBatch(
///     WASM_MEMORY.buffer,
///     ffiRecordBatch.arrayAddr(),
///     ffiRecordBatch.schemaAddr(),
///     true
///   );
///   batches.push(recordBatch);
/// }
/// const table = new arrow.Table(batches);
/// ```
///
/// @param url URL to Parquet file
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

#[wasm_bindgen(js_name = "transformParquetStream")]
#[cfg(all(feature = "writer", feature = "async"))]
pub fn transform_parquet_stream(
    stream: wasm_streams::readable::sys::ReadableStream,
    writer_properties: Option<crate::writer_properties::WriterProperties>,
) -> WasmResult<wasm_streams::readable::sys::ReadableStream> {
    use futures::StreamExt;
    use wasm_bindgen::convert::TryFromJsValue;
    let batches = wasm_streams::ReadableStream::from_raw(stream)
        .into_stream()
        .map(|maybe_chunk| {
            let chunk = maybe_chunk.unwrap();
            arrow_wasm::RecordBatch::try_from_js_value(chunk).unwrap()
        });
    let output_stream = super::writer_async::transform_parquet_stream(
        batches,
        writer_properties.unwrap_or_default(),
    );
    Ok(output_stream.unwrap())
}
