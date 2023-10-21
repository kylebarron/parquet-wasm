use crate::arrow2::error::WasmResult;
use crate::utils::assert_parquet_file_not_empty;
use arrow_wasm::arrow2::{RecordBatch, Table};
use wasm_bindgen::prelude::*;

/// Read a Parquet file into Arrow data using the [`arrow2`](https://crates.io/crates/arrow2) and
/// [`parquet2`](https://crates.io/crates/parquet2) Rust crates.
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
/// import { readParquet } from "parquet-wasm/node/arrow2";
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
pub fn read_parquet(parquet_file: &[u8]) -> WasmResult<Table> {
    assert_parquet_file_not_empty(parquet_file)?;
    Ok(crate::arrow2::reader::read_parquet(parquet_file)?)
}

/// Read metadata from a Parquet file using the [`arrow2`](https://crates.io/crates/arrow2) and
/// [`parquet2`](https://crates.io/crates/parquet2) Rust crates.
///
/// Example:
///
/// ```js
/// // Edit the `parquet-wasm` import as necessary
/// import { readMetadata } from "parquet-wasm/node/arrow2";
///
/// const resp = await fetch("https://example.com/file.parquet");
/// const parquetUint8Array = new Uint8Array(await resp.arrayBuffer());
/// const parquetFileMetaData = readMetadata(parquetUint8Array);
/// ```
///
/// @param parquet_file Uint8Array containing Parquet data
/// @returns a {@linkcode FileMetaData} object containing metadata of the Parquet file.
#[wasm_bindgen(js_name = readMetadata)]
#[cfg(feature = "reader")]
pub fn read_metadata(parquet_file: &[u8]) -> WasmResult<crate::arrow2::metadata::FileMetaData> {
    assert_parquet_file_not_empty(parquet_file)?;

    let metadata = crate::arrow2::reader::read_metadata(parquet_file)?;
    Ok(metadata.into())
}

/// Read a single row group from a Parquet file into Arrow data using the
/// [`arrow2`](https://crates.io/crates/arrow2) and [`parquet2`](https://crates.io/crates/parquet2)
/// Rust crates.
///
/// This returns an Arrow record batch in WebAssembly memory. To transfer the Arrow record batch to
/// JavaScript memory you have two options:
///
/// - (Easier): Call {@linkcode RecordBatch.intoIPCStream} to construct a buffer that can be parsed
///   with Arrow JS's `tableFromIPC` function.
/// - (More performant but bleeding edge): Call {@linkcode RecordBatch.intoFFI} to construct a data
///   representation that can be parsed zero-copy from WebAssembly with
///   [arrow-js-ffi](https://github.com/kylebarron/arrow-js-ffi).
///
/// Example:
///
/// ```js
/// import { tableFromIPC } from "apache-arrow";
/// // Edit the `parquet-wasm` import as necessary
/// import { readRowGroup, readMetadata } from "parquet-wasm/node/arrow2";
///
/// const resp = await fetch("https://example.com/file.parquet");
/// const parquetUint8Array = new Uint8Array(await resp.arrayBuffer());
/// const parquetFileMetaData = readMetadata(parquetUint8Array);
///
/// const arrowSchema = parquetFileMetaData.arrowSchema();
/// // Read only the first row group
/// const parquetRowGroupMeta = parquetFileMetaData.rowGroup(0);
///
/// // Read only the first row group
/// const arrowWasmBatch = readRowGroup(
///   parquetUint8Array,
///   arrowSchema,
///   parquetRowGroupMeta
/// );
/// const arrowJsTable = tableFromIPC(arrowWasmBatch.intoIPCStream());
/// // This table will only have one batch
/// const arrowJsRecordBatch = arrowJsTable.batches[0];
/// ```
///
/// Note that you can get the number of row groups in a Parquet file using {@linkcode FileMetaData.numRowGroups}
///
/// @param parquet_file Uint8Array containing Parquet data
/// @param schema Use {@linkcode FileMetaData.arrowSchema} to create.
/// @param meta {@linkcode RowGroupMetaData} from a call to {@linkcode readMetadata}
#[wasm_bindgen(js_name = readRowGroup)]
#[cfg(feature = "reader")]
pub fn read_row_group(
    parquet_file: &[u8],
    schema: &crate::arrow2::schema::ArrowSchema,
    meta: &crate::arrow2::metadata::RowGroupMetaData,
) -> WasmResult<RecordBatch> {
    assert_parquet_file_not_empty(parquet_file)?;

    let record_batch = crate::arrow2::reader::read_row_group(
        parquet_file,
        schema.clone().into(),
        meta.clone().into(),
    )?;
    Ok(record_batch)
}

/// Asynchronously read metadata from a Parquet file using the
/// [`arrow2`](https://crates.io/crates/arrow2) and [`parquet2`](https://crates.io/crates/parquet2)
/// Rust crates.
///
/// For now, this requires knowing the content length of the file, but hopefully this will be
/// relaxed in the future. If you don't know the contentLength of the file, this will perform a
/// HEAD request to do so.
///
/// Example:
///
/// ```js
/// // Edit the `parquet-wasm` import as necessary
/// import { readMetadataAsync } from "parquet-wasm";
///
/// const parquetFileMetaData = await readMetadataAsync(url, contentLength);
/// ```
///
/// @param url String location of remote Parquet file containing Parquet data
/// @param content_length Number content length of file in bytes
/// @returns a {@linkcode FileMetaData} object containing metadata of the Parquet file.
#[wasm_bindgen(js_name = readMetadataAsync)]
#[cfg(all(feature = "reader", feature = "async"))]
pub async fn read_metadata_async(
    url: String,
    content_length: Option<usize>,
) -> WasmResult<crate::arrow2::metadata::FileMetaData> {
    let metadata = crate::arrow2::reader_async::read_metadata_async(url, content_length).await?;
    Ok(metadata.into())
}

/// Asynchronously read a single row group from a Parquet file into Arrow data using the
/// [`arrow2`](https://crates.io/crates/arrow2) and [`parquet2`](https://crates.io/crates/parquet2)
/// Rust crates.
///
/// Example:
///
/// ```ts
/// import * as arrowJs from "apache-arrow";
/// // Edit the `parquet-wasm` import as necessary
/// import {
///   readRowGroupAsync,
///   readMetadataAsync,
///   RecordBatch,
/// } from "parquet-wasm/node/arrow2";
///
/// const url = "https://example.com/file.parquet";
/// const headResp = await fetch(url, { method: "HEAD" });
/// const length = parseInt(headResp.headers.get("Content-Length"));
///
/// const parquetFileMetaData = await readMetadataAsync(url, length);
/// const arrowSchema = parquetFileMetaData.arrowSchema();
///
/// // Read all batches from the file in parallel
/// const promises: Promise<RecordBatch>[] = [];
/// for (let i = 0; i < parquetFileMetaData.numRowGroups(); i++) {
///   const rowGroupMeta = parquetFileMetaData.rowGroup(i);
///   const rowGroupPromise = readRowGroupAsync(url, rowGroupMeta, arrowSchema);
///   promises.push(rowGroupPromise);
/// }
///
/// // Collect the per-batch requests
/// const wasmRecordBatchChunks = await Promise.all(promises);
///
/// // Parse the wasm record batches into JS record batches
/// const jsRecordBatchChunks: arrowJs.RecordBatch[] = [];
/// for (const wasmRecordBatch of wasmRecordBatchChunks) {
///   const arrowJsTable = arrowJs.tableFromIPC(wasmRecordBatch.intoIPCStream());
///   // This should never throw
///   if (arrowJsTable.batches.length > 1) throw new Error();
///   const arrowJsRecordBatch = arrowJsTable.batches[0];
///   jsRecordBatchChunks.push(arrowJsRecordBatch);
/// }
///
/// // Concatenate the JS record batches into a table
/// const jsTable = new arrowJs.Table(recordBatchChunks);
/// ```
///
/// Note that you can get the number of row groups in a Parquet file using {@linkcode FileMetaData.numRowGroups}
///
/// @param url String location of remote Parquet file containing Parquet data
/// @param schema Use {@linkcode FileMetaData.arrowSchema} to create.
/// @param meta {@linkcode RowGroupMetaData} from a call to {@linkcode readMetadataAsync}
#[wasm_bindgen(js_name = readRowGroupAsync)]
#[cfg(all(feature = "reader", feature = "async"))]
pub async fn read_row_group_async(
    url: String,
    row_group_meta: &crate::arrow2::metadata::RowGroupMetaData,
    arrow_schema: &crate::arrow2::schema::ArrowSchema,
) -> WasmResult<RecordBatch> {
    let record_batch = crate::arrow2::reader_async::read_row_group(
        url,
        &row_group_meta.clone().into(),
        &arrow_schema.clone().into(),
    )
    .await?;
    Ok(record_batch)
}

/// Write Arrow data to a Parquet file using the [`arrow2`](https://crates.io/crates/arrow2) and
/// [`parquet2`](https://crates.io/crates/parquet2) Rust crates.
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
/// } from "parquet-wasm/node/arrow2";
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
///   WriterPropertiesBuilder} to build a writing configuration, then call `.build()` to create an
///   immutable writer properties to pass in here.
/// @returns Uint8Array containing written Parquet data.
#[wasm_bindgen(js_name = writeParquet)]
#[cfg(feature = "writer")]
pub fn write_parquet(
    table: Table,
    writer_properties: Option<crate::arrow2::writer_properties::WriterProperties>,
) -> WasmResult<Vec<u8>> {
    let (schema, chunks) = table.into_inner();
    Ok(crate::arrow2::writer::write_parquet(
        chunks.into_iter(),
        schema,
        writer_properties.unwrap_or_default(),
    )?)
}

#[wasm_bindgen(js_name = readParquetStream)]
#[cfg(all(feature = "reader", feature = "async"))]
pub async fn read_parquet_stream(
    url: String,
) -> WasmResult<wasm_streams::readable::sys::ReadableStream> {
    use futures::StreamExt;
    let stream = super::reader_async::read_record_batch_stream(url)
        .await?
        .map(|batch| Ok(batch.into()));
    Ok(wasm_streams::ReadableStream::from_stream(stream).into_raw())
}
