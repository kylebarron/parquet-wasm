use crate::arrow2::error::WasmResult;
use crate::arrow2::ffi::FFIArrowTable;
use crate::utils::{assert_parquet_file_not_empty, copy_vec_to_uint8_array};
use js_sys::Uint8Array;
use wasm_bindgen::prelude::*;

/// Read a Parquet file into Arrow data using the [`arrow2`](https://crates.io/crates/arrow2) and
/// [`parquet2`](https://crates.io/crates/parquet2) Rust crates.
///
/// Example:
///
/// ```js
/// import { tableFromIPC } from "apache-arrow";
/// // Edit the `parquet-wasm` import as necessary
/// import { readParquet } from "parquet-wasm/node2";
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
pub fn read_parquet(parquet_file: &[u8]) -> WasmResult<Vec<u8>> {
    assert_parquet_file_not_empty(parquet_file)?;
    Ok(crate::arrow2::reader::read_parquet(
        parquet_file,
        |chunk| chunk,
    )?)
}

/// Read a Parquet file into Arrow FFI structs using the
/// [`arrow2`](https://crates.io/crates/arrow2) and [`parquet2`](https://crates.io/crates/parquet2)
/// Rust crates.
///
/// This API is less well tested than the "normal" `readParquet` API, but should be faster and have
/// **much** less memory overhead (by a factor of 2). If you hit any bugs, please create a
/// reproducible issue at <https://github.com/kylebarron/parquet-wasm/issues/new>.
///
/// ## Background
///
/// Under the hood, `parquet-wasm` first decodes a Parquet file into Arrow _in WebAssembly memory_.
/// But then that WebAssembly memory needs to be copied into JavaScript for use by Arrow JS. The
/// "normal" read APIs (e.g. `readParquet`) use the [Arrow IPC
/// format](https://arrow.apache.org/docs/python/ipc.html) to get the data back to JavaScript. But
/// this requires another memory copy _inside WebAssembly_ to assemble the various arrays into a
/// single buffer to be copied back to JS.
///
/// Instead, this API uses Arrow's [C Data
/// Interface](https://arrow.apache.org/docs/format/CDataInterface.html) to be able to copy or view
/// Arrow arrays from within WebAssembly memory without any serialization.
///
/// I wrote an [interactive blog
/// post](https://observablehq.com/@kylebarron/zero-copy-apache-arrow-with-webassembly) on this
/// approach and the Arrow C Data Interface if you want to read more!
///
/// ## Caveats
///
/// This requires you to use [`arrow-js-ffi`](https://github.com/kylebarron/arrow-js-ffi) to parse
/// the Arrow C Data Interface definitions. This library has not yet been tested in production, so
/// it may have bugs!
///
/// ## Example:
///
/// ```js
/// import { Table } from "apache-arrow";
/// import { parseRecordBatch } from "arrow-js-ffi";
/// // Edit the `parquet-wasm` import as necessary
/// import { readParquetFFI, __wasm } from "parquet-wasm/node2";
///
/// // A reference to the WebAssembly memory object. The way to access this is different for each
/// // environment. In Node, use the __wasm export as shown below. In ESM the memory object will
/// // be found on the returned default export.
/// const WASM_MEMORY = __wasm.memory;
///
/// const resp = await fetch("https://example.com/file.parquet");
/// const parquetUint8Array = new Uint8Array(await resp.arrayBuffer());
/// const wasmArrowTable = readParquetFFI(parquetUint8Array);
///
/// const recordBatches = [];
/// for (let i = 0; i < wasmArrowTable.numBatches(); i++) {
///   // Note: Unless you know what you're doing, setting `true` below is recommended to _copy_
///   // table data from WebAssembly into JavaScript memory. This may become the default in the
///   // future.
///   const recordBatch = parseRecordBatch(
///     WASM_MEMORY.buffer,
///     wasmArrowTable.arrayAddr(i),
///     wasmArrowTable.schemaAddr(),
///     true
///   );
///   recordBatches.push(recordBatch);
/// }
///
/// const table = new Table(recordBatches);
///
/// // VERY IMPORTANT! You must call `drop` on the Wasm table object when you're done using it
/// // to release the Wasm memory.
/// // Note that any access to the pointers in this table is undefined behavior after this call.
/// // Calling any `wasmArrowTable` method will error.
/// wasmArrowTable.drop();
/// ```
///
/// @param parquet_file Uint8Array containing Parquet data
/// @returns an {@linkcode FFIArrowTable} object containing the parsed Arrow table in WebAssembly memory. To read into an Arrow JS table, you'll need to use the Arrow C Data interface.
#[wasm_bindgen(js_name = readParquetFFI)]
#[cfg(feature = "reader")]
pub fn read_parquet_ffi(parquet_file: &[u8]) -> WasmResult<FFIArrowTable> {
    assert_parquet_file_not_empty(parquet_file)?;
    Ok(crate::arrow2::reader::read_parquet_ffi(
        parquet_file,
        |chunk| chunk,
    )?)
}

/// Read metadata from a Parquet file using the [`arrow2`](https://crates.io/crates/arrow2) and
/// [`parquet2`](https://crates.io/crates/parquet2) Rust crates.
///
/// Example:
///
/// ```js
/// // Edit the `parquet-wasm` import as necessary
/// import { readMetadata } from "parquet-wasm/node2";
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
/// Example:
///
/// ```js
/// import { tableFromIPC } from "apache-arrow";
/// // Edit the `parquet-wasm` import as necessary
/// import { readRowGroup, readMetadata } from "parquet-wasm/node2";
///
/// const resp = await fetch("https://example.com/file.parquet");
/// const parquetUint8Array = new Uint8Array(await resp.arrayBuffer());
/// const parquetFileMetaData = readMetadata(parquetUint8Array);
///
/// // Read only the first row group
/// const arrowIpcBuffer = wasm.readRowGroup(parquetUint8Array, parquetFileMetaData, 0);
/// const arrowTable = tableFromIPC(arrowUint8Array);
/// ```
///
/// Note that you can get the number of row groups in a Parquet file using {@linkcode FileMetaData.numRowGroups}
///
/// @param parquet_file Uint8Array containing Parquet data
/// @param meta {@linkcode FileMetaData} from a call to {@linkcode readMetadata}
/// @param i Number index of the row group to parse
/// @returns Uint8Array containing Arrow data in [IPC Stream format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format). To parse this into an Arrow table, pass to `tableFromIPC` in the Arrow JS bindings.
#[wasm_bindgen(js_name = readRowGroup)]
#[cfg(feature = "reader")]
pub fn read_row_group(
    parquet_file: &[u8],
    schema: &crate::arrow2::schema::ArrowSchema,
    meta: &crate::arrow2::metadata::RowGroupMetaData,
) -> WasmResult<Vec<u8>> {
    assert_parquet_file_not_empty(parquet_file)?;

    let buffer = crate::arrow2::reader::read_row_group(
        parquet_file,
        schema.clone().into(),
        meta.clone().into(),
        |chunk| chunk,
    )?;
    Ok(buffer)
}

/// Asynchronously read metadata from a Parquet file using the
/// [`arrow2`](https://crates.io/crates/arrow2) and [`parquet2`](https://crates.io/crates/parquet2)
/// Rust crates.
///
/// For now, this requires knowing the content length of the file, but hopefully this will be
/// relaxed in the future.
///
/// Example:
///
/// ```js
/// // Edit the `parquet-wasm` import as necessary
/// import { readMetadataAsync } from "parquet-wasm";
///
/// const parquetFileMetaData = await readMetadataAsync(url);
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
/// ```js
/// import { tableFromIPC } from "apache-arrow";
/// // Edit the `parquet-wasm` import as necessary
/// import { readRowGroupAsync, readMetadataAsync } from "parquet-wasm";
///
/// const url = "https://example.com/file.parquet";
/// const headResp = await fetch(url, {method: 'HEAD'});
/// const length = parseInt(headResp.headers.get('Content-Length'));
///
/// const parquetFileMetaData = await readMetadataAsync(url, length);
///
/// // Read all batches from the file in parallel
/// const promises = [];
/// for (let i = 0; i < parquetFileMetaData.numRowGroups(); i++) {
///   // IMPORTANT: For now, calling `copy()` on the metadata object is required whenever passing in to
///   // a function. Hopefully this can be resolved in the future sometime
///   const rowGroupPromise = wasm.readRowGroupAsync(url, metadata.copy().rowGroup(i));
///   promises.push(rowGroupPromise);
/// }
///
/// const recordBatchChunks = await Promise.all(promises);
/// const table = new arrow.Table(recordBatchChunks);
/// ```
///
/// Note that you can get the number of row groups in a Parquet file using {@linkcode FileMetaData.numRowGroups}
///
/// @param url String location of remote Parquet file containing Parquet data
/// @param content_length Number content length of file in bytes
/// @param meta {@linkcode FileMetaData} from a call to {@linkcode readMetadata}
/// @param i Number index of the row group to load
/// @returns Uint8Array containing Arrow data in [IPC Stream format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format). To parse this into an Arrow table, pass to `tableFromIPC` in the Arrow JS bindings.

// TODO: update these docs!
#[wasm_bindgen(js_name = readRowGroupAsync)]
#[cfg(all(feature = "reader", feature = "async"))]
pub async fn read_row_group_async(
    url: String,
    row_group_meta: &crate::arrow2::metadata::RowGroupMetaData,
    arrow_schema: &crate::arrow2::schema::ArrowSchema,
) -> WasmResult<Uint8Array> {
    let buffer = crate::arrow2::reader_async::read_row_group(
        url,
        &row_group_meta.clone().into(),
        &arrow_schema.clone().into(),
        |chunk| chunk,
    )
    .await?;
    copy_vec_to_uint8_array(&buffer)
}

/// Write Arrow data to a Parquet file using the [`arrow2`](https://crates.io/crates/arrow2) and
/// [`parquet2`](https://crates.io/crates/parquet2) Rust crates.
///
/// For example, to create a Parquet file with Snappy compression:
///
/// ```js
/// import { tableToIPC } from "apache-arrow";
/// // Edit the `parquet-wasm` import as necessary
/// import { WriterPropertiesBuilder, Compression, writeParquet } from "parquet-wasm/node2";
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
/// @param arrow_file Uint8Array containing Arrow data in [IPC **File** format](https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format). If you have an Arrow table in JS, call `tableToIPC(table, "file")` in the JS bindings and pass the result here.
/// @param writer_properties Configuration for writing to Parquet. Use the {@linkcode WriterPropertiesBuilder} to build a writing configuration, then call `.build()` to create an immutable writer properties to pass in here.
/// @returns Uint8Array containing written Parquet data.
#[wasm_bindgen(js_name = writeParquet)]
#[cfg(feature = "writer")]
pub fn write_parquet(
    arrow_file: &[u8],
    writer_properties: Option<crate::arrow2::writer_properties::WriterProperties>,
) -> WasmResult<Vec<u8>> {
    let writer_props = writer_properties.unwrap_or_else(|| {
        crate::arrow2::writer_properties::WriterPropertiesBuilder::default().build()
    });

    Ok(crate::arrow2::writer::write_parquet(
        arrow_file,
        writer_props,
    )?)
}

/// Write Arrow data to a Parquet file using the [`arrow2`](https://crates.io/crates/arrow2) and
/// [`parquet2`](https://crates.io/crates/parquet2) Rust crates.
///
/// For example, to create a Parquet file with Snappy compression:
///
/// ```js
/// import { tableToIPC } from "apache-arrow";
/// // Edit the `parquet-wasm` import as necessary
/// import { WriterPropertiesBuilder, Compression, _writeParquetFFI } from "parquet-wasm/node2";
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
/// @param arrow_table {@linkcode FFIArrowTable} Arrow Table in Wasm memory
/// @param writer_properties Configuration for writing to Parquet. Use the {@linkcode WriterPropertiesBuilder} to build a writing configuration, then call `.build()` to create an immutable writer properties to pass in here.
/// @returns Uint8Array containing written Parquet data.
#[wasm_bindgen(js_name = _writeParquetFFI)]
#[cfg(feature = "writer")]
pub fn write_parquet_ffi(
    arrow_table: FFIArrowTable,
    writer_properties: Option<crate::arrow2::writer_properties::WriterProperties>,
) -> WasmResult<Vec<u8>> {
    let writer_props = writer_properties.unwrap_or_else(|| {
        crate::arrow2::writer_properties::WriterPropertiesBuilder::default().build()
    });

    Ok(crate::arrow2::writer::write_ffi_table_to_parquet(
        arrow_table,
        writer_props,
    )?)
}

#[wasm_bindgen(js_name = readFFIStream)]
#[cfg(all(feature = "reader", feature = "async"))]
pub async fn read_ffi_stream(
    url: String,
) -> WasmResult<wasm_streams::readable::sys::ReadableStream> {
    use futures::StreamExt;
    let stream = super::reader_async::read_record_batch_stream(url)
        .await?
        .map(|batch| Ok(batch.into()));
    Ok(wasm_streams::ReadableStream::from_stream(stream).into_raw())
}
