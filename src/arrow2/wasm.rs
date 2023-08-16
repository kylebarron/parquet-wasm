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

/// Read a Parquet file into Arrow data using the [`arrow2`](https://crates.io/crates/arrow2) and
/// [`parquet2`](https://crates.io/crates/parquet2) Rust crates.
///
/// Example:
///
/// ```js
/// import { tableFromIPC } from "apache-arrow";
/// // Edit the `parquet-wasm` import as necessary
/// import { _readParquetFFI } from "parquet-wasm/node2";
///
/// const resp = await fetch("https://example.com/file.parquet");
/// const parquetUint8Array = new Uint8Array(await resp.arrayBuffer());
/// const wasmArrowTable = _readParquetFFI(parquetUint8Array);
/// // Pointer to the ArrowArray FFI struct for the first record batch and first column
/// const arrayPtr = wasmArrowTable.array(0, 0);
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
