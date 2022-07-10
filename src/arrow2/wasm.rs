use crate::utils::copy_vec_to_uint8_array;
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
/// import { readParquet2 } from "parquet-wasm/node2";
///
/// const resp = await fetch("https://example.com/file.parquet");
/// const parquetUint8Array = new Uint8Array(await resp.arrayBuffer());
/// const arrowUint8Array = readParquet2(parquetUint8Array);
/// const arrowTable = tableFromIPC(arrowUint8Array);
/// ```
///
/// @param parquet_file Uint8Array containing Parquet data
/// @returns Uint8Array containing Arrow data in [IPC Stream format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format). To parse this into an Arrow table, pass to `tableFromIPC` in the Arrow JS bindings.
#[wasm_bindgen(js_name = readParquet2)]
#[cfg(feature = "reader")]
pub fn read_parquet2(parquet_file: &[u8]) -> Result<Uint8Array, JsValue> {
    if parquet_file.is_empty() {
        return Err(JsValue::from_str(
            "Empty input provided or not a Uint8Array.",
        ));
    }

    match crate::arrow2::reader::read_parquet(parquet_file) {
        Ok(buffer) => copy_vec_to_uint8_array(buffer),
        Err(error) => Err(JsValue::from_str(format!("{}", error).as_str())),
    }
}

/// Read metadata from a Parquet file using the [`arrow2`](https://crates.io/crates/arrow2) and
/// [`parquet2`](https://crates.io/crates/parquet2) Rust crates.
///
/// Example:
///
/// ```js
/// // Edit the `parquet-wasm` import as necessary
/// import { readMetadata2 } from "parquet-wasm/node2";
///
/// const resp = await fetch("https://example.com/file.parquet");
/// const parquetUint8Array = new Uint8Array(await resp.arrayBuffer());
/// const parquetFileMetaData = readMetadata2(parquetUint8Array);
/// ```
///
/// @param parquet_file Uint8Array containing Parquet data
/// @returns a {@linkcode FileMetaData} object containing metadata of the Parquet file.
#[wasm_bindgen(js_name = readMetadata2)]
#[cfg(feature = "reader")]
pub fn read_metadata2(
    parquet_file: &[u8],
) -> Result<crate::arrow2::metadata::FileMetaData, JsValue> {
    if parquet_file.is_empty() {
        return Err(JsValue::from_str(
            "Empty input provided or not a Uint8Array.",
        ));
    }

    match crate::arrow2::reader::read_metadata(parquet_file) {
        Ok(metadata) => Ok(metadata.into()),
        Err(error) => Err(JsValue::from_str(format!("{}", error).as_str())),
    }
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
/// import { readRowGroup2, readMetadata2 } from "parquet-wasm/node2";
///
/// const resp = await fetch("https://example.com/file.parquet");
/// const parquetUint8Array = new Uint8Array(await resp.arrayBuffer());
/// const parquetFileMetaData = readMetadata2(parquetUint8Array);
///
/// // Read only the first row group
/// const arrowIpcBuffer = wasm.readRowGroup2(parquetUint8Array, parquetFileMetaData, 0);
/// const arrowTable = tableFromIPC(arrowUint8Array);
/// ```
///
/// Note that you can get the number of row groups in a Parquet file using {@linkcode FileMetaData.numRowGroups}
///
/// @param parquet_file Uint8Array containing Parquet data
/// @param meta {@linkcode FileMetaData} from a call to {@linkcode readMetadata2}
/// @param i Number index of the row group to parse
/// @returns Uint8Array containing Arrow data in [IPC Stream format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format). To parse this into an Arrow table, pass to `tableFromIPC` in the Arrow JS bindings.
#[wasm_bindgen(js_name = readRowGroup2)]
#[cfg(feature = "reader")]
pub fn read_row_group2(
    parquet_file: &[u8],
    meta: &crate::arrow2::metadata::FileMetaData,
    i: usize,
) -> Result<Uint8Array, JsValue> {
    if parquet_file.is_empty() {
        return Err(JsValue::from_str(
            "Empty input provided or not a Uint8Array.",
        ));
    }

    match crate::arrow2::reader::read_row_group(parquet_file, &meta.clone().into(), i) {
        Ok(buffer) => copy_vec_to_uint8_array(buffer),
        Err(error) => Err(JsValue::from_str(format!("{}", error).as_str())),
    }
}

#[wasm_bindgen(js_name = readMetadataAsync2)]
#[cfg(feature = "reader")]
pub async fn read_metadata_async2(
    url: String,
    content_length: usize,
) -> Result<crate::arrow2::metadata::FileMetaData, JsValue> {
    match crate::arrow2::reader_async::read_metadata_async(url, content_length).await {
        Ok(metadata) => Ok(metadata.into()),
        Err(error) => Err(error),
    }
}

#[wasm_bindgen(js_name = readRowGroupAsync2)]
#[cfg(feature = "reader")]
pub async fn read_row_group_async2(
    url: String,
    content_length: usize,
    meta: crate::arrow2::metadata::FileMetaData,
    i: usize,
) -> Result<Uint8Array, JsValue> {
    match crate::arrow2::reader_async::read_row_group(url, content_length, &meta.clone().into(), i)
        .await
    {
        Ok(buffer) => copy_vec_to_uint8_array(buffer),
        Err(error) => Err(JsValue::from_str(format!("{}", error).as_str())),
    }
}

/// Write Arrow data to a Parquet file using the [`arrow2`](https://crates.io/crates/arrow2) and
/// [`parquet2`](https://crates.io/crates/parquet2) Rust crates.
///
/// For example, to create a Parquet file with Snappy compression:
///
/// ```js
/// import { tableToIPC } from "apache-arrow";
/// // Edit the `parquet-wasm` import as necessary
/// import { WriterPropertiesBuilder, Compression, writeParquet2 } from "parquet-wasm/node2";
///
/// // Given an existing arrow table under `table`
/// const arrowUint8Array = tableToIPC(table, "file");
/// const writerProperties = new WriterPropertiesBuilder()
///   .setCompression(Compression.SNAPPY)
///   .build();
/// const parquetUint8Array = writeParquet2(arrowUint8Array, writerProperties);
/// ```
///
/// If `writerProperties` is not provided or is `null`, the default writer properties will be used.
/// This is equivalent to `new WriterPropertiesBuilder().build()`.
///
/// @param arrow_file Uint8Array containing Arrow data in [IPC **File** format](https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format). If you have an Arrow table in JS, call `tableToIPC(table, "file")` in the JS bindings and pass the result here.
/// @param writer_properties Configuration for writing to Parquet. Use the {@linkcode WriterPropertiesBuilder} to build a writing configuration, then call `.build()` to create an immutable writer properties to pass in here.
/// @returns Uint8Array containing written Parquet data.
#[wasm_bindgen(js_name = writeParquet2)]
#[cfg(feature = "writer")]
pub fn write_parquet2(
    arrow_file: &[u8],
    writer_properties: Option<crate::arrow2::writer_properties::WriterProperties>,
) -> Result<Uint8Array, JsValue> {
    let writer_props = writer_properties.unwrap_or_else(|| {
        crate::arrow2::writer_properties::WriterPropertiesBuilder::default().build()
    });

    match crate::arrow2::writer::write_parquet(arrow_file, writer_props) {
        Ok(buffer) => copy_vec_to_uint8_array(buffer),
        Err(error) => Err(JsValue::from_str(format!("{}", error).as_str())),
    }
}
