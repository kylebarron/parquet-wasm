use crate::utils::copy_vec_to_uint8_array;
use js_sys::Uint8Array;
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
pub fn read_parquet(parquet_file: &[u8]) -> Result<Uint8Array, JsValue> {
    if parquet_file.is_empty() {
        return Err(JsValue::from_str(
            "Empty input provided or not a Uint8Array.",
        ));
    }

    match crate::arrow1::reader::read_parquet(parquet_file) {
        Ok(buffer) => copy_vec_to_uint8_array(buffer),
        Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
    }
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
) -> Result<Uint8Array, JsValue> {
    let writer_props = writer_properties.unwrap_or_else(|| {
        crate::arrow1::writer_properties::WriterPropertiesBuilder::default().build()
    });

    match crate::arrow1::writer::write_parquet(arrow_file, writer_props) {
        Ok(buffer) => copy_vec_to_uint8_array(buffer),
        Err(error) => return Err(JsValue::from_str(format!("{}", error).as_str())),
    }
}
