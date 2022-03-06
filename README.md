# `parquet-wasm`

WebAssembly bindings to read and write the Parquet format to Apache Arrow.

This is designed to be used alongside a JavaScript [Arrow](https://arrow.apache.org/) implementation, such as the canonical [JS Arrow library](https://arrow.apache.org/docs/js/) or potentially [`arrow-wasm`](https://github.com/domoritz/arrow-wasm).

## Usage

### `readParquet`

`readParquet(parquet_file: Uint8Array): Uint8Array`

Takes as input a `Uint8Array` containing bytes from a loaded Parquet file. Returns a `Uint8Array` with data in [Arrow IPC **Stream** format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format) [^0]. To parse this into an Arrow table, use `arrow.tableFromIPC` in the JS bindings on the result from `readParquet`.

[^0]: I originally decoded Parquet files to the Arrow IPC File format, but Arrow JS occasionally produced bugs such as `Error: Expected to read 1901288 metadata bytes, but only read 644` when parsing using `arrow.tableFromIPC`. When testing the same buffer in Pyarrow, `pa.ipc.open_file` succeeded but `pa.ipc.open_stream` failed, leading me to believe that the Arrow JS implementation has some bugs to decide when `arrow.tableFromIPC` should internally use the `RecordBatchStreamReader` vs the `RecordBatchFileReader`.

### `writeParquet`

`writeParquet(arrow_file: Uint8Array): Uint8Array`

Takes as input a `Uint8Array` containing bytes in [Arrow IPC **File** format](https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format) [^1]. If you have an Arrow table, call `arrow.tableToIPC(table, 'file')` and pass the result to `writeParquet`.

[^1]: I'm not great at Rust and the IPC File format seemed easier to parse in Rust than the IPC Stream format :slightly_smiling_face:.

For the initial release, `writeParquet` is hard-coded to use Snappy compression and Plain encoding. In the future these should be made configurable.

### `setPanicHook`

`setPanicHook(): void`

Sets [`console_error_panic_hook`](https://github.com/rustwasm/console_error_panic_hook) in Rust, which provides better debugging of panics by having more informative `console.error` messages. Initialize this first if you're getting errors such as `RuntimeError: Unreachable executed`.

## Example

```js
import {tableFromArrays, tableFromIPC, tableToIPC} from 'apache-arrow';
import {readParquet, writeParquet} from "parquet-wasm";

// Create Arrow Table in JS
const LENGTH = 2000;
const rainAmounts = Float32Array.from(
    { length: LENGTH },
    () => Number((Math.random() * 20).toFixed(1)));

const rainDates = Array.from(
    { length: LENGTH },
    (_, i) => new Date(Date.now() - 1000 * 60 * 60 * 24 * i));

const rainfall = tableFromArrays({
    precipitation: rainAmounts,
    date: rainDates
});

// Write Arrow Table to Parquet
const parquetBuffer = writeParquet(tableToIPC(rainfall, 'file'));

// Read Parquet buffer back to Arrow Table
const table = tableFromIPC(readParquet(parquetBuffer));
console.log(table.schema.toString());
// Schema<{ 0: precipitation: Float32, 1: date: Date64<MILLISECOND> }>
```

## Compression support

The Parquet specification permits several compression codecs. This library currently supports:

- [x] Uncompressed
- [x] Snappy
- [x] Gzip
- [x] Brotli
- [x] ZSTD
- [ ] LZ4

LZ4 compression appears not to work yet. When trying to parse a file with LZ4 compression I get an error: `Uncaught (in promise) External format error: underlying IO error: WrongMagicNumber`.

## Future work

- [ ] Tests :smile:
- [ ] User-specified column-specific encodings when writing
- [ ] User-specified compression codec when writing

## Development

- Install [wasm-pack](https://rustwasm.github.io/wasm-pack/)
- Compile: `wasm-pack build`, or change targets, e.g. `wasm-pack build --target nodejs`
- Publish `wasm-pack publish`.

## Credits

A starting point of my work came from @my-liminal-space's [`read-parquet-browser`](https://github.com/my-liminal-space/read-parquet-browser) (which is also dual licensed MIT and Apache 2).
