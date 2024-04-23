# WASM Parquet [![npm version](https://img.shields.io/npm/v/parquet-wasm.svg)](https://www.npmjs.com/package/parquet-wasm)

WebAssembly bindings to read and write the [Apache Parquet](https://parquet.apache.org/) format to and from [Apache Arrow](https://arrow.apache.org/) using the Rust [`parquet`](https://crates.io/crates/parquet) and [`arrow`](https://crates.io/crates/arrow) crates.

This is designed to be used alongside a JavaScript Arrow implementation, such as the canonical [JS Arrow library](https://arrow.apache.org/docs/js/).

Including read and write support and all compression codecs, the brotli-compressed WASM bundle is 1.2 MB. Refer to [custom builds](#custom-builds) for how to build a smaller bundle. A minimal read-only bundle without compression support can be as small as 456 KB brotli-compressed.

## Install

`parquet-wasm` is published to NPM. Install with

```
yarn add parquet-wasm
```

or

```
npm install parquet-wasm
```

## API

Parquet-wasm has both a synchronous and asynchronous API. The sync API is simpler but requires fetching the entire Parquet buffer in advance, which is often prohibitive.

### Sync API

Refer to these functions:

- [`readParquet`](https://kylebarron.dev/parquet-wasm/functions/esm_parquet_wasm.readParquet.html): Read a Parquet file synchronously.
- [`readSchema`](https://kylebarron.dev/parquet-wasm/functions/esm_parquet_wasm.readSchema.html): Read an Arrow schema from a Parquet file synchronously.
- [`writeParquet`](https://kylebarron.dev/parquet-wasm/functions/esm_parquet_wasm.writeParquet.html): Write a Parquet file synchronously.

### Async API

- [`readParquetStream`](https://kylebarron.dev/parquet-wasm/functions/esm_parquet_wasm.readParquetStream.html): Create a [ReadableStream](https://developer.mozilla.org/en-US/docs/Web/API/ReadableStream) that emits Arrow RecordBatches from a Parquet file.
- [`ParquetFile`](https://kylebarron.dev/parquet-wasm/classes/esm_parquet_wasm.ParquetFile.html): A class for reading portions of a remote Parquet file. Use [`fromUrl`](https://kylebarron.dev/parquet-wasm/classes/esm_parquet_wasm.ParquetFile.html#fromUrl) to construct from a remote URL or [`fromFile`](https://kylebarron.dev/parquet-wasm/classes/esm_parquet_wasm.ParquetFile.html#fromFile) to construct from a [`File`](https://developer.mozilla.org/en-US/docs/Web/API/File) handle. Note that when you're done using this class, you'll need to call [`free`](https://kylebarron.dev/parquet-wasm/classes/esm_parquet_wasm.ParquetFile.html#free) to release any memory held by the ParquetFile instance itself.


Both sync and async functions return or accept a [`Table`](https://kylebarron.dev/parquet-wasm/classes/bundler_parquet_wasm.Table.html) class, an Arrow table in WebAssembly memory. Refer to its documentation for moving data into/out of WebAssembly.

## Entry Points


| Entry point                                                               | Description                                             | Documentation        |
| ------------------------------------------------------------------------- | ------------------------------------------------------- | -------------------- |
| `parquet-wasm`, `parquet-wasm/esm`, or `parquet-wasm/esm/parquet_wasm.js` | ESM, to be used directly from the Web as an ES Module   | [Link][esm-docs]     |
| `parquet-wasm/bundler`                                                    | "Bundler" build, to be used in bundlers such as Webpack | [Link][bundler-docs] |
| `parquet-wasm/node`                                                       | Node build, to be used with synchronous `require` in NodeJS         | [Link][node-docs]    |

[bundler-docs]: https://kylebarron.dev/parquet-wasm/modules/bundler_parquet_wasm.html
[node-docs]: https://kylebarron.dev/parquet-wasm/modules/node_parquet_wasm.html
[esm-docs]: https://kylebarron.dev/parquet-wasm/modules/esm_parquet_wasm.html

### ESM

The `esm` entry point is the primary entry point. It is the default export from `parquet-wasm`, and is also accessible at `parquet-wasm/esm` and `parquet-wasm/esm/parquet_wasm.js` (for symmetric imports [directly from a browser](#using-directly-from-a-browser)).

**Note that when using the `esm` bundles, you must manually initialize the WebAssembly module before using any APIs**. Otherwise, you'll get an error `TypeError: Cannot read properties of undefined`. There are multiple ways to initialize the WebAssembly code:

#### Asynchronous initialization

The primary way to initialize is by awaiting the default export.

```js
import wasmInit, {readParquet} from "parquet-wasm";

await wasmInit();
```

Without any parameter, this will try to fetch a file named `'parquet_wasm_bg.wasm'` at the same location as `parquet-wasm`. (E.g. this snippet `input = new URL('parquet_wasm_bg.wasm', import.meta.url);`).

Note that you can also pass in a custom URL if you want to host the `.wasm` file on your own servers.

```js
import wasmInit, {readParquet} from "parquet-wasm";

// Update this version to match the version you're using.
const wasmUrl = "https://cdn.jsdelivr.net/npm/parquet-wasm@0.6.0/esm/parquet_wasm_bg.wasm";
await wasmInit(wasmUrl);
```

#### Synchronous initialization

The `initSync` named export allows for

```js
import {initSync, readParquet} from "parquet-wasm";

// The contents of esm/parquet_wasm_bg.wasm in an ArrayBuffer
const wasmBuffer = new ArrayBuffer(...);

// Initialize the Wasm synchronously
initSync(wasmBuffer)
```

Async initialization should be preferred over downloading the Wasm buffer and then initializing it synchronously, as [`WebAssembly.instantiateStreaming`](https://developer.mozilla.org/en-US/docs/WebAssembly/JavaScript_interface/instantiateStreaming_static) is the most efficient way to both download and initialize Wasm code.

### Bundler

The `bundler` entry point doesn't require manual initialization of the WebAssembly blob, but needs setup with whatever bundler you're using. [Refer to the Rust Wasm documentation for more info](https://rustwasm.github.io/docs/wasm-bindgen/reference/deployment.html#bundlers).

### Node

The `node` entry point can be loaded synchronously from Node.

```js
const {readParquet} = require("parquet-wasm");

const wasmTable = readParquet(...);
```

### Using directly from a browser

You can load the `esm/parquet_wasm.js` file directly from a CDN

```js
const parquet = await import(
  "https://cdn.jsdelivr.net/npm/parquet-wasm@0.6.0/esm/+esm"
)
await parquet.default();

const wasmTable = parquet.readParquet(...);
```

This specific endpoint will minify the ESM before you receive it.

### Debug functions

These functions are not present in normal builds to cut down on bundle size. To create a custom build, see [Custom Builds](#custom-builds) below.

#### `setPanicHook`

`setPanicHook(): void`

Sets [`console_error_panic_hook`](https://github.com/rustwasm/console_error_panic_hook) in Rust, which provides better debugging of panics by having more informative `console.error` messages. Initialize this first if you're getting errors such as `RuntimeError: Unreachable executed`.

The WASM bundle must be compiled with the `console_error_panic_hook` feature for this function to exist.

## Example

```js
import * as arrow from "apache-arrow";
import initWasm, {
  Compression,
  readParquet,
  Table,
  writeParquet,
  WriterPropertiesBuilder,
} from "parquet-wasm";

// Instantiate the WebAssembly context
await initWasm();

// Create Arrow Table in JS
const LENGTH = 2000;
const rainAmounts = Float32Array.from({ length: LENGTH }, () =>
  Number((Math.random() * 20).toFixed(1))
);

const rainDates = Array.from(
  { length: LENGTH },
  (_, i) => new Date(Date.now() - 1000 * 60 * 60 * 24 * i)
);

const rainfall = arrow.tableFromArrays({
  precipitation: rainAmounts,
  date: rainDates,
});

// Write Arrow Table to Parquet

// wasmTable is an Arrow table in WebAssembly memory
const wasmTable = Table.fromIPCStream(arrow.tableToIPC(rainfall, "stream"));
const writerProperties = new WriterPropertiesBuilder()
  .setCompression(Compression.ZSTD)
  .build();
const parquetUint8Array = writeParquet(wasmTable, writerProperties);

// Read Parquet buffer back to Arrow Table
// arrowWasmTable is an Arrow table in WebAssembly memory
const arrowWasmTable = readParquet(parquetUint8Array);

// table is now an Arrow table in JS memory
const table = arrow.tableFromIPC(arrowWasmTable.intoIPCStream());
console.log(table.schema.toString());
// Schema<{ 0: precipitation: Float32, 1: date: Date64<MILLISECOND> }>
```

### Published examples

(These may use older versions of the library with a different API).

- [GeoParquet on the Web (Observable)](https://observablehq.com/@kylebarron/geoparquet-on-the-web)
- [Hello, Parquet-WASM (Observable)](https://observablehq.com/@bmschmidt/hello-parquet-wasm)

## Performance considerations

Tl;dr: When you have a `Table` object (resulting from `readParquet`), try the new
[`Table.intoFFI`](https://kylebarron.dev/parquet-wasm/classes/esm_parquet_wasm.Table.html#intoFFI)
API to move it to JavaScript memory. This API is less well tested than the [`Table.intoIPCStream`](https://kylebarron.dev/parquet-wasm/classes/esm_parquet_wasm.Table.html#intoIPCStream) API, but should be
faster and have **much** less memory overhead (by a factor of 2). If you hit any bugs, please
[create a reproducible issue](https://github.com/kylebarron/parquet-wasm/issues/new).

Under the hood, `parquet-wasm` first decodes a Parquet file into Arrow _in WebAssembly memory_. But
then that WebAssembly memory needs to be copied into JavaScript for use by Arrow JS. The "normal"
conversion APIs (e.g. `Table.intoIPCStream`) use the [Arrow IPC
format](https://arrow.apache.org/docs/python/ipc.html) to get the data back to JavaScript. But this
requires another memory copy _inside WebAssembly_ to assemble the various arrays into a single
buffer to be copied back to JS.

Instead, the new `Table.intoFFI` API uses Arrow's [C Data
Interface](https://arrow.apache.org/docs/format/CDataInterface.html) to be able to copy or view
Arrow arrays from within WebAssembly memory without any serialization.

Note that this approach uses the [`arrow-js-ffi`](https://github.com/kylebarron/arrow-js-ffi)
library to parse the Arrow C Data Interface definitions. This library has not yet been tested in
production, so it may have bugs!

I wrote an [interactive blog
post](https://observablehq.com/@kylebarron/zero-copy-apache-arrow-with-webassembly) on this approach
and the Arrow C Data Interface if you want to read more!

### Example

```js
import * as arrow from "apache-arrow";
import { parseTable } from "arrow-js-ffi";
import initWasm, { wasmMemory, readParquet } from "parquet-wasm";

// Instantiate the WebAssembly context
await initWasm();

// A reference to the WebAssembly memory object.
const WASM_MEMORY = wasmMemory();

const resp = await fetch("https://example.com/file.parquet");
const parquetUint8Array = new Uint8Array(await resp.arrayBuffer());
const wasmArrowTable = readParquet(parquetUint8Array).intoFFI();

// Arrow JS table that was directly copied from Wasm memory
const table: arrow.Table = parseTable(
  WASM_MEMORY.buffer,
  wasmArrowTable.arrayAddrs(),
  wasmArrowTable.schemaAddr()
);

// VERY IMPORTANT! You must call `drop` on the Wasm table object when you're done using it
// to release the Wasm memory.
// Note that any access to the pointers in this table is undefined behavior after this call.
// Calling any `wasmArrowTable` method will error.
wasmArrowTable.drop();
```

## Compression support

The Parquet specification permits several compression codecs. This library currently supports:

- [x] Uncompressed
- [x] Snappy
- [x] Gzip
- [x] Brotli
- [x] ZSTD
- [x] LZ4_RAW
- [ ] LZ4 (deprecated)

LZ4 support in Parquet is a bit messy. As described [here](https://github.com/apache/parquet-format/blob/54e53e5d7794d383529dd30746378f19a12afd58/Compression.md), there are _two_ LZ4 compression options in Parquet (as of version 2.9.0). The original version `LZ4` is now deprecated; it used an undocumented framing scheme which made interoperability difficult. The specification now reads:

> It is strongly suggested that implementors of Parquet writers deprecate this compression codec in their user-facing APIs, and advise users to switch to the newer, interoperable `LZ4_RAW` codec.

It's currently unknown how widespread the ecosystem support is for `LZ4_RAW`. As of `pyarrow` v7, it now writes `LZ4_RAW` by default and presumably has read support for it as well.

## Custom builds

In some cases, you may know ahead of time that your Parquet files will only include a single compression codec, say Snappy, or even no compression at all. In these cases, you may want to create a custom build of `parquet-wasm` to keep bundle size at a minimum. If you install the Rust toolchain and `wasm-pack` (see [Development](DEVELOP.md)), you can create a custom build with only the compression codecs you require.

The minimum supported Rust version in this project is 1.60. To upgrade your toolchain, use `rustup update stable`.

### Example custom builds

Reader-only bundle with Snappy compression:

```
wasm-pack build --no-default-features --features snappy --features reader
```

Writer-only bundle with no compression support, targeting Node:

```
wasm-pack build --target nodejs --no-default-features --features writer
```

Bundle with reader and writer support, targeting Node, using `arrow` and `parquet` crates with all their supported compressions, with `console_error_panic_hook` enabled:

```bash
wasm-pack build \
  --target nodejs \
  --no-default-features \
  --features reader \
  --features writer \
  --features all_compressions \
  --features debug
# Or, given the fact that the default feature includes several of these features, a shorter version:
wasm-pack build --target nodejs --features debug
```

Refer to the [`wasm-pack` documentation](https://rustwasm.github.io/docs/wasm-pack/commands/build.html) for more info on flags such as `--release`, `--dev`, `target`, and to the [Cargo documentation](https://doc.rust-lang.org/cargo/reference/features.html) for more info on how to use features.

### Available features

By default, `all_compressions`, `reader`, `writer`, and `async` features are enabled. Use `--no-default-features` to remove these defaults.

- `reader`: Activate read support.
- `writer`: Activate write support.
- `async`: Activate asynchronous read support.
- `all_compressions`: Activate all supported compressions.
- `brotli`: Activate Brotli compression.
- `gzip`: Activate Gzip compression.
- `snappy`: Activate Snappy compression.
- `zstd`: Activate ZSTD compression.
- `lz4`: Activate LZ4_RAW compression.
- `debug`: Expose the `setPanicHook` function for better error messages for Rust panics.

## Node <20

On Node versions before 20, you'll have to [polyfill the Web Cryptography API](https://docs.rs/getrandom/latest/getrandom/#nodejs-es-module-support).

## Future work

- [ ] Example of pushdown predicate filtering, to download only chunks that match a specific condition
- [ ] Column filtering, to download only certain columns
- [ ] More tests

## Acknowledgements

A starting point of my work came from @my-liminal-space's [`read-parquet-browser`](https://github.com/my-liminal-space/read-parquet-browser) (which is also dual licensed MIT and Apache 2).

@domoritz's [`arrow-wasm`](https://github.com/domoritz/arrow-wasm) was a very helpful reference for bootstrapping Rust-WASM bindings.
