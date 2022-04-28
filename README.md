# WASM Parquet [![npm version](https://img.shields.io/npm/v/parquet-wasm.svg)](https://www.npmjs.com/package/parquet-wasm)

WebAssembly bindings to read and write the [Apache Parquet](https://parquet.apache.org/) format to and from [Apache Arrow](https://arrow.apache.org/).

This is designed to be used alongside a JavaScript Arrow implementation, such as the canonical [JS Arrow library](https://arrow.apache.org/docs/js/).

Including all compression codecs, the brotli-encoded WASM bundle is 907KB.

## Install

`parquet-wasm` is published to NPM. Install with

```
yarn add parquet-wasm
# or
npm install parquet-wasm
```

## API

### Two APIs?

These bindings expose _two_ APIs to users because there are _two separate implementations_ of Parquet and Arrow in Rust.

- [`parquet`](https://crates.io/crates/parquet) and [`arrow`](https://crates.io/crates/arrow): These are the "official" Rust implementations of Arrow and Parquet. These projects started earlier and may be more feature complete.
- [`parquet2`](https://crates.io/crates/parquet2) and [`arrow2`](https://crates.io/crates/arrow2): These are safer (in terms of memory access) and claim to be faster, though I haven't written my own benchmarks yet.

Since these parallel projects exist, why not give the user the choice of which to use? In general the reading API is identical in both APIs, however the write options differ between the two projects.

### Choice of bundles

Presumably no one wants to use both `parquet` and `parquet2` at once, so the default bundles separate `parquet` and `parquet2` into separate entry points to keep bundle size as small as possible. The following describe the six bundles available:

| Entry point                   | Rust crates used        | Description                                             |
| ----------------------------- | ----------------------- | ------------------------------------------------------- |
| `parquet-wasm`                | `parquet` and `arrow`   | "Bundler" build, to be used in bundlers such as Webpack |
| `parquet-wasm/node/arrow1`    | `parquet` and `arrow`   | Node build, to be used with `require` in NodeJS         |
| `parquet-wasm/esm/arrow1`     | `parquet` and `arrow`   | ESM, to be used directly from the Web as an ES Module   |
|                               |                         |                                                         |
| `parquet-wasm/bundler/arrow2` | `parquet2` and `arrow2` | "Bundler" build, to be used in bundlers such as Webpack |
| `parquet-wasm/node/arrow2`    | `parquet2` and `arrow2` | Node build, to be used with `require` in NodeJS         |
| `parquet-wasm/esm/arrow2`     | `parquet2` and `arrow2` | ESM, to be used directly from the Web as an ES Module   |

Note that when using the `esm` bundles, the default export must be awaited. See [here](https://rustwasm.github.io/docs/wasm-bindgen/examples/without-a-bundler.html) for an example.

### `parquet` API

This implementation uses the [`arrow`](https://crates.io/crates/arrow) and [`parquet`](https://crates.io/crates/parquet) Rust crates.

Refer to the [API documentation](https://kylebarron.dev/parquet-wasm/modules/bundler_arrow1.html) for more details and examples.

### `parquet2` API

This implementation uses the [`arrow2`](https://crates.io/crates/arrow2) and [`parquet2`](https://crates.io/crates/parquet2) Rust crates.

Refer to the [API documentation](https://kylebarron.dev/parquet-wasm/modules/bundler_arrow2.html) for more details and examples.

### Debug functions

These functions are not present in normal builds to cut down on bundle size. To create a custom build, see [Custom Builds](#custom-builds) below.

#### `setPanicHook`

`setPanicHook(): void`

Sets [`console_error_panic_hook`](https://github.com/rustwasm/console_error_panic_hook) in Rust, which provides better debugging of panics by having more informative `console.error` messages. Initialize this first if you're getting errors such as `RuntimeError: Unreachable executed`.

The WASM bundle must be compiled with the `console_error_panic_hook` for this function to exist.

## Example

```js
import { tableFromArrays, tableFromIPC, tableToIPC } from "apache-arrow";
import {
  readParquet,
  writeParquet,
  Compression,
  WriterPropertiesBuilder,
} from "parquet-wasm";

// Create Arrow Table in JS
const LENGTH = 2000;
const rainAmounts = Float32Array.from({ length: LENGTH }, () =>
  Number((Math.random() * 20).toFixed(1))
);

const rainDates = Array.from(
  { length: LENGTH },
  (_, i) => new Date(Date.now() - 1000 * 60 * 60 * 24 * i)
);

const rainfall = tableFromArrays({
  precipitation: rainAmounts,
  date: rainDates,
});

// Write Arrow Table to Parquet
const writerProperties = new WriterPropertiesBuilder()
  .setCompression(Compression.ZSTD)
  .build();
const parquetBuffer = writeParquet(
  tableToIPC(rainfall, "stream"),
  writerProperties
);

// Read Parquet buffer back to Arrow Table
const table = tableFromIPC(readParquet(parquetBuffer));
console.log(table.schema.toString());
// Schema<{ 0: precipitation: Float32, 1: date: Date64<MILLISECOND> }>
```

### Published examples

- [GeoParquet on the Web (Observable)](https://observablehq.com/@kylebarron/geoparquet-on-the-web)
- [Hello, Parquet-WASM (Observable)](https://observablehq.com/@bmschmidt/hello-parquet-wasm)

## Compression support

The Parquet specification permits several compression codecs. This library currently supports:

- [x] Uncompressed
- [x] Snappy
- [x] Gzip
- [x] Brotli
- [x] ZSTD
- [ ] LZ4 (deprecated)
- [x] LZ4_RAW. Supported in `arrow2` only.

LZ4 support in Parquet is a bit messy. As described [here](https://github.com/apache/parquet-format/blob/54e53e5d7794d383529dd30746378f19a12afd58/Compression.md), there are _two_ LZ4 compression options in Parquet (as of version 2.9.0). The original version `LZ4` is now deprecated; it used an undocumented framing scheme which made interoperability difficult. The specification now reads:

> It is strongly suggested that implementors of Parquet writers deprecate this compression codec in their user-facing APIs, and advise users to switch to the newer, interoperable `LZ4_RAW` codec.

It's currently unknown how widespread the ecosystem support is for `LZ4_RAW`. As of `pyarrow` v7, it now writes `LZ4_RAW` by default and presumably has read support for it as well.

## Custom builds

In some cases, you may know ahead of time that your Parquet files will only include a single compression codec, say Snappy, or even no compression at all. In these cases, you may want to create a custom build of `parquet-wasm` to keep bundle size at a minimum. If you install the Rust toolchain and `wasm-pack` (see [Development](DEVELOP.md)), you can create a custom build with only the compression codecs you require.

### Example custom builds

Reader-only bundle with Snappy compression using the `arrow` and `parquet` crates:

```
wasm-pack build --no-default-features --features arrow1 --features parquet/snap --features reader
```

Writer-only bundle with no compression support using the `arrow2` and `parquet2` crates, targeting Node:

```
wasm-pack build --target nodejs --no-default-features --features arrow2 --features writer
```

Debug bundle with reader and writer support, targeting Node, using `arrow` and `parquet` crates with all their supported compressions, with `console_error_panic_hook` enabled:

```bash
wasm-pack build --dev --target nodejs \
  --no-default-features --features arrow1 \
  --features reader --features writer \
  --features parquet_supported_compressions \
  --features console_error_panic_hook
# Or, given the fact that the default feature includes several of these features, a shorter version:
wasm-pack build --dev --target nodejs --features console_error_panic_hook
```

Refer to the [`wasm-pack` documentation](https://rustwasm.github.io/docs/wasm-pack/commands/build.html) for more info on flags such as `--release`, `--dev`, `target`, and to the [Cargo documentation](https://doc.rust-lang.org/cargo/reference/features.html) for more info on how to use features.

### Available features

- `arrow1`: Use the `arrow` and `parquet` crates
- `arrow2`: Use the `arrow2` and `parquet2` crates
- `reader`: Activate read support.
- `writer`: Activate write support.
- `parquet_supported_compressions`: Activate all supported compressions for the `parquet` crate
- `parquet2_supported_compressions`: Activate all supported compressions for the `parquet2` crate
- parquet compression features. Should only be activated when `arrow1` is activated.
  - `parquet/brotli`: Activate Brotli compression in the `parquet` crate.
  - `parquet/flate2`: Activate Gzip compression in the `parquet` crate.
  - `parquet/snap`: Activate Snappy compression in the `parquet` crate.
  - ~~`parquet/lz4`~~: ~~Activate LZ4 compression in the `parquet` crate.~~ WASM-compatible version not yet implemented in the `parquet` crate.
  - `parquet/zstd`: Activate ZSTD compression in the `parquet` crate.
- parquet2 compression features. Should only be activated when `arrow2` is activated.
  - `parquet2/brotli`: Activate Brotli compression in the `parquet2` crate.
  - `parquet2/gzip`: Activate Gzip compression in the `parquet2` crate.
  - `parquet2/snappy`: Activate Snappy compression in the `parquet2` crate.
  - `parquet2/lz4_flex`: Activate LZ4_RAW compression in the `parquet2` crate.
  - `parquet2/zstd`: Activate ZSTD compression in the `parquet2` crate.
- `console_error_panic_hook`: Expose the `setPanicHook` function for better error messages for Rust panics.

## Future work

- [ ] Async support to make requests for Parquet chunks directly from Rust. **If you're familiar with Rust async, I would love some help in [#96](https://github.com/kylebarron/parquet-wasm/pull/96)** :slightly_smiling_face:. This would additionally support:
  - [ ] Pushdown predicate filtering, to download only chunks that match a specific condition
  - [ ] Column filtering, to download only certain columns
  - [ ] Async iterable support, to return chunks as they arrive
- [ ] More tests

## Acknowledgements

A starting point of my work came from @my-liminal-space's [`read-parquet-browser`](https://github.com/my-liminal-space/read-parquet-browser) (which is also dual licensed MIT and Apache 2).

@domoritz's [`arrow-wasm`](https://github.com/domoritz/arrow-wasm) was a very helpful reference for bootstrapping Rust-WASM bindings.
