# `parquet-wasm`

WebAssembly bindings to read and write the [Apache Parquet](https://parquet.apache.org/) format to and from [Apache Arrow](https://arrow.apache.org/).

This is designed to be used alongside a JavaScript Arrow implementation, such as the canonical [JS Arrow library](https://arrow.apache.org/docs/js/).

Including all compression codecs, the brotli-encoded WASM bundle is 881KB.

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

| Entry point                  | Rust crates used        | Description                                             |
| ---------------------------- | ----------------------- | ------------------------------------------------------- |
| `parquet-wasm`               | `parquet` and `arrow`   | "Bundler" build, to be used in bundlers such as Webpack |
| `parquet-wasm/node`          | `parquet` and `arrow`   | Node build, to be used with `require` in NodeJS         |
| `parquet-wasm/web`           | `parquet` and `arrow`   | ESM, to be used directly from the Web as an ES Module   |
|                              |                         |                                                         |
| `parquet-wasm/parquet_wasm2` | `parquet2` and `arrow2` | "Bundler" build, to be used in bundlers such as Webpack |
| `parquet-wasm/node2`         | `parquet2` and `arrow2` | Node build, to be used with `require` in NodeJS         |
| `parquet-wasm/web2`          | `parquet2` and `arrow2` | ESM, to be used directly from the Web as an ES Module   |

### `parquet` API

This implementation uses the [`arrow`](https://crates.io/crates/arrow) and [`parquet`]() Rust crates.

#### `readParquet`

`readParquet(parquet_file: Uint8Array): Uint8Array`

Takes as input a `Uint8Array` containing bytes from a loaded Parquet file. Returns a `Uint8Array` with data in [Arrow IPC **Stream** format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format). To parse this into an Arrow table, pass the result of `readParquet` to `arrow.tableFromIPC` in the JS bindings.

#### `writeParquet`

`writeParquet(arrow_file: Uint8Array, writer_properties: WriterProperties): Uint8Array`

Takes as input a `Uint8Array` containing bytes in [Arrow IPC **Stream** format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format). If you have an Arrow table, call `arrow.tableToIPC(table, 'stream')` and pass the result to `writeParquet`.

TODO: writer properties

#### Write options

Explain builder

### `parquet2` API

#### `readParquet2`

`readParquet2(parquet_file: Uint8Array): Uint8Array`

Takes as input a `Uint8Array` containing bytes from a loaded Parquet file. Returns a `Uint8Array` with data in [Arrow IPC **Stream** format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format). To parse this into an Arrow table, pass the result of `readParquet2` to `arrow.tableFromIPC` in the JS bindings.

#### `writeParquet2`

`writeParquet2(arrow_file: Uint8Array): Uint8Array`

Takes as input a `Uint8Array` containing bytes in [Arrow IPC **File** format](https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format) [^1]. If you have an Arrow table, call `arrow.tableToIPC(table, 'file')` and pass the result to `writeParquet2`.

[^1]: I'm not great at Rust and the IPC File format seemed easier to parse in Rust than the IPC Stream format :slightly_smiling_face:.

For the initial release, `writeParquet2` is hard-coded to use Snappy compression and Plain encoding. In the future these should be made configurable.

### Common

Functions in common between the two APIs (present in every build).

#### `setPanicHook`

`setPanicHook(): void`

Sets [`console_error_panic_hook`](https://github.com/rustwasm/console_error_panic_hook) in Rust, which provides better debugging of panics by having more informative `console.error` messages. Initialize this first if you're getting errors such as `RuntimeError: Unreachable executed`.

## Example

```js
import { tableFromArrays, tableFromIPC, tableToIPC } from "apache-arrow";
import { readParquet, writeParquet } from "parquet-wasm";

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
const parquetBuffer = writeParquet(tableToIPC(rainfall, "stream"));

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

## Custom builds

In some cases, you may know ahead of time that your Parquet files will only include a single compression codec, say Snappy, or even no compression at all. In these cases, you may want to create a custom build of `parquet-wasm` to keep bundle size at a minimum. If you install the Rust toolchain and `wasm-pack` (see [Development](#development)), you can create a custom build with only the compression codecs you require.

To create the build, run

```bash
wasm-pack build \
  --release \
  `# Choose your JS target: one of --target bundler, --target nodejs, or --target web` \
  --target nodejs \
  `# Turn off all defaults` \
  --no-default-features \
  `# Choose your Rust parquet implementation: One of --features arrow1 or --features arrow2` \
  --features arrow1 \
  `# Choose your compressions (TODO: need to expose these features)`
```

## Future work

- [ ] Tests :smile:
- [ ] User-specified column-specific encodings when writing
- [ ] User-specified compression codec when writing

## Development

- Install [wasm-pack](https://rustwasm.github.io/wasm-pack/)
- Compile: `wasm-pack build`, or change targets, e.g. `wasm-pack build --target nodejs`
- Publish `wasm-pack publish`.

### MacOS

Some steps may need a specific configuration if run on MacOS. Specifically, the default `clang` shipped with Macs (as of March 2022) doesn't have WebAssembly compilation supported out of the box. To build ZSTD, you may need to install a later version via Homebrew and update your paths to find the correct executables.

```
brew install llvm
export PATH="/usr/local/opt/llvm/bin/:$PATH"
export CC=/usr/local/opt/llvm/bin/clang
export AR=/usr/local/opt/llvm/bin/llvm-ar
```

See [this description](https://github.com/kylebarron/parquet-wasm/pull/2#issue-1159174043) and its references for more info.

### Publishing

`wasm-pack` supports [three different targets](https://rustwasm.github.io/docs/wasm-pack/commands/build.html#target):

- `bundler` (used with bundlers like Webpack)
- `nodejs` (used with Node, supports `require`)
- `web` (used as an ES module directly from the web)

There are good reasons to distribute as any of these... so why not distribute as all three? `wasm-pack` doesn't support this directly but the build script in `scripts/build.sh` calls `wasm-pack` three times and merges the outputs. This means that bundler users can use the default, Node users can use `parquet-wasm/node` and ES Modules users can use `parquet-wasm/web` in their imports.

To publish:

```
bash ./scripts/build.sh
wasm-pack publish
```

## Acknowledgements

A starting point of my work came from @my-liminal-space's [`read-parquet-browser`](https://github.com/my-liminal-space/read-parquet-browser) (which is also dual licensed MIT and Apache 2).

@domoritz's [`arrow-wasm`](https://github.com/domoritz/arrow-wasm) was a very helpful reference for bootstrapping Rust-WASM bindings.
