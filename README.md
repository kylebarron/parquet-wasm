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

| Entry point             | Rust crates used        | Description                                             |
| ----------------------- | ----------------------- | ------------------------------------------------------- |
| `parquet-wasm`          | `parquet` and `arrow`   | "Bundler" build, to be used in bundlers such as Webpack |
| `parquet-wasm/node/arrow1`     | `parquet` and `arrow`   | Node build, to be used with `require` in NodeJS         |
| `parquet-wasm/esm/arrow1`      | `parquet` and `arrow`   | ESM, to be used directly from the Web as an ES Module   |
|                         |                         |                                                         |
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

## Compression support

The Parquet specification permits several compression codecs. This library currently supports:

- [x] Uncompressed
- [x] Snappy
- [x] Gzip
- [x] Brotli
- [x] ZSTD. Supported in `arrow1`, will be supported in `arrow2` when the next version of the upstream `parquet2` package is released.
- [ ] LZ4. Work is progressing but no support yet.

## Future work

- [ ] More tests :smile:

## Acknowledgements

A starting point of my work came from @my-liminal-space's [`read-parquet-browser`](https://github.com/my-liminal-space/read-parquet-browser) (which is also dual licensed MIT and Apache 2).

@domoritz's [`arrow-wasm`](https://github.com/domoritz/arrow-wasm) was a very helpful reference for bootstrapping Rust-WASM bindings.
