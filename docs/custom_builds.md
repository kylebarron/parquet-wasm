## Custom builds

In some cases, you may know ahead of time that your Parquet files will only include a single compression codec, say Snappy, or even no compression at all. In these cases, you may want to create a custom build of `parquet-wasm` to keep bundle size at a minimum. If you install the Rust toolchain and `wasm-pack` (see [Development](#development)), you can create a custom build with only the compression codecs you require.

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
  - ~~`parquet2/lz4`~~: ~~Activate LZ4 compression in the `parquet2` crate~~. WASM-compatible version not yet implemented, pending https://github.com/jorgecarleitao/parquet2/pull/91
  - ~~`parquet2/zstd`~~: ~~Activate ZSTD compression in the `parquet2` crate.~~ ZSTD should work in parquet2's next release.
- `console_error_panic_hook`: Expose the `setPanicHook` function for better error messages for Rust panics.

