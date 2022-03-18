# Changelog

## [0.2.0] - 2022-03-17

* Restore arrow-rs support by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/21
* Write parquet with arrow1 by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/23
* Refactor code into lower-level functions, use `?` operator by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/25
* Make record batch size the nrows of the first row group by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/26
* Rename arrow-rs api as default by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/31
* Implement writerPropertiesBuilder for arrow1 by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/30
* Refactor into modules by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/32
* Update bundling to create arrow2 entrypoints by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/33
* Node testing setup by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/34
* Helper to copy vec<u8> to Uint8Array by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/38
* Faster builds on Node CI tests by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/39
* Rust CI caching by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/40
* ZSTD mac instructions in readme by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/42
* Keep opt-level = s and remove `console_error_panic_hook` by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/48
* WriterPropertiesBuilder for arrow2 by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/49
* Docstrings for public functions, structs, enums by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/50
* Compression-specific features by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/51
* Add more node tests by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/52
* Separate reader and writer features by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/47
* Docs update by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/53
* Working typedoc by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/55
* Update docstrings and readme by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/60

**Full Changelog**: https://github.com/kylebarron/parquet-wasm/compare/v0.1.1...v0.2.0

## [0.1.1] - 2022-03-06

- Attempt better bundling, with APIs for bundlers, Node, and the Web.

## [0.1.0] - 2022-03-06

- Initial release
- Barebones `read_parquet` and `write_parquet` functions.
