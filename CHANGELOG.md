# Changelog

## [0.6.0] - 2024-04-21

### New! :sparkles:

- Class-based API + concurrent streams + column selections + File reader by @H-Plus-Time in https://github.com/kylebarron/parquet-wasm/pull/407. This added a new `ParquetFile` API for working with files at remote URLs without downloading them first.
- Conditional exports in `package.json`. This should make it easier to use across Node and browser.
- Improved documentation for how to use different entry points.

### Breaking Changes:

- arrow2 and parquet2-based implementation has been removed.
- Layout of files has changed. Your import may need to change.
- Imports are now `parquet-wasm`, `parquet-wasm/esm`, `parquet-wasm/bundler`, and `parquet-wasm/node`.

## What's Changed

- Add conditional exports by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/382
- CI production build size summary by @H-Plus-Time in https://github.com/kylebarron/parquet-wasm/pull/401
- Remove arrow2 implementation by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/446
- feat: add lz4_raw support for `arrow1` by @fspoettel in https://github.com/kylebarron/parquet-wasm/pull/466
- Highlight that esm entry point needs await of default export by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/487
- Fixes for both report builds and PR comment workflow by @H-Plus-Time in https://github.com/kylebarron/parquet-wasm/pull/495
- fix package exports by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/414
- Object store wasm usage by @H-Plus-Time in https://github.com/kylebarron/parquet-wasm/pull/490
- Set Parquet key-value metadata by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/503
- Read parquet with options by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/506
- Documentation updates for 0.6 by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/507
- Avoid bigint for metadata queries by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/508
- Update async API by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/510
- Add test to read empty file by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/512
- bump arrow libraries to version 51 by @jdoig in https://github.com/kylebarron/parquet-wasm/pull/496

## New Contributors

- @fspoettel made their first contribution in https://github.com/kylebarron/parquet-wasm/pull/466
- @jdoig made their first contribution in https://github.com/kylebarron/parquet-wasm/pull/496

**Full Changelog**: https://github.com/kylebarron/parquet-wasm/compare/v0.5.0...v0.6.0

## [0.5.0] - 2023-10-21

## What's Changed

- Switch to an API based on table abstractions from [arrow-wasm](https://github.com/kylebarron/arrow-wasm).
- Update docs
- Initial implementation of reading to a stream of Arrow batches. By @H-Plus-Time in https://github.com/kylebarron/parquet-wasm/pull/296

## New Contributors

- @H-Plus-Time made their first contribution in https://github.com/kylebarron/parquet-wasm/pull/296

**Full Changelog**: https://github.com/kylebarron/parquet-wasm/compare/v0.4.0...v0.5.0

## [0.4.0] - 2023-08-15

## What's Changed

- Async reader support in the arrow2 bindings
- Improved memory usage via `readParquetFFI` in conjunction with `arrow-js-ffi`.
- Remove "2" from function names in arrow2 api by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/173
- Make arrow2 the default bundle by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/174
- Add bindings for arrow2 metadata (without serde support) by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/153
- Add lz4_raw and zstd compressions for parquet2 by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/114

**Full Changelog**: https://github.com/kylebarron/parquet-wasm/compare/v0.3.1...v0.4.0

## [0.4.0-beta.1] - 2022-08-08

## What's Changed

- Add lz4_raw and zstd compressions for parquet2 by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/114
- Simplify cargo features by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/117
- Add vscode rust-analyzer target setting by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/131
- add msrv by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/132
- pin clap to 3.1.\* by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/139
- Make writerProperties optional in JS api by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/152
- Add bindings for arrow2 metadata (without serde support) by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/153
- Async reader by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/96
- Cleaner error handling by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/157
- implement `From` instead of custom methods by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/168
- Remove "2" from function names in arrow2 api by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/173
- Make arrow2 the default bundle by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/174
- Improved documentation for async reading by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/175

**Full Changelog**: https://github.com/kylebarron/parquet-wasm/compare/v0.3.1...v0.4.0-beta.1

## [0.3.1] - 2022-04-26

## What's Changed

- Bump arrow from 11.0.0 to 11.1.0 by @dependabot in https://github.com/kylebarron/parquet-wasm/pull/77
- Update lockfile by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/76
- Add clippy by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/78
- Remove old debug script by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/79
- Bump clap from 3.1.8 to 3.1.9 by @dependabot in https://github.com/kylebarron/parquet-wasm/pull/87
- Check that input exists/is a uint8array by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/102
- Update test files to those written by pyarrow v7 by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/103
- Update to arrow and parquet 12.0 by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/105
- Bump clap from 3.1.9 to 3.1.12 by @dependabot in https://github.com/kylebarron/parquet-wasm/pull/98
- Create arrow1/arrow2 read benchmarks by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/82
- Publish docs on tag by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/106
- Update readme by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/107
- Add published examples section to readme by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/108
- Unify build script by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/109
- esm2 entrypoint with no import.meta.url by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/110

**Full Changelog**: https://github.com/kylebarron/parquet-wasm/compare/v0.3.0...v0.3.1

## [0.3.0] - 2022-04-04

## What's Changed

- Debug cli by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/64
- Bump to arrow 11.0 to support zstd compression by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/66
- Update bundling by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/67
- Add dependabot by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/70
- Bump clap from 3.1.6 to 3.1.8 by @dependabot in https://github.com/kylebarron/parquet-wasm/pull/71
- Bump getrandom from 0.2.5 to 0.2.6 by @dependabot in https://github.com/kylebarron/parquet-wasm/pull/72

## New Contributors

- @dependabot made their first contribution in https://github.com/kylebarron/parquet-wasm/pull/71

**Full Changelog**: https://github.com/kylebarron/parquet-wasm/compare/v0.2.0...v0.3.0

## [0.2.0] - 2022-03-17

- Restore arrow-rs support by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/21
- Write parquet with arrow1 by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/23
- Refactor code into lower-level functions, use `?` operator by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/25
- Make record batch size the nrows of the first row group by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/26
- Rename arrow-rs api as default by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/31
- Implement writerPropertiesBuilder for arrow1 by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/30
- Refactor into modules by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/32
- Update bundling to create arrow2 entrypoints by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/33
- Node testing setup by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/34
- Helper to copy vec<u8> to Uint8Array by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/38
- Faster builds on Node CI tests by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/39
- Rust CI caching by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/40
- ZSTD mac instructions in readme by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/42
- Keep opt-level = s and remove `console_error_panic_hook` by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/48
- WriterPropertiesBuilder for arrow2 by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/49
- Docstrings for public functions, structs, enums by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/50
- Compression-specific features by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/51
- Add more node tests by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/52
- Separate reader and writer features by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/47
- Docs update by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/53
- Working typedoc by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/55
- Update docstrings and readme by @kylebarron in https://github.com/kylebarron/parquet-wasm/pull/60

**Full Changelog**: https://github.com/kylebarron/parquet-wasm/compare/v0.1.1...v0.2.0

## [0.1.1] - 2022-03-06

- Attempt better bundling, with APIs for bundlers, Node, and the Web.

## [0.1.0] - 2022-03-06

- Initial release
- Barebones `read_parquet` and `write_parquet` functions.
