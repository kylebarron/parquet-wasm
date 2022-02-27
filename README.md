# `parquet-wasm`

A light binding from Rust's `parquet` crate to Web Assembly.

## Usage

- `read_parquet`: Pass in the bytes from a loaded parquet file. Returns data in Arrow IPC Stream format.

## Credits

This relied heavily on [`read-parquet-browser`](https://github.com/my-liminal-space/read-parquet-browser) under the MIT/Apache 2 license.
