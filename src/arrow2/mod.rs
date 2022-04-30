#[cfg(feature = "reader")]
pub mod reader;

#[cfg(feature = "reader")]
pub mod reader_async;

#[cfg(feature = "reader")]
pub mod ranged_reader;

#[cfg(feature = "reader")]
pub mod async_parquet_file;

pub mod wasm;

#[cfg(feature = "writer")]
pub mod writer;

#[cfg(feature = "writer")]
pub mod writer_properties;
