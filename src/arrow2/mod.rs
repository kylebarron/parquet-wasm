#[cfg(feature = "reader")]
pub mod ffi;

#[cfg(feature = "reader")]
pub mod reader;

#[cfg(all(feature = "reader", feature = "async"))]
pub mod reader_async;

#[cfg(feature = "reader")]
pub mod metadata;

#[cfg(feature = "reader")]
pub mod schema;

pub mod wasm;

#[cfg(feature = "writer")]
pub mod writer;

#[cfg(feature = "writer")]
pub mod writer_properties;

pub mod error;
