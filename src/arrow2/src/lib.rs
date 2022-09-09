extern crate web_sys;

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

pub mod common;
pub mod utils;

// When the `wee_alloc` feature is enabled, use `wee_alloc` as the global
// allocator.
/*#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;*/
