extern crate web_sys;

pub mod common;
pub mod utils;

pub mod error;
pub mod metadata;
#[cfg(feature = "reader")]
pub mod reader;
#[cfg(all(feature = "reader", feature = "async"))]
pub mod reader_async;
pub mod wasm;
#[cfg(feature = "writer")]
pub mod writer;
#[cfg(feature = "writer")]
pub mod writer_properties;

// When the `wee_alloc` feature is enabled, use `wee_alloc` as the global
// allocator.
/*#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;*/
