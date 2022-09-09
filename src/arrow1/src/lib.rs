extern crate web_sys;

#[cfg(feature = "reader")]
pub mod reader;

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
