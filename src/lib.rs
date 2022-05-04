extern crate web_sys;

#[cfg(feature = "arrow1")]
pub mod arrow1;

#[cfg(feature = "arrow2")]
pub mod arrow2;

pub mod common;
pub mod fetch;
pub mod utils;

// When the `wee_alloc` feature is enabled, use `wee_alloc` as the global
// allocator.
/*#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;*/
