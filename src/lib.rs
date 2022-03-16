extern crate web_sys;

#[cfg(feature = "arrow")]
mod arrow1;

#[cfg(feature = "arrow2")]
mod arrow2;

mod common;
mod utils;

// When the `wee_alloc` feature is enabled, use `wee_alloc` as the global
// allocator.
/*#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;*/
