extern crate web_sys;

#[cfg(feature = "arrow1")]
mod arrow1;

#[cfg(feature = "arrow2")]
mod arrow2;

mod common;
mod utils;

use wasm_bindgen::prelude::*;

// When the `wee_alloc` feature is enabled, use `wee_alloc` as the global
// allocator.
/*#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;*/

#[wasm_bindgen(js_name = setPanicHook)]
pub fn set_panic_hook() {
    utils::set_panic_hook();
}
