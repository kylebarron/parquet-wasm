//! Test suite for the Web and headless browsers.

#![cfg(target_arch = "wasm32")]
// Necessary for the assert_eq! which now fails clippy
#![allow(clippy::eq_op)]

extern crate wasm_bindgen_test;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
fn pass() {
    assert_eq!(1 + 1, 2);
}
