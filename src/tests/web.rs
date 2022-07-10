use wasm_bindgen_test::*;
use wasm_bindgen_test::wasm_bindgen_test_configure;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
fn pass() {
    assert_eq!(1, 1);
}

#[wasm_bindgen_test]
fn pass2() {
    assert_eq!(2, 2);
}

// #[wasm_bindgen_test]
// fn fail() {
//     assert_eq!(1, 2);
// }
