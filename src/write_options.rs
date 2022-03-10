use js_sys::{Object, Reflect};
use arrow2::io::parquet::write::{WriteOptions as UpstreamWriteOptions};
use crate::enums::{Compression, Encoding};
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
#[derive(Copy, Clone, Debug)]
pub struct WriteOptions {
    pub compression: Compression,
    pub encoding: Encoding,
}


#[wasm_bindgen]
pub struct WriteOptions(arrow2::io::parquet::write::WriteOptions);

#[wasm_bindgen]
pub fn temp(options: &JsValue) -> Result<(), JsValue> {
    if (options.is_undefined()) {
        return Ok(());
    }

    let message: JsValue = Reflect::get(&options, &JsValue::from_str("message"))?;
    log!("{:?}", message.as_string().unwrap());
    Ok(())
}

// #[wasm_bindgen]
// impl WriteOptions {
//     #[wasm_bindgen(constructor)]
//     pub fn new(options: &JsValue) -> Self {
//         let message: JsValue = Reflect::get(&options, &JsValue::from_str("message"))?;


//         options.ok
//         Self {
//             write_statistics:
//             compression: compression,
//             encoding: encoding
//         }
//     }
// }

// # [wasm_bindgen]
// pub fn greet(ele: &JsValue, options: &JsValue) -> Result<(), JsValue> {
//   match ele.dyn_ref::<HtmlDivElement>() {
//     Some(div) => {
//       if (options.is_undefined()) {
//         div.set_inner_text("Hello from Rust");
//       } else {
//         let message: JsValue = Reflect::get(&options, &JsValue::from_str("message"))?;
//         let message = message.as_string().unwrap();
//         div.set_inner_text(&message);
//       }
//       Ok(())
//     }
//     None => Err(JsValue::from_str("ele must be a div"))
//   }
// }


