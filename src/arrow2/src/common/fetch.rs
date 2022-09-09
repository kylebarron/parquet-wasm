use futures::channel::oneshot;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{Request, RequestInit, RequestMode, Response};

use wasm_bindgen_futures::spawn_local;

/// Get content-length of file
pub async fn _get_content_length(url: String) -> Result<usize, JsValue> {
    let mut opts = RequestInit::new();
    opts.method("HEAD");
    opts.mode(RequestMode::Cors);

    let request = Request::new_with_str_and_init(&url, &opts)?;
    let window = web_sys::window().unwrap();

    let resp_value = JsFuture::from(window.fetch_with_request(&request)).await?;
    let resp: Response = resp_value.dyn_into().unwrap();

    let length = resp.headers().get("content-length")?;
    let a = length.unwrap();
    let length_int = a.parse::<usize>().unwrap();
    Ok(length_int)
}

pub async fn get_content_length(url: String) -> Result<usize, JsValue> {
    let (sender, receiver) = oneshot::channel::<usize>();
    spawn_local(async move {
        let inner_data = _get_content_length(url).await.unwrap();
        sender.send(inner_data).unwrap();
    });
    let data = receiver.await.unwrap();
    Ok(data)
}

/// Construct range header from start and length
pub fn range_from_start_and_length(start: u64, length: u64) -> String {
    // TODO: should this be start + length - 1?
    format!("bytes={}-{}", start, start + length)
}

pub fn range_from_start(start: u64) -> String {
    format!("bytes={}-", start)
}

pub fn range_from_end(length: u64) -> String {
    format!("bytes=-{}", length)
}

/// Make range request on remote file
async fn _make_range_request(url: &str, start: u64, length: usize) -> Result<Vec<u8>, JsValue> {
    let mut opts = RequestInit::new();
    opts.method("GET");
    opts.mode(RequestMode::Cors);

    let request = Request::new_with_str_and_init(url, &opts)?;

    request
        .headers()
        .set("Range", &range_from_start_and_length(start, length as u64))?;

    let window = web_sys::window().unwrap();
    let resp_value = JsFuture::from(window.fetch_with_request(&request)).await?;
    let resp: Response = resp_value.dyn_into().unwrap();

    Ok(resp_into_bytes(resp).await)
}

pub async fn make_range_request(
    url: String,
    start: u64,
    length: usize,
) -> Result<Vec<u8>, JsValue> {
    let (sender, receiver) = oneshot::channel::<Vec<u8>>();
    spawn_local(async move {
        let inner_data = _make_range_request(&url, start, length).await.unwrap();
        sender.send(inner_data).unwrap();
    });
    let data = receiver.await.unwrap();
    Ok(data)
}

async fn resp_into_bytes(resp: Response) -> Vec<u8> {
    let array_buffer_promise = JsFuture::from(resp.array_buffer().unwrap());
    let array_buffer: JsValue = array_buffer_promise
        .await
        .expect("Could not get ArrayBuffer from file");

    js_sys::Uint8Array::new(&array_buffer).to_vec()
}
