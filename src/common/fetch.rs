use std::convert::TryInto;

use futures::channel::oneshot;
use wasm_bindgen::prelude::*;

use wasm_bindgen_futures::spawn_local;

/// Get content-length of file
pub async fn _get_content_length(url: String) -> Result<usize, JsValue> {
    let client = reqwest::Client::new();
    let resp = client.head(url).send().await?;
    Ok(resp.content_length().unwrap().try_into().unwrap())
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
    let client = reqwest::Client::new();
    let range_str = range_from_start_and_length(start, length as u64);
    let resp = client
        .get(url)
        .header("Range", range_str)
        .send()
        .await?
        .error_for_status()?;
    Ok(resp.bytes().await?.to_vec())
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
