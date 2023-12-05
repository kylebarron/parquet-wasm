use std::convert::TryInto;

use futures::channel::oneshot;
use futures::future::BoxFuture;
use range_reader::{RangeOutput, RangedAsyncReader};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;

/// Get content-length of file
pub async fn _get_content_length(url: String) -> Result<usize, reqwest::Error> {
    let client = reqwest::Client::new();
    let resp = client.head(url).send().await?;
    Ok(resp.content_length().unwrap().try_into().unwrap())
}

pub async fn get_content_length(url: String) -> Result<usize, reqwest::Error> {
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
    format!("bytes={}-{}", start, start + length - 1)
}

pub fn range_from_start(start: u64) -> String {
    format!("bytes={}-", start)
}

pub fn range_from_end(length: u64) -> String {
    format!("bytes=-{}", length)
}

/// Make range request on remote file
async fn _make_range_request(
    url: &str,
    start: u64,
    length: usize,
) -> Result<Vec<u8>, reqwest::Error> {
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

/// Create a RangedAsyncReader
pub fn create_reader(
    url: String,
    content_length: usize,
    min_request_size: Option<usize>,
) -> RangedAsyncReader {
    // at least 4kb per s3 request. Adjust to your liking.
    let min_request_size = min_request_size.unwrap_or(4 * 1024);

    // Closure for making an individual HTTP range request to a file
    let range_get = Box::new(move |start: u64, length: usize| {
        let url = url.clone();

        Box::pin(async move {
            let data = make_range_request(url.clone(), start, length)
                .await
                .unwrap();
            Ok(RangeOutput { start, data })
        }) as BoxFuture<'static, std::io::Result<RangeOutput>>
    });

    RangedAsyncReader::new(content_length, min_request_size, range_get)
}
