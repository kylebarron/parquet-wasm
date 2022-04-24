use arrow2::error::ArrowError;
use arrow2::io::ipc::write::{StreamWriter as IPCStreamWriter, WriteOptions as IPCWriteOptions};
// NOTE: It's FileReader on latest main but RecordReader in 0.9.2
use arrow2::io::parquet::read::FileReader as ParquetFileReader;
use js_sys::ArrayBuffer;
use std::io::Cursor;
use tokio::sync::oneshot;

use arrow2::array::{Array, Int64Array};
use arrow2::datatypes::DataType;
// use arrow2::error::Result;
use arrow2::io::parquet::read;
use futures::{future::BoxFuture, StreamExt};
use parquet2::read::read_metadata_async;
use range_reader::{RangeOutput, RangedAsyncReader};
// use crate::arrow2::ranged_reader::{RangeOutput, RangedAsyncReader};
// use s3::Bucket;

use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::{JsFuture, spawn_local};
use wasm_rs_async_executor::single_threaded as executor;
use web_sys::{Request, RequestInit, RequestMode, Response};

/// Internal function to read a buffer with Parquet data into a buffer with Arrow IPC Stream data
/// using the arrow2 and parquet2 crates
pub fn read_parquet(parquet_file: &[u8]) -> Result<Vec<u8>, ArrowError> {
    // Create Parquet reader
    let input_file = Cursor::new(parquet_file);
    let file_reader = ParquetFileReader::try_new(input_file, None, None, None, None)?;
    let schema = file_reader.schema().clone();

    // Create IPC writer
    let mut output_file = Vec::new();
    let options = IPCWriteOptions { compression: None };
    let mut writer = IPCStreamWriter::new(&mut output_file, options);
    writer.start(&schema, None)?;

    // Iterate over reader chunks, writing each into the IPC writer
    for maybe_chunk in file_reader {
        let chunk = maybe_chunk?;
        writer.write(&chunk, None)?;
    }

    writer.finish()?;
    Ok(output_file)
}

async fn resp_into_bytes(resp: Response) -> Vec<u8> {
    let array_buffer_promise = JsFuture::from(resp.array_buffer().unwrap());
    let array_buffer: JsValue = array_buffer_promise
        .await
        .expect("Could not get ArrayBuffer from file");

    js_sys::Uint8Array::new(&array_buffer).to_vec()
}

async fn make_range_request(url: String, start: u64, length: usize) -> Result<Vec<u8>, JsValue> {
    let mut opts = RequestInit::new();
    opts.method("GET");
    opts.mode(RequestMode::Cors);

    let request = Request::new_with_str_and_init(&url, &opts)?;

    request.headers().set(
        "Range",
        format!("bytes={}-{}", start, start + length as u64).as_str(),
    )?;

    let window = web_sys::window().unwrap();
    let resp_value = JsFuture::from(window.fetch_with_request(&request)).await?;
    let resp: Response = resp_value.dyn_into().unwrap();

    Ok(resp_into_bytes(resp).await)
}

pub async fn get_content_length(url: String) -> Result<usize, JsValue> {
    let mut opts = RequestInit::new();
    opts.method("HEAD");
    opts.mode(RequestMode::Cors);

    let request = Request::new_with_str_and_init(&url, &opts)?;
    let window = web_sys::window().unwrap();
    let resp_value = JsFuture::from(window.fetch_with_request(&request)).await?;
    let resp: Response = resp_value.dyn_into().unwrap();
    let length = resp.headers().get("content-length")?;
    let a = length.unwrap();
    let lengthInt = a.parse::<usize>().unwrap();
    Ok(lengthInt)

    // log!("{lengthInt:?}");
    // // log!("{length:?}");
    // log!("{resp:?}");

    // Ok(())
}

#[wasm_bindgen]
pub async fn read_parquet_metadata_async(parquet_file_url: String) -> Result<(), JsValue> {
    let length = get_content_length(parquet_file_url).await.unwrap();

    // let (sender1, receiver1) = oneshot::channel();
    // let (sender2, receiver2) = oneshot::channel();
    // let task1 = executor::spawn(async move {
    //     dbg!("task 1 awaiting");
    //     let _ = receiver1.await;
    //     dbg!("task 1 -> task 2");
    //     let _ = sender2.send(());
    //     let element = web_sys::window()
    //         .unwrap()
    //         .document()
    //         .unwrap()
    //         .get_element_by_id("display")
    //         .unwrap();

    //     let mut ctr = 0u8;
    //     while ctr < 255 {
    //         element.set_inner_html(&format!("{}", ctr));
    //         ctr = ctr.wrapping_add(1);
    //         executor::yield_animation_frame().await;
    //     }
    // });

    let range_get = Box::new(move |start: u64, length: usize| {
        let url = parquet_file_url.clone();

        Box::pin(async move {
            let url = url.clone();
            let data = make_range_request(url, start, length).await.unwrap();

            Ok(RangeOutput { start, data })
        }) as BoxFuture<'static, std::io::Result<RangeOutput>>
    });

    // at least 4kb per s3 request. Adjust to your liking.
    let mut reader = RangedAsyncReader::new(length, 4 * 1024, range_get);

    let metadata = read_metadata_async(&mut reader).await.unwrap();

    Ok(())
}

// #[tokio::main]
// async fn main() -> Result<()> {

//     // metadata
//     println!("{}", metadata.num_rows);

//     // pages of the first row group and first column
//     // This is IO bounded and SHOULD be done in a shared thread pool (e.g. Tokio)
//     let column_metadata = &metadata.row_groups[0].columns()[0];
//     let pages = get_page_stream(column_metadata, &mut reader, None, vec![]).await?;

//     // decompress the pages. This is CPU bounded and SHOULD be done in a dedicated thread pool (e.g. Rayon)
//     let pages = pages.map(|compressed_page| decompress(compressed_page?, &mut vec![]));

//     // deserialize the pages. This is CPU bounded and SHOULD be done in a dedicated thread pool (e.g. Rayon)
//     let array =
//         page_stream_to_array(pages, &metadata.row_groups[0].columns()[0], DataType::Int64).await?;

//     let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
//     // ... and have fun with it.
//     println!("len: {}", array.len());
//     println!("null_count: {}", array.null_count());
//     Ok(())
// }
