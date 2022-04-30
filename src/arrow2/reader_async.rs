use arrow2::error::ArrowError;
use arrow2::io::ipc::write::{StreamWriter as IPCStreamWriter, WriteOptions as IPCWriteOptions};
// NOTE: It's FileReader on latest main but RecordReader in 0.9.2
use arrow2::io::parquet::read::FileMetaData;
use arrow2::io::parquet::read::FileReader as ParquetFileReader;
use futures::channel::oneshot;
use std::io::Cursor;

use futures::future::BoxFuture;
use parquet2::read::read_metadata_async;
// use range_reader::{RangeOutput, RangedAsyncReader};
use crate::arrow2::ranged_reader::{RangeOutput, RangedAsyncReader};

use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;

use crate::fetch::make_range_request;

use crate::log;

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

pub async fn read_parquet_metadata_async(
    url: String,
    content_length: usize,
) -> Result<FileMetaData, JsValue> {
    let range_get = Box::new(move |start: u64, length: usize| {
        let url = url.clone();

        Box::pin(async move {
            let (sender2, receiver2) = oneshot::channel::<Vec<u8>>();
            spawn_local(async move {
                log!("Making range request");
                let inner_data = make_range_request(url, start, length).await.unwrap();
                log!("Got data: {:?}", inner_data);
                sender2.send(inner_data).unwrap();
            });
            let data = receiver2.await.unwrap();

            Ok(RangeOutput { start, data })
        }) as BoxFuture<'static, std::io::Result<RangeOutput>>
    });

    // at least 4kb per s3 request. Adjust to your liking.
    let mut reader = RangedAsyncReader::new(content_length, 4 * 1024, range_get);

    let metadata = read_metadata_async(&mut reader).await.unwrap();
    log!("Number of rows: {}", metadata.num_rows);

    Ok(metadata)
}
