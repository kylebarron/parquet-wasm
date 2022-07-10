use arrow2::error::Error as ArrowError;
use arrow2::io::ipc::write::{StreamWriter as IPCStreamWriter, WriteOptions as IPCWriteOptions};
use arrow2::io::parquet::read::FileReader as ParquetFileReader;
use arrow2::io::parquet::read::{infer_schema, FileMetaData};
use futures::channel::oneshot;
use std::io::Cursor;

use futures::future::BoxFuture;
use parquet2::read::read_metadata_async as _read_metadata_async;
use range_reader::{RangeOutput, RangedAsyncReader};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;

use crate::common::fetch::make_range_request;

use crate::log;

use arrow2::error::Result as ArrowResult;
use arrow2::io::parquet::read::{read_columns_many_async, RowGroupDeserializer};

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

pub fn create_reader(url: String, content_length: usize) -> RangedAsyncReader {
    let range_get = Box::new(move |start: u64, length: usize| {
        let url = url.clone();

        Box::pin(async move {
            let (sender2, receiver2) = oneshot::channel::<Vec<u8>>();
            spawn_local(async move {
                log!("Making range request");
                let inner_data = make_range_request(url, start, length).await.unwrap();
                sender2.send(inner_data).unwrap();
            });
            let data = receiver2.await.unwrap();

            Ok(RangeOutput { start, data })
        }) as BoxFuture<'static, std::io::Result<RangeOutput>>
    });

    // at least 4kb per s3 request. Adjust to your liking.
    RangedAsyncReader::new(content_length, 4 * 1024, range_get)
}

pub async fn read_metadata_async(
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
                sender2.send(inner_data).unwrap();
            });
            let data = receiver2.await.unwrap();

            Ok(RangeOutput { start, data })
        }) as BoxFuture<'static, std::io::Result<RangeOutput>>
    });

    // at least 4kb per s3 request. Adjust to your liking.
    let mut reader = RangedAsyncReader::new(content_length, 4 * 1024, range_get);

    let metadata = _read_metadata_async(&mut reader).await.unwrap();
    log!("Number of rows: {}", metadata.num_rows);

    Ok(metadata)
}

pub async fn read_row_group(
    url: String,
    content_length: usize,
    metadata: &FileMetaData,
    i: usize,
) -> Result<Vec<u8>, ArrowError> {
    // Closure for making an individual HTTP range request to a file
    let range_get = Box::new(move |start: u64, length: usize| {
        let url = url.clone();

        Box::pin(async move {
            let (local_oneshot_sender, local_oneshot_receiver) = oneshot::channel::<Vec<u8>>();
            spawn_local(async move {
                log!("Making range request");
                let inner_data = make_range_request(url, start, length).await.unwrap();
                local_oneshot_sender.send(inner_data).unwrap();
            });
            let data = local_oneshot_receiver.await.unwrap();

            Ok(RangeOutput { start, data })
        }) as BoxFuture<'static, std::io::Result<RangeOutput>>
    });

    let min_request_size = 4 * 1024;

    let reader_factory = || {
        Box::pin(futures::future::ready(Ok(RangedAsyncReader::new(
            content_length,
            min_request_size,
            range_get.clone(),
        )))) as BoxFuture<'static, std::result::Result<RangedAsyncReader, std::io::Error>>
    };

    // let's read the first row group only. Iterate over them to your liking
    let group = &metadata.row_groups[i];

    // no chunk size in deserializing
    let chunk_size = None;

    let schema = infer_schema(metadata)?;
    let fields = schema.fields.clone();

    // this is IO-bounded (and issues a join, thus the reader_factory)
    let column_chunks = read_columns_many_async(reader_factory, group, fields, chunk_size).await?;

    // Create IPC writer
    let mut output_file = Vec::new();
    let options = IPCWriteOptions { compression: None };
    let mut writer = IPCStreamWriter::new(&mut output_file, options);
    writer.start(&schema, None)?;

    // this is CPU-bounded and should be sent to a separate thread-pool.
    // We do it here for simplicity
    let chunks = RowGroupDeserializer::new(column_chunks, group.num_rows() as usize, None);
    let chunks = chunks.collect::<ArrowResult<Vec<_>>>()?;
    for chunk in chunks {
        // let chunk2 = chunk;
        writer.write(&chunk, None)?;
    }

    writer.finish()?;
    Ok(output_file)
}
