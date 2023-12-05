use futures::channel::oneshot;
use futures::future::BoxFuture;
use parquet::arrow::ProjectionMask;
use std::ops::Range;
use std::sync::Arc;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;

use crate::arrow1::error::{Result, WasmResult, ParquetWasmError};
use crate::common::fetch::{
    create_reader, get_content_length, range_from_end, range_from_start_and_length,
};

use arrow::ipc::writer::StreamWriter;
use arrow_wasm::arrow1::{RecordBatch, Table};
use bytes::Bytes;
use futures::TryStreamExt;
use futures::{stream, FutureExt, StreamExt};
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::arrow::async_reader::{
    AsyncFileReader, ParquetRecordBatchStream, ParquetRecordBatchStreamBuilder,
};

use async_compat::{Compat, CompatExt};
use parquet::file::footer::{decode_footer, decode_metadata};
use parquet::file::metadata::{FileMetaData, ParquetMetaData};
use range_reader::RangedAsyncReader;
use reqwest::Client;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct AsyncParquetFile {
    reader: HTTPFileReader,
    meta: ArrowReaderMetadata,
    batch_size: usize,
    projection_mask: Option<ProjectionMask>,
}

#[wasm_bindgen]
impl AsyncParquetFile {
    #[wasm_bindgen(constructor)]
    pub async fn new(url: String) -> WasmResult<AsyncParquetFile> {
        let client = Client::new();
        let mut reader = HTTPFileReader::new(url.clone(), client.clone(), 1024);
        let meta = ArrowReaderMetadata::load_async(&mut reader, Default::default()).await?;
        Ok(Self { reader, meta, projection_mask: None, batch_size: 1024 })
    }
    #[wasm_bindgen]
    pub fn with_batch_size(self, batch_size: usize) -> Self {
        Self { batch_size, ..self }
    }
    #[wasm_bindgen]
    pub fn select_columns(self, columns: Vec<String>) -> WasmResult<AsyncParquetFile> {
        let pq_schema = self.meta.parquet_schema();
        let col_paths = pq_schema.columns().iter().map(|col| col.path().string()).collect::<Vec<_>>();
        let indices: Vec<usize> = columns.iter().map(|col| {
            let field_indices: Vec<usize> = col_paths.iter().enumerate().filter(|(idx, path)| {
                // identical OR the path starts with the column AND the substring is immediately followed by the
                // path separator
                path.to_string() == col.clone() || path.starts_with(col) && {
                    let left_index = path.find(col).unwrap();
                    path.chars().nth(left_index + col.len()).unwrap() == '.'
                }
            }).map(|(idx, _)| idx).collect();
            if field_indices.is_empty() {
                Err(ParquetWasmError::UnknownColumn(col.clone()))
            } else {
                Ok(field_indices)
            }
        }).collect::<Result<Vec<Vec<usize>>>>()?.into_iter().flatten().collect();
        let projection_mask = Some(ProjectionMask::leaves(pq_schema, indices));
        Ok(Self { projection_mask, ..self })
    }

    #[wasm_bindgen]
    pub fn metadata(&self) -> WasmResult<crate::arrow1::metadata::ParquetMetaData> {
        Ok(self.meta.metadata().as_ref().to_owned().into())
    }

    #[wasm_bindgen]
    pub async fn read_row_group(&self, i: usize) -> WasmResult<Table> {
        let builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
            self.reader.clone(),
            self.meta.clone(),
        )
        .with_batch_size(self.batch_size)
        .with_projection(self.projection_mask.as_ref().unwrap_or(&ProjectionMask::all()).clone());
        let stream = builder.with_row_groups(vec![i]).build()?;
        let results = stream.try_collect::<Vec<_>>().await.unwrap();

        // NOTE: This is not only one batch by default due to arrow-rs's default rechunking.
        // assert_eq!(results.len(), 1, "Expected one record batch");
        // Ok(RecordBatch::new(results.pop().unwrap()))
        Ok(Table::new(results))
    }
    #[wasm_bindgen]
    pub async fn stream(&self, concurrency: Option<usize>) -> WasmResult<wasm_streams::readable::sys::ReadableStream> {
        use futures::StreamExt;
        let concurrency = concurrency.unwrap_or(1);
        let meta = self.meta.clone();
        let reader = self.reader.clone();
        let batch_size = self.batch_size.clone();
        let num_row_groups = self.meta.metadata().num_row_groups();
        let projection_mask = self.projection_mask.as_ref().unwrap_or(&ProjectionMask::all()).clone();
        let buffered_stream = stream::iter((0..num_row_groups).map(move |i| {
            let builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
                reader.clone(),
                meta.clone(),
            )
            .with_batch_size(batch_size)
            .with_projection(projection_mask.clone())
            .with_row_groups(vec![i]);
            builder.build().unwrap().try_collect::<Vec<_>>()
        })).buffered(concurrency);
        let out_stream = buffered_stream.flat_map(|maybe_record_batches| {
            stream::iter(maybe_record_batches.unwrap()).map(|record_batch| {
                Ok(RecordBatch::new(record_batch).into())
            })
        });
        Ok(wasm_streams::ReadableStream::from_stream(out_stream).into_raw())
    }
}

#[derive(Debug, Clone)]
pub struct HTTPFileReader {
    url: String,
    client: Client,
    coalesce_byte_size: usize,
}

impl HTTPFileReader {
    pub fn new(url: String, client: Client, coalesce_byte_size: usize) -> Self {
        Self {
            url,
            client,
            coalesce_byte_size,
        }
    }
}

impl AsyncFileReader for HTTPFileReader {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        async move {
            let range_str =
                range_from_start_and_length(range.start as u64, (range.end - range.start) as u64);

            // Map reqwest error to parquet error
            // let map_err = |err| parquet::errors::ParquetError::External(Box::new(err));

            let bytes = make_range_request_with_client(
                self.url.to_string(),
                self.client.clone(),
                range_str,
            )
            .await
            .unwrap();

            Ok(bytes)
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<usize>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        let fetch_ranges = merge_ranges(&ranges, self.coalesce_byte_size);

        // NOTE: This still does _sequential_ requests, but it should be _fewer_ requests if they
        // can be merged.
        async move {
            let mut fetched = Vec::with_capacity(ranges.len());

            for range in fetch_ranges.iter() {
                let data = self.get_bytes(range.clone()).await?;
                fetched.push(data);
            }

            Ok(ranges
                .iter()
                .map(|range| {
                    let idx = fetch_ranges.partition_point(|v| v.start <= range.start) - 1;
                    let fetch_range = &fetch_ranges[idx];
                    let fetch_bytes = &fetched[idx];

                    let start = range.start - fetch_range.start;
                    let end = range.end - fetch_range.start;
                    fetch_bytes.slice(start..end)
                })
                .collect())
        }
        .boxed()
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        async move {
            let meta = fetch_parquet_metadata(self.url.as_str(), &self.client, None).await?;
            Ok(Arc::new(meta))
        }
        .boxed()
    }
}

pub async fn make_range_request_with_client(
    url: String,
    client: Client,
    range_str: String,
) -> std::result::Result<Bytes, JsValue> {
    let (sender, receiver) = oneshot::channel();
    spawn_local(async move {
        let resp = client
            .get(url)
            .header("Range", range_str)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
        let bytes = resp.bytes().await.unwrap();
        sender.send(bytes).unwrap();
    });
    let data = receiver.await.unwrap();
    Ok(data)
}

// async fn make_request(
//     url: &str,
//     client: &Client,
//     range: Range<usize>,
// ) -> parquet::errors::Result<Bytes> {
//     todo!()
// }

// async fn get_bytes<'a>(
//     url: &'a str,
//     client: &'a Client,
//     range: Range<usize>,
// ) -> BoxFuture<'a, parquet::errors::Result<Bytes>> {
//     async move {
//         let range_str =
//             range_from_start_and_length(range.start as u64, (range.end - range.start) as u64);

//         // Map reqwest error to parquet error
//         let map_err = |err| parquet::errors::ParquetError::External(Box::new(err));

//         let resp = client
//             .get(url)
//             .header("Range", range_str)
//             .send()
//             .await
//             .map_err(map_err)?
//             .error_for_status()
//             .map_err(map_err)?;
//         let bytes = resp.bytes().await.map_err(map_err)?;
//         Ok(bytes)
//     }
//     .boxed()
// }

// async fn get_byte_ranges<'a>(
//     url: &'a str,
//     client: &'a Client,
//     ranges: Vec<Range<usize>>,
//     coalesce_byte_size: usize,
// ) -> BoxFuture<'a, parquet::errors::Result<Vec<Bytes>>> {
//     let fetch_ranges = merge_ranges(&ranges, coalesce_byte_size);

//     let fetched: Vec<_> = futures::stream::iter(fetch_ranges.iter().cloned())
//         .map(move |range| get_bytes(url, client, range))
//         .buffered(10)
//         .try_collect()
//         .await?;

//     todo!()
//     // let bodies = stream::iter(fetch_ranges)
//     //     .map(|range| {
//     //         let client = &client;
//     //         async move {
//     //             let resp = client.get(url).send().await?;
//     //             resp.bytes().await
//     //         }
//     //     })
//     //     .buffer_unordered(10);
// }

/// Returns a sorted list of ranges that cover `ranges`
///
/// Copied from object-store
/// https://github.com/apache/arrow-rs/blob/61da64a0557c80af5bb43b5f15c6d8bb6a314cb2/object_store/src/util.rs#L132C1-L169C1
fn merge_ranges(ranges: &[Range<usize>], coalesce: usize) -> Vec<Range<usize>> {
    if ranges.is_empty() {
        return vec![];
    }

    let mut ranges = ranges.to_vec();
    ranges.sort_unstable_by_key(|range| range.start);

    let mut ret = Vec::with_capacity(ranges.len());
    let mut start_idx = 0;
    let mut end_idx = 1;

    while start_idx != ranges.len() {
        let mut range_end = ranges[start_idx].end;

        while end_idx != ranges.len()
            && ranges[end_idx]
                .start
                .checked_sub(range_end)
                .map(|delta| delta <= coalesce)
                .unwrap_or(true)
        {
            range_end = range_end.max(ranges[end_idx].end);
            end_idx += 1;
        }

        let start = ranges[start_idx].start;
        let end = range_end;
        ret.push(start..end);

        start_idx = end_idx;
        end_idx += 1;
    }

    ret
}

// Derived from:
// https://github.com/apache/arrow-rs/blob/61da64a0557c80af5bb43b5f15c6d8bb6a314cb2/parquet/src/arrow/async_reader/metadata.rs#L54-L57
pub async fn fetch_parquet_metadata(
    url: &str,
    client: &Client,
    prefetch: Option<usize>,
) -> parquet::errors::Result<ParquetMetaData> {
    let suffix_length = prefetch.unwrap_or(8);
    let range_str = range_from_end(suffix_length as u64);

    // Map reqwest error to parquet error
    // let map_err = |err| parquet::errors::ParquetError::External(Box::new(err));

    let suffix = make_range_request_with_client(url.to_string(), client.clone(), range_str)
        .await
        .unwrap();
    let suffix_len = suffix.len();

    let mut footer = [0; 8];
    footer.copy_from_slice(&suffix[suffix_len - 8..suffix_len]);

    let metadata_byte_length = decode_footer(&footer)?;

    // Did not fetch the entire file metadata in the initial read, need to make a second request
    let metadata = if metadata_byte_length > suffix_len - 8 {
        let metadata_range_str = range_from_end((metadata_byte_length + 8) as u64);

        let meta_bytes =
            make_range_request_with_client(url.to_string(), client.clone(), metadata_range_str)
                .await
                .unwrap();

        decode_metadata(&meta_bytes[0..meta_bytes.len() - 8])?
    } else {
        let metadata_start = suffix_len - metadata_byte_length - 8;

        let slice = &suffix[metadata_start..suffix_len - 8];
        decode_metadata(slice)?
    };

    Ok(metadata)
}

pub async fn read_metadata_async(
    url: String,
    content_length: Option<usize>,
) -> Result<FileMetaData> {
    let content_length = match content_length {
        Some(content_length) => content_length,
        None => get_content_length(url.clone()).await?,
    };
    let reader = create_reader(url, content_length, None);
    let builder = ParquetRecordBatchStreamBuilder::new(reader.compat()).await?;
    let meta = builder.metadata().file_metadata().clone();
    Ok(meta)
}

pub async fn _read_row_group(
    url: String,
    content_length: Option<usize>,
    row_group: usize,
) -> Result<(
    ParquetRecordBatchStream<Compat<RangedAsyncReader>>,
    Arc<arrow::datatypes::Schema>,
)> {
    let content_length = match content_length {
        Some(content_length) => content_length,
        None => get_content_length(url.clone()).await?,
    };
    let reader = create_reader(url, content_length, None);
    let builder = ParquetRecordBatchStreamBuilder::new(reader.compat()).await?;
    let arrow_schema = builder.schema().clone();
    let parquet_reader = builder.with_row_groups(vec![row_group]).build()?;
    Ok((parquet_reader, arrow_schema))
}

pub async fn read_row_group(
    url: String,
    row_group: usize,
    chunk_fn: impl Fn(arrow::record_batch::RecordBatch) -> arrow::record_batch::RecordBatch,
) -> Result<Vec<u8>> {
    let (mut parquet_reader, arrow_schema) = _read_row_group(url, None, row_group).await?;
    // Create IPC Writer
    let mut output_file = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut output_file, &arrow_schema)?;
        while let Some(maybe_record_batch) = parquet_reader.next().await {
            let record_batch = chunk_fn(maybe_record_batch?);
            writer.write(&record_batch)?;
        }
        writer.finish()?;
    }
    Ok(output_file)
}

pub async fn read_record_batch_stream(
    url: String,
    content_length: Option<usize>,
) -> Result<ParquetRecordBatchStream<Compat<RangedAsyncReader>>> {
    let content_length = match content_length {
        Some(_content_length) => _content_length,
        None => get_content_length(url.clone()).await?,
    };
    let reader = crate::common::fetch::create_reader(url, content_length, None);

    let builder = ParquetRecordBatchStreamBuilder::new(reader.compat()).await?;
    let parquet_reader = builder.build()?;
    Ok(parquet_reader)
}
