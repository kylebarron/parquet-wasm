//! An asynchronous Parquet reader that is able to read and inspect remote files without
//! downloading them in entirety.

use crate::common::fetch::{
    create_reader, get_content_length, range_from_end, range_from_start_and_length,
};
use crate::error::{ParquetWasmError, Result, WasmResult};
use futures::channel::oneshot;
use futures::future::BoxFuture;
use js_sys::Object;
use object_store::ObjectStore;
use object_store_wasm::parse::{parse_url, parse_url_opts};
use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;
use url::Url;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;

use arrow::ipc::writer::StreamWriter;
use arrow_wasm::{RecordBatch, Table};
use bytes::Bytes;
use futures::TryStreamExt;
use futures::{stream, FutureExt, StreamExt};
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::arrow::async_reader::{
    AsyncFileReader, ParquetObjectReader, ParquetRecordBatchStream, ParquetRecordBatchStreamBuilder,
};

use async_compat::{Compat, CompatExt};
use parquet::file::footer::{decode_footer, decode_metadata};
use parquet::file::metadata::{FileMetaData, ParquetMetaData};
use range_reader::RangedAsyncReader;
use reqwest::Client;

use async_trait::async_trait;

#[async_trait(?Send)]
trait SharedIO<T: AsyncFileReader + Unpin + Clone + 'static> {
    fn generate_builder(
        reader: &T,
        meta: &ArrowReaderMetadata,
        batch_size: &usize,
        projection_mask: &Option<ProjectionMask>,
    ) -> ParquetRecordBatchStreamBuilder<T> {
        let builder =
            ParquetRecordBatchStreamBuilder::new_with_metadata(reader.clone(), meta.clone())
                .with_batch_size(*batch_size)
                .with_projection(
                    projection_mask
                        .as_ref()
                        .unwrap_or(&ProjectionMask::all())
                        .clone(),
                );
        builder
    }

    async fn inner_read_row_group(
        &self,
        reader: &T,
        meta: &ArrowReaderMetadata,
        batch_size: &usize,
        projection_mask: &Option<ProjectionMask>,
        i: usize,
    ) -> Result<Table> {
        let builder = Self::generate_builder(reader, meta, batch_size, projection_mask);
        let schema = builder.schema().clone();
        let stream = builder.with_row_groups(vec![i]).build()?;
        let results = stream.try_collect::<Vec<_>>().await.unwrap();

        // NOTE: This is not only one batch by default due to arrow-rs's default rechunking.
        // assert_eq!(results.len(), 1, "Expected one record batch");
        // Ok(RecordBatch::new(results.pop().unwrap()))
        Ok(Table::new(schema, results))
    }

    async fn inner_stream(
        &self,
        concurrency: Option<usize>,
        meta: &ArrowReaderMetadata,
        reader: &T,
        batch_size: &usize,
        projection_mask: &Option<ProjectionMask>,
    ) -> WasmResult<wasm_streams::readable::sys::ReadableStream> {
        use futures::StreamExt;
        let concurrency = concurrency.unwrap_or(1);
        let meta = meta.clone();
        let reader = reader.clone();
        let batch_size = *batch_size;
        let num_row_groups = meta.metadata().num_row_groups();
        let projection_mask = projection_mask.clone();
        let buffered_stream = stream::iter((0..num_row_groups).map(move |i| {
            let builder = Self::generate_builder(&reader, &meta, &batch_size, &projection_mask)
                .with_row_groups(vec![i]);
            builder.build().unwrap().try_collect::<Vec<_>>()
        }))
        .buffered(concurrency);
        let out_stream = buffered_stream.flat_map(|maybe_record_batches| {
            stream::iter(maybe_record_batches.unwrap())
                .map(|record_batch| Ok(RecordBatch::new(record_batch).into()))
        });
        Ok(wasm_streams::ReadableStream::from_stream(out_stream).into_raw())
    }
}

#[wasm_bindgen]
pub struct AsyncParquetFile {
    reader: ParquetObjectReader,
    meta: ArrowReaderMetadata,
    batch_size: usize,
    projection_mask: Option<ProjectionMask>,
}

impl SharedIO<HTTPFileReader> for AsyncParquetFile {}
impl SharedIO<ParquetObjectReader> for AsyncParquetFile {}

#[wasm_bindgen]
impl AsyncParquetFile {
    #[wasm_bindgen(constructor)]
    pub async fn new(url: String, options: Option<Object>) -> WasmResult<AsyncParquetFile> {
        let parsed_url = Url::parse(&url)?;
        let (storage_container, path) = match options {
            Some(options) => {
                let deserialized_options: HashMap<String, String> =
                    serde_wasm_bindgen::from_value(options.into())?;
                parse_url_opts(&parsed_url, deserialized_options.iter())?
            }
            None => parse_url(&parsed_url)?,
        };
        let file_meta = storage_container.head(&path).await?;
        let mut reader = ParquetObjectReader::new(storage_container.into(), file_meta);
        let meta = ArrowReaderMetadata::load_async(&mut reader, Default::default()).await?;
        Ok(Self {
            reader,
            meta,
            projection_mask: None,
            batch_size: 1024,
        })
    }

    #[wasm_bindgen(js_name = withBatchSize)]
    pub fn with_batch_size(self, batch_size: usize) -> Self {
        Self { batch_size, ..self }
    }

    #[wasm_bindgen(js_name = selectColumns)]
    pub fn select_columns(self, columns: Vec<String>) -> WasmResult<AsyncParquetFile> {
        let pq_schema = self.meta.parquet_schema();
        let projection_mask = Some(generate_projection_mask(columns, pq_schema)?);
        Ok(Self {
            projection_mask,
            ..self
        })
    }

    #[wasm_bindgen]
    pub fn metadata(&self) -> WasmResult<crate::metadata::ParquetMetaData> {
        Ok(self.meta.metadata().as_ref().to_owned().into())
    }

    #[wasm_bindgen(js_name = readRowGroup)]
    pub async fn read_row_group(&self, i: usize) -> WasmResult<Table> {
        let inner = self
            .inner_read_row_group(
                &self.reader,
                &self.meta,
                &self.batch_size,
                &self.projection_mask,
                i,
            )
            .await
            .unwrap();
        Ok(inner)
    }

    #[wasm_bindgen]
    pub async fn stream(
        &self,
        concurrency: Option<usize>,
    ) -> WasmResult<wasm_streams::readable::sys::ReadableStream> {
        self.inner_stream(
            concurrency,
            &self.meta,
            &self.reader,
            &self.batch_size,
            &self.projection_mask,
        )
        .await
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

/// Safety: Do not use this in a multi-threaded environment,
/// (transitively depends on !Send web_sys::File)
#[wasm_bindgen]
pub struct AsyncParquetLocalFile {
    reader: JsFileReader,
    meta: ArrowReaderMetadata,
    batch_size: usize,
    projection_mask: Option<ProjectionMask>,
}

impl SharedIO<JsFileReader> for AsyncParquetLocalFile {}

#[wasm_bindgen]
impl AsyncParquetLocalFile {
    #[wasm_bindgen(constructor)]
    pub async fn new(handle: web_sys::File) -> WasmResult<AsyncParquetLocalFile> {
        let mut reader = JsFileReader::new(handle, 1024);
        let meta = ArrowReaderMetadata::load_async(&mut reader, Default::default()).await?;
        Ok(Self {
            reader,
            meta,
            batch_size: 1024,
            projection_mask: None,
        })
    }

    #[wasm_bindgen(js_name = withBatchSize)]
    pub fn with_batch_size(self, batch_size: usize) -> Self {
        Self { batch_size, ..self }
    }

    #[wasm_bindgen(js_name = selectColumns)]
    pub fn select_columns(self, columns: Vec<String>) -> WasmResult<AsyncParquetLocalFile> {
        let pq_schema = self.meta.parquet_schema();
        let projection_mask = Some(generate_projection_mask(columns, pq_schema)?);
        Ok(Self {
            projection_mask,
            ..self
        })
    }

    #[wasm_bindgen]
    pub fn metadata(&self) -> WasmResult<crate::metadata::ParquetMetaData> {
        Ok(self.meta.metadata().as_ref().to_owned().into())
    }

    #[wasm_bindgen(js_name = readRowGroup)]
    pub async fn read_row_group(&self, i: usize) -> WasmResult<Table> {
        let inner = self
            .inner_read_row_group(
                &self.reader,
                &self.meta,
                &self.batch_size,
                &self.projection_mask,
                i,
            )
            .await
            .unwrap();
        Ok(inner)
    }

    #[wasm_bindgen]
    pub async fn stream(
        &self,
        concurrency: Option<usize>,
    ) -> WasmResult<wasm_streams::readable::sys::ReadableStream> {
        self.inner_stream(
            concurrency,
            &self.meta,
            &self.reader,
            &self.batch_size,
            &self.projection_mask,
        )
        .await
    }
}

#[derive(Debug, Clone)]
struct WrappedFile {
    inner: web_sys::File,
    pub size: f64,
}
/// Safety: This is not in fact thread-safe. Do not attempt to use this in work-stealing
/// async runtimes / multi-threaded environments
///
/// web_sys::File objects, like all JSValues, are !Send (even in JS, there's
/// maybe ~5 Transferable types), and eventually boil down to PhantomData<*mut u8>.
/// Any struct that holds one is inherently !Send, which disqualifies it from being used
/// with the AsyncFileReader trait.
unsafe impl Send for WrappedFile {}

impl WrappedFile {
    pub fn new(inner: web_sys::File) -> Self {
        let size = inner.size();
        Self { inner, size }
    }
    pub async fn get_bytes(&mut self, range: Range<usize>) -> Vec<u8> {
        use js_sys::Uint8Array;
        use wasm_bindgen_futures::JsFuture;
        let (sender, receiver) = oneshot::channel();
        let file = self.inner.clone();
        spawn_local(async move {
            let subset_blob = file
                .slice_with_i32_and_i32(
                    range.start.try_into().unwrap(),
                    range.end.try_into().unwrap(),
                )
                .unwrap();
            let buf = JsFuture::from(subset_blob.array_buffer()).await.unwrap();
            let out_vec = Uint8Array::new_with_byte_offset(&buf, 0).to_vec();
            sender.send(out_vec).unwrap();
        });

        receiver.await.unwrap()
    }
}

#[derive(Debug, Clone)]
pub struct JsFileReader {
    file: WrappedFile,
    coalesce_byte_size: usize,
}

impl JsFileReader {
    pub fn new(file: web_sys::File, coalesce_byte_size: usize) -> Self {
        Self {
            file: WrappedFile::new(file),
            coalesce_byte_size,
        }
    }
}

impl AsyncFileReader for JsFileReader {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        async move {
            let (sender, receiver) = oneshot::channel();
            let mut file = self.file.clone();
            spawn_local(async move {
                let result: Bytes = file.get_bytes(range).await.into();
                sender.send(result).unwrap()
            });
            let data = receiver.await.unwrap();
            Ok(data)
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
        // Assuming that we have a file on the local file system, these fetches should be
        // _relatively_ fast
        async move {
            let mut fetched = Vec::with_capacity(ranges.len());

            for range in fetch_ranges.iter() {
                let data = self.get_bytes(range.clone()).await?;
                fetched.push(data);
            }

            Ok(ranges
                .iter()
                .map(|range| {
                    // a given range CAN span two coalesced row group sets.
                    // log!("Range: {:?} Actual length: {:?}", range.end - range.start, res.len());
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
            // we only *really* need the last 8 bytes to determine the location of the metadata bytes
            let file_size: usize = (self.file.size as i64).try_into().unwrap();
            // we already know the size of the file!
            let suffix_range: Range<usize> = (file_size - 8)..file_size;
            let suffix = self.get_bytes(suffix_range).await.unwrap();
            let suffix_len = suffix.len();

            let mut footer = [0; 8];
            footer.copy_from_slice(&suffix[suffix_len - 8..suffix_len]);
            let metadata_byte_length = decode_footer(&footer)?;
            // Did not fetch the entire file metadata in the initial read, need to make a second request
            let meta = if metadata_byte_length > suffix_len - 8 {
                // might want to figure out how to get get_bytes to accept a one-sided range
                let meta_range = (file_size - metadata_byte_length - 8)..file_size;

                let meta_bytes = self.get_bytes(meta_range).await.unwrap();

                decode_metadata(&meta_bytes[0..meta_bytes.len() - 8])?
            } else {
                let metadata_start = suffix_len - metadata_byte_length - 8;

                let slice = &suffix[metadata_start..suffix_len - 8];
                decode_metadata(slice)?
            };
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

fn generate_projection_mask(
    columns: Vec<String>,
    pq_schema: &SchemaDescriptor,
) -> Result<ProjectionMask> {
    let col_paths = pq_schema
        .columns()
        .iter()
        .map(|col| col.path().string())
        .collect::<Vec<_>>();
    let indices: Vec<usize> = columns
        .iter()
        .map(|col| {
            let field_indices: Vec<usize> = col_paths
                .iter()
                .enumerate()
                .filter(|(_idx, path)| {
                    // identical OR the path starts with the column AND the substring is immediately followed by the
                    // path separator
                    path.to_string() == col.clone()
                        || path.starts_with(col) && {
                            let left_index = path.find(col).unwrap();
                            path.chars().nth(left_index + col.len()).unwrap() == '.'
                        }
                })
                .map(|(idx, _)| idx)
                .collect();
            if field_indices.is_empty() {
                Err(ParquetWasmError::UnknownColumn(col.clone()))
            } else {
                Ok(field_indices)
            }
        })
        .collect::<Result<Vec<Vec<usize>>>>()?
        .into_iter()
        .flatten()
        .collect();
    let projection_mask = ProjectionMask::leaves(pq_schema, indices);
    Ok(projection_mask)
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
