//! An asynchronous Parquet reader that is able to read and inspect remote files without
//! downloading them in entirety.

use crate::common::fetch::{
    create_reader, get_content_length, range_from_end, range_from_start_and_length,
};
use crate::error::{Result, WasmResult};
use crate::read_options::{JsReaderOptions, ReaderOptions};
use futures::channel::oneshot;
use futures::future::BoxFuture;
use object_store::coalesce_ranges;
use std::ops::Range;
use std::sync::Arc;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;

use arrow::ipc::writer::StreamWriter;
use arrow_wasm::{RecordBatch, Table};
use bytes::Bytes;
use futures::TryStreamExt;
use futures::{FutureExt, StreamExt, stream};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::{
    AsyncFileReader, MetadataSuffixFetch, ParquetRecordBatchStream, ParquetRecordBatchStreamBuilder,
};

use async_compat::{Compat, CompatExt};
use parquet::file::metadata::{FileMetaData, ParquetMetaData, ParquetMetaDataReader};
use range_reader::RangedAsyncReader;
use reqwest::Client;

/// Range requests with a gap less than or equal to this,
/// will be coalesced into a single request by [`coalesce_ranges`]
const OBJECT_STORE_COALESCE_DEFAULT: u64 = 1024 * 1024;

fn create_builder<T: AsyncFileReader + Unpin + Clone + 'static>(
    reader: &T,
    meta: &ArrowReaderMetadata,
    options: &JsReaderOptions,
) -> Result<ParquetRecordBatchStreamBuilder<T>> {
    let builder = ParquetRecordBatchStreamBuilder::new_with_metadata(reader.clone(), meta.clone());
    options.apply_to_builder(builder)
}

/// An abstraction over either a browser File handle or an ObjectStore instance
///
/// This allows exposing a single ParquetFile class to the user.
#[derive(Clone)]
enum InnerParquetFile {
    File(JsFileReader),
    Http(HTTPFileReader),
}

impl AsyncFileReader for InnerParquetFile {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        match self {
            Self::File(reader) => reader.get_bytes(range),
            Self::Http(reader) => reader.get_bytes(range),
        }
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        match self {
            Self::File(reader) => reader.get_byte_ranges(ranges),
            Self::Http(reader) => reader.get_byte_ranges(ranges),
        }
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        match self {
            Self::File(reader) => reader.get_metadata(options),
            Self::Http(reader) => reader.get_metadata(options),
        }
    }
}

#[wasm_bindgen]
pub struct ParquetFile {
    reader: InnerParquetFile,
    meta: ArrowReaderMetadata,
}

#[wasm_bindgen]
impl ParquetFile {
    /// Construct a ParquetFile from a new URL.
    #[wasm_bindgen(js_name = fromUrl)]
    pub async fn from_url(url: String) -> WasmResult<ParquetFile> {
        let client = Client::new();
        let mut reader = HTTPFileReader::new(url, client, OBJECT_STORE_COALESCE_DEFAULT);
        let meta = ArrowReaderMetadata::load_async(&mut reader, Default::default()).await?;
        Ok(Self {
            reader: InnerParquetFile::Http(reader),
            meta,
        })
    }

    /// Construct a ParquetFile from a new [Blob] or [File] handle.
    ///
    /// [Blob]: https://developer.mozilla.org/en-US/docs/Web/API/Blob
    /// [File]: https://developer.mozilla.org/en-US/docs/Web/API/File
    ///
    /// Safety: Do not use this in a multi-threaded environment,
    /// (transitively depends on `!Send` `web_sys::Blob`)
    #[wasm_bindgen(js_name = fromFile)]
    pub async fn from_file(handle: web_sys::Blob) -> WasmResult<ParquetFile> {
        let mut reader = JsFileReader::new(handle, 1024);
        let meta = ArrowReaderMetadata::load_async(&mut reader, Default::default()).await?;
        Ok(Self {
            reader: InnerParquetFile::File(reader),
            meta,
        })
    }

    #[wasm_bindgen]
    pub fn metadata(&self) -> WasmResult<crate::metadata::ParquetMetaData> {
        Ok(self.meta.metadata().as_ref().to_owned().into())
    }

    #[wasm_bindgen]
    pub fn schema(&self) -> WasmResult<arrow_wasm::Schema> {
        Ok(self.meta.schema().clone().into())
    }

    /// Read from the Parquet file in an async fashion.
    ///
    /// @param options
    ///
    ///    Options for reading Parquet data. Optional keys include:
    ///
    ///    - `batchSize`: The number of rows in each batch. If not provided, the upstream parquet
    ///           default is 1024.
    ///    - `rowGroups`: Only read data from the provided row group indexes.
    ///    - `limit`: Provide a limit to the number of rows to be read.
    ///    - `offset`: Provide an offset to skip over the given number of rows.
    ///    - `columns`: The column names from the file to read.
    #[wasm_bindgen]
    pub async fn read(&self, options: Option<ReaderOptions>) -> WasmResult<Table> {
        let options = options
            .map(|x| x.try_into())
            .transpose()?
            .unwrap_or_default();
        let builder = create_builder(&self.reader, &self.meta, &options)?;

        let schema = builder.schema().clone();
        let stream = builder.build()?;
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();

        Ok(Table::new(schema, batches))
    }

    /// Create a readable stream of record batches.
    ///
    /// Each item in the stream will be a {@linkcode RecordBatch}.
    ///
    /// @param options
    ///
    ///    Options for reading Parquet data. Optional keys include:
    ///
    ///    - `batchSize`: The number of rows in each batch. If not provided, the upstream parquet
    ///           default is 1024.
    ///    - `rowGroups`: Only read data from the provided row group indexes.
    ///    - `limit`: Provide a limit to the number of rows to be read.
    ///    - `offset`: Provide an offset to skip over the given number of rows.
    ///    - `columns`: The column names from the file to read.
    ///    - `concurrency`: The number of concurrent requests to make
    #[wasm_bindgen]
    pub async fn stream(
        &self,
        options: Option<ReaderOptions>,
    ) -> WasmResult<wasm_streams::readable::sys::ReadableStream> {
        let options: JsReaderOptions = options
            .map(|x| x.try_into())
            .transpose()?
            .unwrap_or_default();

        let concurrency = options.concurrency.unwrap_or_default().max(1);
        let row_groups = options
            .row_groups
            .clone()
            .unwrap_or_else(|| (0..self.meta.metadata().num_row_groups()).collect());
        let reader = self.reader.clone();
        let meta = self.meta.clone();

        let buffered_stream = stream::iter(row_groups.into_iter().map(move |i| {
            let builder = create_builder(&reader.clone(), &meta.clone(), &options.clone())
                .unwrap()
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

#[derive(Debug, Clone)]
pub struct HTTPFileReader {
    url: String,
    client: Client,
    coalesce_byte_size: u64,
}

impl HTTPFileReader {
    pub fn new(url: String, client: Client, coalesce_byte_size: u64) -> Self {
        Self {
            url,
            client,
            coalesce_byte_size,
        }
    }
}

impl MetadataSuffixFetch for &mut HTTPFileReader {
    fn fetch_suffix(&mut self, suffix: usize) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        async move {
            let range_str = range_from_end(suffix);

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
}

async fn get_bytes_http(
    url: String,
    client: Client,
    range: Range<u64>,
) -> parquet::errors::Result<Bytes> {
    let range_str = range_from_start_and_length(range.start, range.end - range.start);

    // Map reqwest error to parquet error
    // let map_err = |err| parquet::errors::ParquetError::External(Box::new(err));

    let bytes = make_range_request_with_client(url, client, range_str)
        .await
        .unwrap();

    Ok(bytes)
}

impl AsyncFileReader for HTTPFileReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        get_bytes_http(self.url.clone(), self.client.clone(), range).boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        async move {
            coalesce_ranges(
                &ranges,
                |range| get_bytes_http(self.url.clone(), self.client.clone(), range),
                self.coalesce_byte_size,
            )
            .await
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        async move {
            let metadata = ParquetMetaDataReader::new()
                .with_page_indexes(true)
                .load_via_suffix_and_finish(self)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

#[derive(Debug, Clone)]
struct WrappedFile {
    inner: web_sys::Blob,
    pub size: u64,
}
/// Safety: This is not in fact thread-safe. Do not attempt to use this in work-stealing
/// async runtimes / multi-threaded environments
///
/// web_sys::Blob objects, like all JSValues, are !Send (even in JS, there's
/// maybe ~5 Transferable types), and eventually boil down to PhantomData<*mut u8>.
/// Any struct that holds one is inherently !Send, which disqualifies it from being used
/// with the AsyncFileReader trait.
unsafe impl Send for WrappedFile {}
unsafe impl Sync for WrappedFile {}

impl WrappedFile {
    pub fn new(inner: web_sys::Blob) -> Self {
        let size = inner.size() as u64;
        Self { inner, size }
    }

    pub async fn get_bytes(&mut self, range: Range<u64>) -> Vec<u8> {
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

async fn get_bytes_file(
    mut file: WrappedFile,
    range: Range<u64>,
) -> parquet::errors::Result<Bytes> {
    let (sender, receiver) = oneshot::channel();
    spawn_local(async move {
        let result: Bytes = file.get_bytes(range).await.into();
        sender.send(result).unwrap()
    });
    let data = receiver.await.unwrap();
    Ok(data)
}

#[derive(Debug, Clone)]
pub struct JsFileReader {
    file: WrappedFile,
    coalesce_byte_size: u64,
}

impl JsFileReader {
    pub fn new(file: web_sys::Blob, coalesce_byte_size: u64) -> Self {
        Self {
            file: WrappedFile::new(file),
            coalesce_byte_size,
        }
    }
}

impl AsyncFileReader for JsFileReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
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
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        async move {
            coalesce_ranges(
                &ranges,
                |range| get_bytes_file(self.file.clone(), range),
                self.coalesce_byte_size,
            )
            .await
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        let file_size = self.file.size;
        async move {
            let metadata = ParquetMetaDataReader::new()
                .with_page_indexes(true)
                .load_and_finish(self, file_size)
                .await?;
            Ok(Arc::new(metadata))
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
