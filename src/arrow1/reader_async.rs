use std::sync::Arc;

use crate::arrow1::error::Result;
use crate::common::fetch::{create_reader, get_content_length};

use arrow::ipc::writer::StreamWriter;
use futures::stream::StreamExt;
use parquet::arrow::async_reader::{ParquetRecordBatchStream, ParquetRecordBatchStreamBuilder};

use async_compat::{Compat, CompatExt};
use parquet::file::metadata::FileMetaData;
use range_reader::RangedAsyncReader;

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
    chunk_fn: impl Fn(arrow::record_batch::RecordBatch) -> arrow::record_batch::RecordBatch
) -> Result<Vec<u8>> {
    let (mut parquet_reader, arrow_schema) =
        _read_row_group(url, None, row_group).await?;
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