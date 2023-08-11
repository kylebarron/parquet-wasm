use std::sync::Arc;

use crate::arrow1::error::Result;
use crate::common::fetch::{create_reader, get_content_length};

use arrow::ipc::writer::StreamWriter;
use futures::stream::{Stream, StreamExt};
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
    content_length: Option<usize>,
    row_group: usize,
) -> Result<Vec<u8>> {
    let (mut parquet_reader, arrow_schema) =
        _read_row_group(url, content_length, row_group).await?;
    // Create IPC Writer
    let mut output_file = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut output_file, &arrow_schema)?;
        while let Some(maybe_record_batch) = parquet_reader.next().await {
            let record_batch = maybe_record_batch?;
            writer.write(&record_batch)?;
        }
        writer.finish()?;
    }
    Ok(output_file)
}

pub async fn read_parquet(url: String) -> Result<Vec<u8>> {
    let length = get_content_length(url.clone()).await?;
    let reader = create_reader(url, length, None);
    let builder = ParquetRecordBatchStreamBuilder::new(reader.compat()).await?;
    // quite a few options here - projection masks, row group subselection, etc...
    let arrow_schema = builder.schema().clone();
    let parquet_reader = builder.build()?;
    let intermediate: Vec<_> = parquet_reader.collect().await;
    // Create IPC Writer
    let mut output_file = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut output_file, &arrow_schema)?;

        // Iterate over record batches, writing them to IPC stream
        for maybe_record_batch in intermediate {
            let record_batch = maybe_record_batch?;
            writer.write(&record_batch)?;
        }
        writer.finish()?;
    }

    Ok(output_file)
}
pub type BoxedVecStream =
    Box<dyn Stream<Item = std::result::Result<Vec<u8>, std::io::Error>> + Unpin + Send>;
pub async fn read_parquet_stream(url: String, content_length: usize) -> Result<BoxedVecStream> {
    let reader = create_reader(url, content_length, None);
    let builder = ParquetRecordBatchStreamBuilder::new(reader.compat()).await?;
    let arrow_schema = builder.schema().clone();
    let parquet_reader = builder.build()?;
    // preferred flow: fetch -> parquet reader stream -> ipc writer sink -> ReadableStream sink
    Ok(Box::new(parquet_reader.map(move |maybe_record_batch| {
        let record_batch = maybe_record_batch.unwrap();
        let mut intermediate_vec = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut intermediate_vec, &arrow_schema).unwrap();
            let _ = writer.write(&record_batch);
            // writer.close();
        }
        Ok::<Vec<u8>, std::io::Error>(intermediate_vec)
    })))
}
