use crate::arrow1::error::Result;
use crate::common::fetch::{create_reader, get_content_length};
use crate::log;

use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatchReader;
use futures::stream::StreamExt;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;

use async_compat::CompatExt;

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
    let mut writer = StreamWriter::try_new(&mut output_file, &arrow_schema)?;

    // Iterate over record batches, writing them to IPC stream
    for maybe_record_batch in intermediate {
        let record_batch = maybe_record_batch?;
        writer.write(&record_batch)?;
    }
    writer.finish()?;

    let writer_buffer = writer.into_inner()?;
    Ok(writer_buffer.to_vec())
}
