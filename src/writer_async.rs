use crate::error::Result;
use crate::common::stream::WrappedWritableStream;
use async_compat::CompatExt;
use futures::StreamExt;
use parquet::arrow::async_writer::AsyncArrowWriter;
use wasm_bindgen_futures::spawn_local;

pub fn transform_parquet_stream(
    batches: impl futures::Stream<Item = arrow_wasm::RecordBatch> + 'static,
    writer_properties: crate::writer_properties::WriterProperties,
) -> Result<wasm_streams::readable::sys::ReadableStream> {
    let options = Some(writer_properties.into());
    // let encoding = writer_properties.get_encoding();

    let (writable_stream, output_stream) = {
        let raw_stream = wasm_streams::transform::sys::TransformStream::new().unwrap();
        let raw_writable = raw_stream.writable();
        let inner_writer = wasm_streams::WritableStream::from_raw(raw_writable).into_async_write();
        let writable_stream = WrappedWritableStream {
            stream: inner_writer,
        };
        (writable_stream, raw_stream.readable())
    };
    spawn_local::<_>(async move {
        let mut adapted_stream = batches.peekable();
        let mut pinned_stream = std::pin::pin!(adapted_stream);
        let first_batch = pinned_stream.as_mut().peek().await.unwrap();
        let schema = first_batch.schema().into_inner();
        // Need to create an encoding for each column
        let mut writer =
            AsyncArrowWriter::try_new(writable_stream.compat(), schema, options).unwrap();
        while let Some(batch) = pinned_stream.next().await {
            let _ = writer.write(&batch.into()).await;
        }
        let _ = writer.close().await;
    });
    Ok(output_stream)
}