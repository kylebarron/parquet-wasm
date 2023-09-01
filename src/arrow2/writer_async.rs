use crate::arrow2::error::Result;
use arrow2::io::parquet::write::FileSink;
use futures::{AsyncWrite, SinkExt, StreamExt};
use wasm_bindgen_futures::spawn_local;

struct WrappedWritableStream<'writer> {
    stream: wasm_streams::writable::IntoAsyncWrite<'writer>,
}

impl<'writer> AsyncWrite for WrappedWritableStream<'writer> {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        AsyncWrite::poll_write(std::pin::Pin::new(&mut self.get_mut().stream), cx, buf)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        AsyncWrite::poll_flush(std::pin::Pin::new(&mut self.get_mut().stream), cx)
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        AsyncWrite::poll_close(std::pin::Pin::new(&mut self.get_mut().stream), cx)
    }
}

unsafe impl<'writer> Send for WrappedWritableStream<'writer> {}

pub fn transform_parquet_stream(
    batches: impl futures::Stream<Item = arrow_wasm::arrow2::RecordBatch> + 'static,
    writer_properties: crate::arrow2::writer_properties::WriterProperties,
) -> Result<wasm_streams::readable::sys::ReadableStream> {
    let options = writer_properties.get_write_options();
    let encoding = writer_properties.get_encoding();

    let (writable_stream, output_stream) = {
        let raw_stream = wasm_streams::transform::sys::TransformStream::new();
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
        let mut encodings = vec![];
        for _ in &schema.fields {
            // Note, the nested encoding is for nested Parquet columns
            // Here we assume columns are not nested
            encodings.push(vec![encoding]);
        }
        let mut writer = FileSink::try_new(writable_stream, schema, encodings, options).unwrap();
        while let Some(batch) = pinned_stream.next().await {
            let _ = writer.send(batch.into_inner().1).await;
        }
        let _ = writer.close().await;
    });
    Ok(output_stream)
}
