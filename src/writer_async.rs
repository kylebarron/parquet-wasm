use crate::common::stream::WrappedWritableStream;
use crate::error::{ParquetWasmError, Result};
use async_compat::CompatExt;
use futures::channel::oneshot;
use futures::StreamExt;
use parquet::arrow::async_writer::AsyncArrowWriter;
use wasm_bindgen_futures::spawn_local;

pub async fn transform_parquet_stream(
    batches: impl futures::Stream<Item = Result<arrow_wasm::RecordBatch>> + 'static,
    writer_properties: crate::writer_properties::WriterProperties,
) -> Result<wasm_streams::readable::sys::ReadableStream> {
    let options = Some(writer_properties.into());

    let raw_stream = wasm_streams::transform::sys::TransformStream::new();
    if let Ok(raw_stream) = raw_stream {
        let (writable_stream, output_stream) = {
            let raw_writable = raw_stream.writable();
            let inner_writer =
                wasm_streams::WritableStream::from_raw(raw_writable).into_async_write();
            let writable_stream = WrappedWritableStream {
                stream: inner_writer,
            };
            (writable_stream, raw_stream.readable())
        };
        // construct a channel for the purposes of signalling errors occuring at the start of the stream.
        // Errors that occur during writing will have to fuse the stream.
        let (sender, receiver) = oneshot::channel::<Result<()>>();
        spawn_local(async move {
            let mut adapted_stream = batches.peekable();
            let mut pinned_stream = std::pin::pin!(adapted_stream);
            let first_batch = pinned_stream.as_mut().peek().await;
            if let Some(Ok(first_batch)) = first_batch {
                let schema = first_batch.schema().into_inner();
                let writer = AsyncArrowWriter::try_new(writable_stream.compat(), schema, options);
                match writer {
                    Ok(mut writer) => {
                        // unblock the calling thread's receiver (indicating that stream initialization was error-free)
                        let _ = sender.send(Ok(()));
                        while let Some(batch) = pinned_stream.next().await {
                            if let Ok(batch) = batch {
                                let _ = writer.write(&batch.into()).await;
                            }
                        }
                        let _ = writer.close().await;
                    }
                    Err(err) => {
                        let _ = sender.send(Err(ParquetWasmError::ParquetError(Box::new(err))));
                    }
                }
            } else if let Some(Err(err)) = first_batch {
                let _ = sender.send(Err(ParquetWasmError::DynCastingError(
                    err.to_string().into(),
                )));
            } else {
                let _ = sender.send(Err(ParquetWasmError::DynCastingError(
                    "null first batch".to_string().into(),
                )));
            }
        });
        match receiver.await.unwrap() {
            Ok(()) => Ok(output_stream),
            Err(err) => Err(err),
        }
    } else {
        Err(ParquetWasmError::PlatformSupportError(
            "Failed to create TransformStream".to_string(),
        ))
    }
}
