use std::task::Poll;
use std::time::Duration;

use futures::AsyncWrite;
use js_sys::Uint8Array;
use wasm_bindgen_futures::spawn_local;

pub struct ReadableStreamSink(web_sys::ReadableStreamDefaultController);

impl std::io::Write for ReadableStreamSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let intermediate = Uint8Array::from(buf);
        let _ = self.0.enqueue_with_chunk(&intermediate);
        Ok(intermediate.byte_length() as usize)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        todo!()
    }
}

impl From<web_sys::ReadableStreamDefaultController> for ReadableStreamSink {
    fn from(value: web_sys::ReadableStreamDefaultController) -> Self {
        Self { 0: value}
    }
}

pub struct AsyncReadableStreamSink {
    controller: web_sys::ReadableStreamDefaultController,
}

#[cfg(all(feature = "reader", feature = "async"))]
impl AsyncWrite for AsyncReadableStreamSink {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, std::io::Error>> {
        let this = self.get_mut();
        if this.controller.desired_size().unwrap() <= 0.0 {
            let waker = cx.waker().clone();
            let sleep_duration = Duration::from_millis(16);
            spawn_local(async move {
                async_std::task::sleep(sleep_duration).await;
                waker.wake_by_ref();
            });
            Poll::Pending
        } else {
            let intermediate = Uint8Array::from(buf);
            let buf_size = usize::try_from(intermediate.byte_length()).unwrap();
            let _ = this.controller.enqueue_with_chunk(&intermediate);
            Poll::Ready(Ok(buf_size))
        }
    }

    fn poll_flush(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> Poll<std::io::Result<()>> {
        todo!()
    }

    fn poll_close(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let _ = this.controller.close();
        Poll::Ready(Ok(()))
    }
}

impl From<web_sys::ReadableStreamDefaultController> for AsyncReadableStreamSink {
    fn from(value: web_sys::ReadableStreamDefaultController) -> Self {
        Self { controller: value }
    }
}