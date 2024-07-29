use futures::AsyncWrite;

pub struct WrappedWritableStream<'writer> {
    pub stream: wasm_streams::writable::IntoAsyncWrite<'writer>,
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
