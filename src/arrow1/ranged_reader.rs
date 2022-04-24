use std::io::{Result, SeekFrom};
use std::pin::Pin;

use futures::{
    future::BoxFuture,
    io::{AsyncRead, AsyncSeek},
    Future,
};

/// A range of bytes with a known starting position.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RangeOutput {
    /// the start
    pub start: u64,
    /// the data
    pub data: Vec<u8>,
}

/// A function that returns a [`BoxFuture`] of [`RangeOutput`].
/// For example, an async request to return a range of bytes from a blob from the internet.
pub type RangedFuture =
    Box<dyn Fn(u64, usize) -> BoxFuture<'static, std::io::Result<RangeOutput>> + Send + Sync>;

/// A struct that converts [`RangedFuture`] to a `AsyncRead + AsyncSeek` with an internal buffer.
pub struct RangedAsyncReader {
    pos: u64,
    length: u64, // total size
    state: State,
    ranged_future: RangedFuture,
    min_request_size: usize, // requests have at least this size
}

enum State {
    HasChunk(RangeOutput),
    Seeking(BoxFuture<'static, std::io::Result<RangeOutput>>),
}

impl RangedAsyncReader {
    /// Creates a new [`RangedAsyncReader`]. `length` is the total size of the blob being seeked,
    /// `min_request_size` is the minimum number of bytes allowed to be requested to `range_get`.
    pub fn new(length: usize, min_request_size: usize, ranged_future: RangedFuture) -> Self {
        let length = length as u64;
        Self {
            pos: 0,
            length,
            state: State::HasChunk(RangeOutput {
                start: 0,
                data: vec![],
            }),
            ranged_future,
            min_request_size,
        }
    }
}

// whether `test_interval` is inside `a` (start, length).
// a    = [          ]
// test =    [     ]
// returns true
fn range_includes(a: (usize, usize), test_interval: (usize, usize)) -> bool {
    if test_interval.0 < a.0 {
        return false;
    }
    let test_end = test_interval.0 + test_interval.1;
    let a_end = a.0 + a.1;
    if test_end > a_end {
        return false;
    }
    true
}

impl AsyncRead for RangedAsyncReader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<Result<usize>> {
        let requested_range = (self.pos as usize, buf.len());
        let min_request_size = self.min_request_size;
        match &mut self.state {
            State::HasChunk(output) => {
                let existing_range = (output.start as usize, output.data.len());
                if range_includes(existing_range, requested_range) {
                    let offset = requested_range.0 - existing_range.0;
                    buf.copy_from_slice(&output.data[offset..offset + buf.len()]);
                    self.pos += buf.len() as u64;
                    std::task::Poll::Ready(Ok(buf.len()))
                } else {
                    let start = requested_range.0 as u64;
                    let length = std::cmp::max(min_request_size, requested_range.1);
                    let future = (self.ranged_future)(start, length);
                    self.state = State::Seeking(Box::pin(future));
                    self.poll_read(cx, buf)
                }
            }
            State::Seeking(ref mut future) => match Pin::new(future).poll(cx) {
                std::task::Poll::Ready(v) => {
                    match v {
                        Ok(output) => self.state = State::HasChunk(output),
                        Err(e) => return std::task::Poll::Ready(Err(e)),
                    };
                    self.poll_read(cx, buf)
                }
                std::task::Poll::Pending => std::task::Poll::Pending,
            },
        }
    }
}

impl AsyncSeek for RangedAsyncReader {
    fn poll_seek(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        pos: SeekFrom,
    ) -> std::task::Poll<Result<u64>> {
        match pos {
            SeekFrom::Start(pos) => self.pos = pos,
            SeekFrom::End(pos) => self.pos = (self.length as i64 + pos) as u64,
            SeekFrom::Current(pos) => self.pos = (self.pos as i64 + pos) as u64,
        };
        std::task::Poll::Ready(Ok(self.pos))
    }
}
