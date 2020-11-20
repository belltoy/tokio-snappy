//!  Wrap rust-snappy for `tokio::io::AsyncRead` and `tokio::io::AsyncWrite`.
//!
use std::pin::Pin;
use std::io::{Read, Write, Result};
use std::task::{Context, Poll};
use std::mem::{
    MaybeUninit,
};

use pin_project::pin_project;
use tokio::io::{
    AsyncRead,
    AsyncWrite,
    ReadBuf,
};
use bytes::{
    buf::{
        Reader,
        Writer,
    },
    Buf,
    BufMut,
    BytesMut,
};
use futures::ready;

// max uncompressed content size
const MAX_BLOCK_SIZE: usize = 1 << 16;

// max_compress_len(MAX_BLOCK_SIZE)
const MAX_COMPRESSED_SIZE: usize = 76490;

/// Async implementation for the snappy framed encoder/decoder.
#[pin_project]
#[derive(Debug)]
pub struct SnappyIO<T> {
    #[pin] inner: T,
    read_buf: BytesMut,
    decoder: snap::read::FrameDecoder<Reader<BytesMut>>,
    encoder: snap::write::FrameEncoder<Writer<BytesMut>>,
}

impl<T> SnappyIO<T> {

    /// Create a new SnappyIO, wrapped the given io object.
    pub fn new(io: T) -> Self {
        let encoder_writer = BytesMut::with_capacity(MAX_BLOCK_SIZE);
        let decoder_reader = BytesMut::with_capacity(MAX_COMPRESSED_SIZE);
        Self {
            inner: io,
            read_buf: BytesMut::with_capacity(MAX_BLOCK_SIZE),
            decoder: snap::read::FrameDecoder::new(decoder_reader.reader()),
            encoder: snap::write::FrameEncoder::new(encoder_writer.writer()),
        }
    }

    /// Consume this `SnappyIO`, returning the underlying value.
    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for SnappyIO<T> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<Result<()>> {
        let mut this = self.project();

        loop {
            if this.read_buf.remaining() > 0 {
                let amt = std::cmp::min(this.read_buf.remaining(), buf.remaining());
                let slice = this.read_buf.split_to(amt);
                buf.put_slice(&slice);
                return Poll::Ready(Ok(()));
            }

            let decoder_reader = this.decoder.get_mut();
            let decoder_buf: &mut BytesMut = decoder_reader.get_mut();
            let buf_len = decoder_buf.len();
            if buf_len < 4 {
                decoder_buf.reserve(4 - buf_len);

                let n = {
                    let dst = decoder_buf.bytes_mut();
                    let dst = unsafe { &mut *(dst as *mut _ as *mut [MaybeUninit<u8>]) };
                    let mut buf = ReadBuf::uninit(&mut dst[..4]);
                    let ptr = buf.filled().as_ptr();
                    let inner = this.inner.as_mut();
                    ready!(inner.poll_read(cx, &mut buf)?);

                    // Ensure the pointer does not change from under us
                    assert_eq!(ptr, buf.filled().as_ptr());
                    buf.filled().len()
                };
                if n == 0 {
                    return Poll::Ready(Ok(()));
                }
                // Safety: This is guaranteed to be the number of initialized (and read)
                // bytes due to the invariants provided by `ReadBuf::filled`.
                unsafe {
                    decoder_buf.advance_mut(n);
                }

                continue;
            }

            let mut chunk_len_buf = &decoder_buf.as_ref()[1..];
            let chunk_len = chunk_len_buf.get_uint_le(3) as usize;

            let buf_len = decoder_buf.len();
            // Read the entire chunk into buf
            if buf_len < chunk_len + 4 {
                decoder_buf.reserve(chunk_len + 4 - buf_len);
                let n = {
                    let dst = decoder_buf.bytes_mut();
                    let dst = unsafe { &mut *(dst as *mut _ as *mut [MaybeUninit<u8>]) };
                    let mut buf = ReadBuf::uninit(&mut dst[..chunk_len]);
                    let ptr = buf.filled().as_ptr();
                    ready!(this.inner.as_mut().poll_read(cx, &mut buf)?);

                    assert_eq!(ptr, buf.filled().as_ptr());
                    buf.filled().len()
                };
                if n == 0 {
                    return Poll::Ready(Ok(()));
                }

                unsafe {
                    decoder_buf.advance_mut(n);
                }

                continue;
            }

            if decoder_buf.len() == chunk_len + 4 {
                let dst = this.read_buf.bytes_mut();
                let mut dst = unsafe { &mut *(dst as *mut _ as *mut [u8]) };
                let _decoded = this.decoder.read(&mut dst)?;
                unsafe {
                    this.read_buf.advance_mut(_decoded);
                }
            }
        }
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for SnappyIO<T> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let len = std::cmp::min(buf.len(), MAX_BLOCK_SIZE);

        let mut this = self.project();
        loop {
            let output_buf = this.encoder.get_mut().get_mut();
            if output_buf.has_remaining() {
                let n = ready!(this.inner.as_mut().poll_write(cx, output_buf.bytes())?);
                output_buf.advance(n);
                return Poll::Ready(Ok(len));
            }

            let _ = this.encoder.write(&buf[..len])?;
            let output_buf = this.encoder.get_mut().get_mut();

            if output_buf.is_empty() {
                this.encoder.flush()?;
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.project().inner.poll_shutdown(cx)
    }

}
