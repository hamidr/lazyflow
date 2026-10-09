//! Interop between [`Pipe`] and
//! [`futures::Stream`](futures_core::Stream).

use std::future::poll_fn;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;

use crate::pipeline::Pipe;
use crate::pull::{ChunkFut, PipeError, PullOperator};

const DEFAULT_CHUNK_SIZE: usize = 256;

struct PullFromStream<B> {
    stream: Pin<Box<dyn Stream<Item = B> + Send>>,
    chunk_size: usize,
}

impl<B: Send + 'static> PullOperator<B> for PullFromStream<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            let mut chunk = Vec::with_capacity(self.chunk_size);
            for _ in 0..self.chunk_size {
                match poll_fn(|cx| self.stream.as_mut().poll_next(cx)).await {
                    Some(item) => chunk.push(item),
                    None => break,
                }
            }
            if chunk.is_empty() {
                Ok(None)
            } else {
                Ok(Some(chunk))
            }
        })
    }
}

impl<B: Send + 'static> Pipe<B> {
    /// Create a pipe from any [`Stream`].
    ///
    /// The resulting pipe is single-use -- cloning and materializing
    /// both clones returns an error. Uses a 256-element internal
    /// buffer; use [`from_stream_buffered`](Self::from_stream_buffered)
    /// to customize.
    pub fn from_stream(stream: impl Stream<Item = B> + Send + 'static) -> Self {
        Self::from_stream_buffered(stream, DEFAULT_CHUNK_SIZE)
    }

    /// Create a pipe from a [`Stream`] with a
    /// custom chunk size for internal buffering.
    pub fn from_stream_buffered(
        stream: impl Stream<Item = B> + Send + 'static,
        chunk_size: usize,
    ) -> Self {
        Pipe::from_pull_once(PullFromStream {
            stream: Box::pin(stream),
            chunk_size: chunk_size.max(1),
        })
    }
}

/// A [`Stream`] that yields elements from a [`Pipe`].
///
/// Created by [`Pipe::into_stream`]. Uses a background task and
/// bounded channel to bridge pull-based pipe to poll-based stream.
pub struct PipeStream<B> {
    rx: tokio::sync::mpsc::Receiver<Result<B, PipeError>>,
    _abort: tokio::task::AbortHandle,
}

impl<B> Drop for PipeStream<B> {
    fn drop(&mut self) {
        self._abort.abort();
    }
}

impl<B: Send + 'static> Stream for PipeStream<B> {
    type Item = Result<B, PipeError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

impl<B: Send + 'static> Pipe<B> {
    /// Convert this pipe into a [`Stream`].
    ///
    /// Spawns a background task that pulls chunks and sends elements
    /// one by one through a bounded channel (capacity 256). The task
    /// is cancelled when the stream is dropped. Use
    /// [`into_stream_buffered`](Self::into_stream_buffered) to
    /// customize the channel capacity.
    pub fn into_stream(self) -> PipeStream<B> {
        self.into_stream_buffered(256)
    }

    /// Convert this pipe into a [`Stream`]
    /// with a custom channel capacity for backpressure tuning.
    pub fn into_stream_buffered(self, buffer_size: usize) -> PipeStream<B> {
        let (tx, rx) = tokio::sync::mpsc::channel(buffer_size.max(1));
        let mut root = self.into_pull();
        let handle = tokio::spawn(async move {
            let report_tx = tx.clone();
            crate::panic::report_panic(&report_tx, async move {
                loop {
                    match root.next_chunk().await {
                        Ok(Some(chunk)) => {
                            for item in chunk {
                                if tx.send(Ok(item)).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Ok(None) => return,
                        Err(e) => {
                            let _ = tx.send(Err(e)).await;
                            return;
                        }
                    }
                }
            })
            .await;
        });
        PipeStream {
            rx,
            _abort: handle.abort_handle(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Minimal helper: poll a Stream to Vec without pulling in futures-util
    async fn collect_stream<S: Stream + Unpin>(mut stream: S) -> Vec<S::Item> {
        let mut items = Vec::new();
        loop {
            match poll_fn(|cx| Pin::new(&mut stream).poll_next(cx)).await {
                Some(item) => items.push(item),
                None => return items,
            }
        }
    }

    // Wrap tokio mpsc::Receiver as a Stream (minimal, no tokio-stream dep)
    struct RecvStream<T>(tokio::sync::mpsc::Receiver<T>);

    impl<T> Stream for RecvStream<T> {
        type Item = T;
        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
            self.0.poll_recv(cx)
        }
    }

    #[tokio::test]
    async fn from_stream_to_pipe() {
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        tx.send(1i64).await.unwrap();
        tx.send(2).await.unwrap();
        tx.send(3).await.unwrap();
        drop(tx);

        let result = Pipe::from_stream(RecvStream(rx))
            .map(|x| x * 10)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![10, 20, 30]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pipe_to_stream() {
        let stream = Pipe::from_iter(vec![1, 2, 3]).map(|x| x * 10).into_stream();

        let items: Vec<i64> = collect_stream(stream)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(items, vec![10, 20, 30]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn roundtrip_stream_pipe_stream() {
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        for i in 1..=5 {
            tx.send(i).await.unwrap();
        }
        drop(tx);

        let stream = Pipe::from_stream(RecvStream(rx))
            .filter(|x| x % 2 == 0)
            .map(|x| x * 100)
            .into_stream();

        let items: Vec<i64> = collect_stream(stream)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(items, vec![200, 400]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn into_stream_propagates_errors() {
        use crate::pull::ChunkFut;

        struct FailAfterOne {
            yielded: bool,
        }
        impl PullOperator<i64> for FailAfterOne {
            fn next_chunk(&mut self) -> ChunkFut<'_, i64> {
                Box::pin(async move {
                    if self.yielded {
                        Err("boom".into())
                    } else {
                        self.yielded = true;
                        Ok(Some(vec![42]))
                    }
                })
            }
        }

        let stream = Pipe::from_pull_once(FailAfterOne { yielded: false }).into_stream();
        let items: Vec<Result<i64, PipeError>> = collect_stream(stream).await;

        assert_eq!(items.len(), 2);
        assert_eq!(items[0].as_ref().unwrap(), &42);
        assert!(items[1].is_err());
    }
}
