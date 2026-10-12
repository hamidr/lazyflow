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
            // Waits only for the first item, then takes what is already
            // ready: a live stream must not hold items back to fill a chunk.
            let mut chunk = Vec::new();
            poll_fn(|cx| {
                while chunk.len() < self.chunk_size {
                    match self.stream.as_mut().poll_next(cx) {
                        Poll::Ready(Some(item)) => chunk.push(item),
                        Poll::Ready(None) => break,
                        Poll::Pending if chunk.is_empty() => return Poll::Pending,
                        Poll::Pending => break,
                    }
                }
                Poll::Ready(())
            })
            .await;
            Ok(Some(chunk).filter(|chunk| !chunk.is_empty()))
        })
    }
}

impl<B: Send + 'static> Pipe<B> {
    /// Create a pipe from any [`Stream`].
    ///
    /// The resulting pipe is single-use -- cloning and materializing
    /// both clones returns an error. Items the stream has ready are
    /// passed on in chunks of at most 256; an item is never held back
    /// waiting for more. Use [`from_stream_buffered`](Self::from_stream_buffered)
    /// to change the maximum.
    pub fn from_stream(stream: impl Stream<Item = B> + Send + 'static) -> Self {
        Self::from_stream_buffered(stream, DEFAULT_CHUNK_SIZE)
    }

    /// Create a pipe from a [`Stream`] with a custom maximum chunk size.
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

    /// Yields the items of an iterator, each immediately ready.
    struct Ready<I>(I);

    impl<I: Iterator + Unpin> Stream for Ready<I> {
        type Item = I::Item;
        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<I::Item>> {
            Poll::Ready(self.0.next())
        }
    }

    fn pull_from<S: Stream<Item = i64> + Send + 'static>(
        stream: S,
        chunk_size: usize,
    ) -> PullFromStream<i64> {
        PullFromStream {
            stream: Box::pin(stream),
            chunk_size,
        }
    }

    const PROMPTLY: std::time::Duration = std::time::Duration::from_millis(100);

    #[tokio::test(start_paused = true)]
    async fn a_ready_item_is_delivered_while_the_stream_stays_pending() {
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        tx.send(1).await.unwrap();
        let mut pull = pull_from(RecvStream(rx), DEFAULT_CHUNK_SIZE);
        let chunk = tokio::time::timeout(PROMPTLY, pull.next_chunk()).await;
        assert_eq!(
            chunk.expect("waited for a full chunk").unwrap(),
            Some(vec![1])
        );
        drop(tx);
    }

    #[tokio::test(start_paused = true)]
    async fn the_items_ready_together_form_one_chunk() {
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        for i in 0..3 {
            tx.send(i).await.unwrap();
        }
        let mut pull = pull_from(RecvStream(rx), DEFAULT_CHUNK_SIZE);
        let chunk = tokio::time::timeout(PROMPTLY, pull.next_chunk()).await;
        assert_eq!(chunk.unwrap().unwrap(), Some(vec![0, 1, 2]));
        drop(tx);
    }

    #[tokio::test]
    async fn a_ready_burst_is_split_at_the_chunk_size() {
        let mut pull = pull_from(Ready(0..7), 3);
        let mut sizes = Vec::new();
        while let Some(chunk) = pull.next_chunk().await.unwrap() {
            sizes.push(chunk.len());
        }
        assert_eq!(sizes, [3, 3, 1]);
    }

    #[tokio::test]
    async fn chunk_size_one_yields_single_items() {
        let mut pull = pull_from(Ready(0..2), 1);
        assert_eq!(pull.next_chunk().await.unwrap(), Some(vec![0]));
        assert_eq!(pull.next_chunk().await.unwrap(), Some(vec![1]));
        assert_eq!(pull.next_chunk().await.unwrap(), None);
    }

    #[tokio::test(start_paused = true)]
    async fn an_empty_pending_stream_waits_without_yielding_empty_chunks() {
        let (tx, rx) = tokio::sync::mpsc::channel::<i64>(1);
        let mut pull = pull_from(RecvStream(rx), DEFAULT_CHUNK_SIZE);
        assert!(
            tokio::time::timeout(PROMPTLY, pull.next_chunk())
                .await
                .is_err()
        );
        drop(tx);
        assert_eq!(pull.next_chunk().await.unwrap(), None);
    }
}
