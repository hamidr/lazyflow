//! Terminal operations and specialized impls: collect, fold, count, for_each,
//! first, last, into_pull, unchunks, flatten, unzip.

use std::collections::VecDeque;

use crate::pull::{PipeError, PullOperator, collect_all, fold_all};

use super::Pipe;
use super::pull_ops::PullFlatten;

impl<B: Send + 'static> Pipe<B> {
    /// Materialize the pipeline into a pull operator chain.
    pub(crate) fn into_pull(self) -> Box<dyn PullOperator<B>> {
        (self.factory)()
    }

    /// Collect all elements into a `Vec`.
    pub async fn collect(self) -> Result<Vec<B>, PipeError> {
        let mut root = (self.factory)();
        collect_all(&mut *root).await
    }

    /// Reduce all elements to a single value.
    pub async fn fold<C: Send + 'static>(
        self,
        init: C,
        f: impl Fn(C, B) -> C + Send,
    ) -> Result<C, PipeError> {
        let mut root = (self.factory)();
        fold_all(&mut *root, init, f).await
    }

    /// Drive the pipeline chunk by chunk, folding each whole chunk into
    /// the accumulator. `f` returns a [`std::ops::ControlFlow`]:
    /// `Continue(acc)` keeps pulling the next chunk, `Break(acc)` stops
    /// immediately at this chunk boundary without pulling further.
    ///
    /// Unlike [`fold`](Self::fold) (one call per element), this hands
    /// the closure a whole `Vec<B>` chunk at once, so a consumer can
    /// amortize per-item work -- accounting, metering, budget checks --
    /// to once per chunk, and short-circuit cooperatively without a
    /// separate cancel token. Peak buffering is bounded to the items
    /// already pulled plus the in-flight chunk.
    pub async fn try_fold_chunks<C: Send + 'static>(
        self,
        init: C,
        mut f: impl FnMut(C, Vec<B>) -> std::ops::ControlFlow<C, C> + Send,
    ) -> Result<C, PipeError> {
        let mut root = (self.factory)();
        let mut acc = init;
        while let Some(chunk) = root.next_chunk().await? {
            match f(acc, chunk) {
                std::ops::ControlFlow::Continue(c) => acc = c,
                std::ops::ControlFlow::Break(c) => return Ok(c),
            }
        }
        Ok(acc)
    }

    /// Reduce all elements using the first element as the initial
    /// accumulator. Returns `None` for empty streams.
    pub async fn reduce(self, f: impl Fn(B, B) -> B + Send) -> Result<Option<B>, PipeError> {
        let mut root = (self.factory)();
        let first = match root.next_chunk().await? {
            Some(mut chunk) => {
                let mut iter = chunk.drain(..);
                let first = iter.next().unwrap();
                iter.fold(first, &f)
            }
            None => return Ok(None),
        };
        let mut acc = first;
        while let Some(chunk) = root.next_chunk().await? {
            for item in chunk {
                acc = f(acc, item);
            }
        }
        Ok(Some(acc))
    }

    /// Count elements.
    pub async fn count(self) -> Result<usize, PipeError> {
        let mut root = (self.factory)();
        let mut n = 0usize;
        while let Some(chunk) = root.next_chunk().await? {
            n += chunk.len();
        }
        Ok(n)
    }

    /// Run a side-effect for each element, discarding the values.
    pub async fn for_each(self, f: impl Fn(B) + Send) -> Result<(), PipeError> {
        let mut root = (self.factory)();
        while let Some(chunk) = root.next_chunk().await? {
            for item in chunk {
                f(item);
            }
        }
        Ok(())
    }

    /// Run an async side-effect for each element, discarding the values.
    pub async fn eval_for_each<Fut: std::future::Future<Output = Result<(), PipeError>> + Send>(
        self,
        f: impl Fn(B) -> Fut + Send,
    ) -> Result<(), PipeError> {
        let mut root = (self.factory)();
        while let Some(chunk) = root.next_chunk().await? {
            for item in chunk {
                f(item).await?;
            }
        }
        Ok(())
    }

    /// Return the first element, or `None` if empty.
    pub async fn first(self) -> Result<Option<B>, PipeError> {
        let mut root = (self.factory)();
        match root.next_chunk().await? {
            Some(chunk) => Ok(chunk.into_iter().next()),
            None => Ok(None),
        }
    }

    /// Return the last element, or `None` if empty.
    pub async fn last(self) -> Result<Option<B>, PipeError> {
        let mut root = (self.factory)();
        let mut last = None;
        while let Some(chunk) = root.next_chunk().await? {
            if let Some(item) = chunk.into_iter().last() {
                last = Some(item);
            }
        }
        Ok(last)
    }
}

/// Flatten a `Pipe<Vec<B>>` back to `Pipe<B>`.
impl<B: Clone + Send + Sync + 'static> Pipe<Vec<B>> {
    /// Flatten chunked elements back to individual elements.
    pub fn unchunks(self) -> Pipe<B> {
        self.flat_map(Pipe::from_iter)
    }
}

/// Flatten a `Pipe<Pipe<B>>` into `Pipe<B>`.
impl<B: Send + 'static> Pipe<Pipe<B>> {
    /// Flatten nested pipes: each inner pipe is drained sequentially.
    pub fn flatten(self) -> Pipe<B> {
        let parent = self.factory;
        Pipe::from_factory(move || {
            Box::new(PullFlatten {
                outer: parent(),
                inner: None,
                pending: VecDeque::new(),
            })
        })
    }
}

/// Split a `Pipe<(A, B)>` into two pipes via broadcast.
///
/// Uses `broadcast(2)` internally -- the source is shared.
/// Both pipes must be consumed; dropping one stalls the other.
impl<A: Clone + Send + Sync + 'static, B: Clone + Send + Sync + 'static> Pipe<(A, B)> {
    pub fn unzip(self, buffer_size: usize) -> (Pipe<A>, Pipe<B>) {
        let branches = self.broadcast(2, buffer_size);
        let mut iter = branches.into_iter();
        let left = iter.next().unwrap().map(|(a, _)| a);
        let right = iter.next().unwrap().map(|(_, b)| b);
        (left, right)
    }
}

#[cfg(test)]
mod try_fold_chunks_tests {
    use super::Pipe;
    use std::ops::ControlFlow;

    #[tokio::test]
    async fn breaks_at_chunk_boundary() {
        // from_iter chunks at 256; a Break after the first chunk must
        // stop pulling, so far fewer than all 1000 items are seen.
        let out = Pipe::from_iter(0..1000)
            .try_fold_chunks(Vec::new(), |mut acc: Vec<i32>, chunk| {
                let first = acc.is_empty();
                acc.extend(chunk);
                if first {
                    ControlFlow::Break(acc)
                } else {
                    ControlFlow::Continue(acc)
                }
            })
            .await
            .unwrap();
        assert!(
            out.len() < 1000,
            "Break must stop early, got {} items",
            out.len()
        );
        assert!(!out.is_empty(), "the first chunk still lands");
    }

    #[tokio::test]
    async fn continue_drains_every_item() {
        let out = Pipe::from_iter(0..1000)
            .try_fold_chunks(Vec::new(), |mut acc: Vec<i32>, chunk| {
                acc.extend(chunk);
                ControlFlow::Continue(acc)
            })
            .await
            .unwrap();
        assert_eq!(out.len(), 1000, "Continue drains the whole pipe");
    }
}
