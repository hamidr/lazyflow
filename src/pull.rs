//! Pull-based execution protocol.
//!
//! The [`PullOperator`] trait is the core execution interface. External
//! crates implement it to provide custom sources (SQL cursors, gRPC
//! streams, file readers) that compose with [`Pipe`](crate::pipeline::Pipe)
//! via [`Pipe::from_pull()`](crate::pipeline::Pipe::from_pull).
//!
//! Internally, elements flow through the pipeline in chunks (`Vec<B>`).
//! The chunk boundary is an implementation detail -- the public
//! [`Pipe`](crate::pipeline::Pipe) API exposes only element-level operations.

use std::future::Future;
use std::pin::Pin;

use crate::operator::Operator;

/// Error type for pipe operations.
///
/// Known error sources get dedicated variants for pattern matching.
/// User-defined errors (from `eval_map`, `eval_filter`, custom
/// `PullOperator` implementations) land in [`Custom`](PipeError::Custom).
#[derive(Debug)]
pub enum PipeError {
    /// I/O error from `AsyncRead`/`AsyncWrite` operations.
    Io(std::io::Error),
    /// Internal channel closed unexpectedly (producer or consumer dropped).
    Closed,
    /// All retry attempts exhausted.
    RetryExhausted,
    /// A pull took longer than its [`timeout`](crate::pipeline::Pipe::timeout).
    Timeout,
    /// A task running part of the pipe panicked; carries the panic message.
    /// Without it the task's channel would close as at a normal end of
    /// stream and the pipe would end short instead of failing.
    Panicked(String),
    /// User-defined or third-party error.
    Custom(Box<dyn std::error::Error + Send + Sync>),
}

impl std::fmt::Display for PipeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PipeError::Io(e) => write!(f, "I/O error: {e}"),
            PipeError::Closed => write!(f, "channel closed"),
            PipeError::RetryExhausted => write!(f, "retry exhausted"),
            PipeError::Timeout => write!(f, "timeout"),
            PipeError::Panicked(msg) => write!(f, "task panicked: {msg}"),
            PipeError::Custom(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for PipeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PipeError::Io(e) => Some(e),
            PipeError::Custom(e) => Some(&**e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for PipeError {
    fn from(e: std::io::Error) -> Self {
        PipeError::Io(e)
    }
}

impl From<Box<dyn std::error::Error + Send + Sync>> for PipeError {
    fn from(e: Box<dyn std::error::Error + Send + Sync>) -> Self {
        PipeError::Custom(e)
    }
}

impl From<String> for PipeError {
    fn from(s: String) -> Self {
        PipeError::Custom(s.into())
    }
}

impl From<&str> for PipeError {
    fn from(s: &str) -> Self {
        PipeError::Custom(s.into())
    }
}

/// Future returning an optional chunk of elements.
pub type ChunkFut<'a, B> =
    Pin<Box<dyn Future<Output = Result<Option<Vec<B>>, PipeError>> + Send + 'a>>;

/// Pull-based operator that yields chunks on demand.
///
/// This is the execution protocol for pipe. External crates
/// implement this trait to provide custom sources that compose with
/// [`Pipe`](crate::pipeline::Pipe):
///
/// ```ignore
/// struct MyCursor { /* ... */ }
///
/// impl PullOperator<MyRow> for MyCursor {
///     fn next_chunk(&mut self) -> ChunkFut<'_, MyRow> {
///         Box::pin(async move {
///             // fetch next batch from source
///             Ok(Some(vec![/* rows */]))
///         })
///     }
/// }
///
/// let pipe = Pipe::from_pull(Box::new(MyCursor { /* ... */ }));
/// let rows = pipe.collect().await?;
/// ```
pub trait PullOperator<B: Send + 'static>: Send {
    /// Pull the next chunk. Returns `None` when exhausted.
    fn next_chunk(&mut self) -> ChunkFut<'_, B>;
}

impl<B: Send + 'static, T: PullOperator<B> + ?Sized> PullOperator<B> for Box<T> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        (**self).next_chunk()
    }
}

pub(crate) struct ErrorSource<B> {
    error: Option<PipeError>,
    _phantom: std::marker::PhantomData<B>,
}

impl<B> ErrorSource<B> {
    pub(crate) fn new(error: PipeError) -> Self {
        Self {
            error: Some(error),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<B: Send + 'static> PullOperator<B> for ErrorSource<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            match self.error.take() {
                Some(e) => Err(e),
                None => Ok(None),
            }
        })
    }
}

pub(crate) struct PullSource<B> {
    items: Option<Vec<B>>,
}

impl<B> PullSource<B> {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn new(items: Vec<B>) -> Self {
        if items.is_empty() {
            Self { items: None }
        } else {
            Self { items: Some(items) }
        }
    }

    pub(crate) fn empty() -> Self {
        Self { items: None }
    }
}

impl<B: Send + 'static> PullOperator<B> for PullSource<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move { Ok(self.items.take()) })
    }
}

const ARC_SOURCE_CHUNK_SIZE: usize = 256;

/// Source backed by `Arc<[B]>` -- clones elements in chunks on demand
/// instead of cloning the entire dataset upfront.
pub(crate) struct ArcSource<B> {
    data: std::sync::Arc<[B]>,
    offset: usize,
}

impl<B> ArcSource<B> {
    pub(crate) fn new(data: std::sync::Arc<[B]>) -> Self {
        Self { data, offset: 0 }
    }
}

impl<B: Clone + Send + Sync + 'static> PullOperator<B> for ArcSource<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            if self.offset >= self.data.len() {
                return Ok(None);
            }
            let end = (self.offset + ARC_SOURCE_CHUNK_SIZE).min(self.data.len());
            let chunk: Vec<B> = self.data[self.offset..end].to_vec();
            self.offset = end;
            Ok(Some(chunk))
        })
    }
}

const DEFAULT_CHUNK_SIZE: usize = 256;

pub(crate) struct PullUnfold<B, S, F> {
    state: Option<S>,
    step: F,
    chunk_size: usize,
    _phantom: std::marker::PhantomData<B>,
}

impl<B, S, F> PullUnfold<B, S, F> {
    pub(crate) fn new(seed: S, step: F) -> Self {
        Self {
            state: Some(seed),
            step,
            chunk_size: DEFAULT_CHUNK_SIZE,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<B: Send + 'static, S: Send + 'static, F: Fn(&mut S) -> Option<B> + Send + 'static>
    PullOperator<B> for PullUnfold<B, S, F>
{
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            let state = match self.state.as_mut() {
                Some(s) => s,
                None => return Ok(None),
            };
            let mut chunk = Vec::with_capacity(self.chunk_size);
            for _ in 0..self.chunk_size {
                match (self.step)(state) {
                    Some(item) => chunk.push(item),
                    None => {
                        self.state = None;
                        break;
                    }
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

pub(crate) struct PullIterate<B, F> {
    current: Option<B>,
    step: F,
    chunk_size: usize,
}

impl<B: Clone, F> PullIterate<B, F> {
    pub(crate) fn new(init: B, step: F) -> Self {
        Self {
            current: Some(init),
            step,
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }
}

impl<B: Clone + Send + 'static, F: Fn(&B) -> B + Send + 'static> PullOperator<B>
    for PullIterate<B, F>
{
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            let mut val = match self.current.take() {
                Some(v) => v,
                None => return Ok(None),
            };
            let mut chunk = Vec::with_capacity(self.chunk_size);
            chunk.push(val.clone());
            for _ in 1..self.chunk_size {
                val = (self.step)(&val);
                chunk.push(val.clone());
            }
            self.current = Some((self.step)(&val));
            Ok(Some(chunk))
        })
    }
}

pub(crate) struct PullMap<A, F> {
    pub(crate) child: Box<dyn PullOperator<A>>,
    pub(crate) f: F,
}

impl<A: Send + 'static, B: Send + 'static, F: Fn(A) -> B + Send + 'static> PullOperator<B>
    for PullMap<A, F>
{
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            match self.child.next_chunk().await? {
                Some(chunk) => Ok(Some(chunk.into_iter().map(&self.f).collect())),
                None => Ok(None),
            }
        })
    }
}

pub(crate) struct PullFilter<B, F> {
    pub(crate) child: Box<dyn PullOperator<B>>,
    pub(crate) f: F,
}

/// Max consecutive empty chunks before yielding to the runtime.
pub(crate) const YIELD_AFTER_EMPTY: usize = 32;

impl<B: Send + 'static, F: Fn(&B) -> bool + Send + 'static> PullOperator<B> for PullFilter<B, F> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            let mut empty_runs = 0usize;
            loop {
                match self.child.next_chunk().await? {
                    Some(mut chunk) => {
                        chunk.retain(|b| (self.f)(b));
                        if !chunk.is_empty() {
                            return Ok(Some(chunk));
                        }
                        empty_runs += 1;
                        if empty_runs >= YIELD_AFTER_EMPTY {
                            empty_runs = 0;
                            tokio::task::yield_now().await;
                        }
                    }
                    None => return Ok(None),
                }
            }
        })
    }
}

pub(crate) struct PullAndThen<A, B, F> {
    pub(crate) child: Box<dyn PullOperator<A>>,
    pub(crate) f: F,
    pub(crate) buf: Vec<B>,
}

impl<A: Send + 'static, B: Send + 'static, F: Fn(A) -> Option<B> + Send + 'static> PullOperator<B>
    for PullAndThen<A, B, F>
{
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            let mut empty_runs = 0usize;
            loop {
                match self.child.next_chunk().await? {
                    Some(chunk) => {
                        self.buf.clear();
                        self.buf
                            .extend(chunk.into_iter().filter_map(|a| (self.f)(a)));
                        if !self.buf.is_empty() {
                            return Ok(Some(std::mem::take(&mut self.buf)));
                        }
                        empty_runs += 1;
                        if empty_runs >= YIELD_AFTER_EMPTY {
                            empty_runs = 0;
                            tokio::task::yield_now().await;
                        }
                    }
                    None => return Ok(None),
                }
            }
        })
    }
}

pub(crate) struct PullFlatMap<A: Send + 'static, B: Send + 'static, F> {
    child: Box<dyn PullOperator<A>>,
    f: F,
    current_sub: Option<Box<dyn PullOperator<B>>>,
    pending: std::collections::VecDeque<A>,
}

impl<A: Send + 'static, B: Send + 'static, F> PullFlatMap<A, B, F> {
    pub(crate) fn new(child: Box<dyn PullOperator<A>>, f: F) -> Self {
        Self {
            child,
            f,
            current_sub: None,
            pending: std::collections::VecDeque::new(),
        }
    }
}

impl<A: Send + 'static, B: Send + 'static, F: Fn(A) -> crate::pipeline::Pipe<B> + Send + 'static>
    PullOperator<B> for PullFlatMap<A, B, F>
{
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            loop {
                if let Some(ref mut sub) = self.current_sub {
                    match sub.next_chunk().await? {
                        Some(chunk) => return Ok(Some(chunk)),
                        None => {
                            self.current_sub = None;
                        }
                    }
                }
                if let Some(item) = self.pending.pop_front() {
                    let pipe = (self.f)(item);
                    self.current_sub = Some(pipe.into_pull());
                    continue;
                }
                match self.child.next_chunk().await? {
                    Some(chunk) => {
                        self.pending.extend(chunk);
                        continue;
                    }
                    None => return Ok(None),
                }
            }
        })
    }
}

pub(crate) struct PullTap<B, F> {
    pub(crate) child: Box<dyn PullOperator<B>>,
    pub(crate) f: F,
}

impl<B: Send + 'static, F: Fn(&B) + Send + 'static> PullOperator<B> for PullTap<B, F> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            match self.child.next_chunk().await? {
                Some(chunk) => {
                    for item in &chunk {
                        (self.f)(item);
                    }
                    Ok(Some(chunk))
                }
                None => Ok(None),
            }
        })
    }
}

pub(crate) struct PullTake<B> {
    pub(crate) child: Box<dyn PullOperator<B>>,
    pub(crate) remaining: usize,
}

impl<B: Send + 'static> PullOperator<B> for PullTake<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            if self.remaining == 0 {
                return Ok(None);
            }
            match self.child.next_chunk().await? {
                Some(chunk) => {
                    if chunk.len() <= self.remaining {
                        self.remaining -= chunk.len();
                        Ok(Some(chunk))
                    } else {
                        let taken: Vec<B> = chunk.into_iter().take(self.remaining).collect();
                        self.remaining = 0;
                        Ok(Some(taken))
                    }
                }
                None => Ok(None),
            }
        })
    }
}

pub(crate) struct PullDrop<B> {
    pub(crate) child: Box<dyn PullOperator<B>>,
    pub(crate) remaining: usize,
}

impl<B: Send + 'static> PullOperator<B> for PullDrop<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            loop {
                match self.child.next_chunk().await? {
                    Some(chunk) => {
                        if self.remaining == 0 {
                            return Ok(Some(chunk));
                        }
                        if chunk.len() <= self.remaining {
                            self.remaining -= chunk.len();
                        } else {
                            let rest: Vec<B> = chunk.into_iter().skip(self.remaining).collect();
                            self.remaining = 0;
                            return Ok(Some(rest));
                        }
                    }
                    None => return Ok(None),
                }
            }
        })
    }
}

pub(crate) struct PullScan<A, S, F> {
    pub(crate) child: Box<dyn PullOperator<A>>,
    pub(crate) state: S,
    pub(crate) f: F,
}

impl<
    A: Send + 'static,
    B: Send + 'static,
    S: Send + 'static,
    F: Fn(&mut S, A) -> B + Send + 'static,
> PullOperator<B> for PullScan<A, S, F>
{
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            match self.child.next_chunk().await? {
                Some(chunk) => {
                    let result: Vec<B> = chunk
                        .into_iter()
                        .map(|a| (self.f)(&mut self.state, a))
                        .collect();
                    Ok(Some(result))
                }
                None => Ok(None),
            }
        })
    }
}

pub(crate) struct PullPipe<A: Send + 'static, B: Send + 'static> {
    pub(crate) operator: std::sync::Arc<dyn Operator<A, B> + Send + Sync>,
    pub(crate) child: Box<dyn PullOperator<A>>,
}

impl<A: Send + 'static, B: Send + 'static> PullOperator<B> for PullPipe<A, B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            let chunk = self.child.next_chunk().await?;
            match chunk {
                Some(chunk) => {
                    let mut result = Vec::with_capacity(chunk.len());
                    for item in chunk {
                        result.push(self.operator.execute(item).await?);
                    }
                    Ok(Some(result))
                }
                None => Ok(None),
            }
        })
    }
}

pub(crate) struct PullZip<A: Send + 'static, B: Send + 'static> {
    pub(crate) left: Box<dyn PullOperator<A>>,
    pub(crate) right: Box<dyn PullOperator<B>>,
    left_buf: std::collections::VecDeque<A>,
    right_buf: std::collections::VecDeque<B>,
    left_done: bool,
    right_done: bool,
}

impl<A: Send + 'static, B: Send + 'static> PullZip<A, B> {
    pub(crate) fn new(left: Box<dyn PullOperator<A>>, right: Box<dyn PullOperator<B>>) -> Self {
        Self {
            left,
            right,
            left_buf: std::collections::VecDeque::new(),
            right_buf: std::collections::VecDeque::new(),
            left_done: false,
            right_done: false,
        }
    }
}

impl<A: Send + 'static, B: Send + 'static> PullOperator<(A, B)> for PullZip<A, B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, (A, B)> {
        Box::pin(async move {
            // Fill buffers until both have elements or a side is exhausted.
            while self.left_buf.is_empty() && !self.left_done {
                match self.left.next_chunk().await? {
                    Some(chunk) => self.left_buf.extend(chunk),
                    None => {
                        self.left_done = true;
                    }
                }
            }
            while self.right_buf.is_empty() && !self.right_done {
                match self.right.next_chunk().await? {
                    Some(chunk) => self.right_buf.extend(chunk),
                    None => {
                        self.right_done = true;
                    }
                }
            }

            let len = self.left_buf.len().min(self.right_buf.len());
            if len == 0 {
                return Ok(None);
            }

            let pairs: Vec<(A, B)> = self
                .left_buf
                .drain(..len)
                .zip(self.right_buf.drain(..len))
                .collect();
            Ok(Some(pairs))
        })
    }
}

pub(crate) async fn collect_all<B: Send + 'static>(
    op: &mut dyn PullOperator<B>,
) -> Result<Vec<B>, PipeError> {
    let mut all = Vec::new();
    while let Some(chunk) = op.next_chunk().await? {
        if all.is_empty() {
            all = chunk;
        } else {
            all.reserve(chunk.len());
            all.extend(chunk);
        }
    }
    Ok(all)
}

pub(crate) async fn fold_all<A: Send + 'static, C, F: Fn(C, A) -> C>(
    op: &mut dyn PullOperator<A>,
    init: C,
    f: F,
) -> Result<C, PipeError> {
    let mut acc = init;
    while let Some(chunk) = op.next_chunk().await? {
        for item in chunk {
            acc = f(acc, item);
        }
    }
    Ok(acc)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::PinFut;

    #[derive(Debug)]
    struct ToStringOp;

    impl Operator<i64, String> for ToStringOp {
        fn execute<'a>(&'a self, input: i64) -> PinFut<'a, String> {
            Box::pin(async move { Ok(input.to_string()) })
        }
    }

    #[tokio::test]
    async fn source_yields_once() {
        let mut src = PullSource::new(vec![1, 2, 3]);
        let chunk = src.next_chunk().await.unwrap().unwrap();
        assert_eq!(chunk, vec![1, 2, 3]);
        assert!(src.next_chunk().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn map_changes_type() {
        let src: Box<dyn PullOperator<i64>> = Box::new(PullSource::new(vec![1, 2, 3]));
        let mut map = PullMap {
            child: src,
            f: |x: i64| x.to_string(),
        };
        let chunk: Vec<String> = map.next_chunk().await.unwrap().unwrap();
        assert_eq!(chunk, vec!["1", "2", "3"]);
    }

    #[tokio::test]
    async fn pipe_changes_type() {
        let src: Box<dyn PullOperator<i64>> = Box::new(PullSource::new(vec![1, 2, 3]));
        let mut pipe = PullPipe {
            operator: std::sync::Arc::new(ToStringOp),
            child: src,
        };
        let chunk: Vec<String> = pipe.next_chunk().await.unwrap().unwrap();
        assert_eq!(chunk, vec!["1", "2", "3"]);
    }

    #[tokio::test]
    async fn filter_same_type() {
        let src: Box<dyn PullOperator<i64>> = Box::new(PullSource::new(vec![1, 2, 3, 4, 5]));
        let mut filt = PullFilter {
            child: src,
            f: |x: &i64| x % 2 == 0,
        };
        let chunk = filt.next_chunk().await.unwrap().unwrap();
        assert_eq!(chunk, vec![2, 4]);
    }

    #[tokio::test]
    async fn collect_all_gathers() {
        let mut src = PullSource::new(vec![1, 2, 3]);
        let result = collect_all(&mut src).await.unwrap();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn fold_sum() {
        let mut src = PullSource::new(vec![1, 2, 3, 4, 5]);
        let sum = fold_all(&mut src, 0i64, |acc, x| acc + x).await.unwrap();
        assert_eq!(sum, 15);
    }
}
