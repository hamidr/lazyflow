//! Synchronous operators that transform pipe elements.

use std::sync::Arc;

use crate::operator::Operator;
use crate::pull::{
    PullAndThen, PullDrop, PullFilter, PullFlatMap, PullMap, PullPipe, PullScan, PullTake, PullTap,
};

use super::Pipe;
use super::pull_ops::{
    PullChain, PullChunks, PullInterleave, PullIntersperse, PullSkipWhile, PullSlidingWindow,
    PullTakeWhile,
};

impl<B: Send + 'static> Pipe<B> {
    /// Transform each element (may change type).
    pub fn map<C: Send + 'static>(self, f: impl Fn(B) -> C + Send + Sync + 'static) -> Pipe<C> {
        let parent = self.factory;
        let f = Arc::new(f);
        Pipe::from_factory(move || {
            let child = parent();
            let f = Arc::clone(&f);
            Box::new(PullMap {
                child,
                f: move |b| f(b),
            })
        })
    }

    /// Fused map + filter (may change type).
    pub fn and_then<C: Send + 'static>(
        self,
        f: impl Fn(B) -> Option<C> + Send + Sync + 'static,
    ) -> Pipe<C> {
        let parent = self.factory;
        let f = Arc::new(f);
        Pipe::from_factory(move || {
            let child = parent();
            let f = Arc::clone(&f);
            Box::new(PullAndThen {
                child,
                f: move |b| f(b),
                buf: Vec::new(),
            })
        })
    }

    /// Each element expands into a sub-stream (may change type).
    pub fn flat_map<C: Send + 'static>(
        self,
        f: impl Fn(B) -> Pipe<C> + Send + Sync + 'static,
    ) -> Pipe<C> {
        let parent = self.factory;
        let f = Arc::new(f);
        Pipe::from_factory(move || {
            let child = parent();
            let f = Arc::clone(&f);
            Box::new(PullFlatMap::new(child, move |b| f(b)))
        })
    }

    /// Stateful per-element transform (may change type).
    pub fn scan<C: Send + 'static, S: Clone + Send + Sync + 'static>(
        self,
        init: S,
        f: impl Fn(&mut S, B) -> C + Send + Sync + 'static,
    ) -> Pipe<C> {
        let parent = self.factory;
        type ScanFn<S, B, C> = Arc<dyn Fn(&mut S, B) -> C + Send + Sync>;
        let f: ScanFn<S, B, C> = Arc::new(f);
        Pipe::from_factory(move || {
            let child = parent();
            let state = init.clone();
            let f = Arc::clone(&f);
            Box::new(PullScan {
                child,
                state,
                f: move |s: &mut S, b: B| f(s, b),
            })
        })
    }

    /// Apply an async [`Operator`] to each element (may change type).
    pub fn pipe<C: Send + 'static, T: Operator<B, C> + Sync + 'static>(self, op: T) -> Pipe<C> {
        let parent = self.factory;
        let op: Arc<dyn Operator<B, C> + Send + Sync> = Arc::new(op);
        Pipe::from_factory(move || {
            let child = parent();
            Box::new(PullPipe {
                operator: Arc::clone(&op),
                child,
            })
        })
    }

    /// Keep elements matching the predicate.
    pub fn filter(self, f: impl Fn(&B) -> bool + Send + Sync + 'static) -> Self {
        let parent = self.factory;
        let f: Arc<dyn Fn(&B) -> bool + Send + Sync> = Arc::new(f);
        Self::from_factory(move || {
            let child = parent();
            let f = Arc::clone(&f);
            Box::new(PullFilter {
                child,
                f: move |b: &B| f(b),
            })
        })
    }

    /// Side-effect on each element, pass through unchanged.
    pub fn tap(self, f: impl Fn(&B) + Send + Sync + 'static) -> Self {
        let parent = self.factory;
        let f: Arc<dyn Fn(&B) + Send + Sync> = Arc::new(f);
        Self::from_factory(move || {
            let child = parent();
            let f = Arc::clone(&f);
            Box::new(PullTap {
                child,
                f: move |b: &B| f(b),
            })
        })
    }

    /// Switch to a new inner pipe on each element, cancelling the previous.
    ///
    /// Like [`flat_map`](Self::flat_map) but only the latest inner pipe
    /// runs at a time. When a new element arrives, the previous inner
    /// pipe is cancelled. Useful for "latest wins" patterns like
    /// autocomplete or route changes.
    pub fn switch_map<C: Send + 'static>(
        self,
        f: impl Fn(B) -> Pipe<C> + Send + Sync + 'static,
    ) -> Pipe<C> {
        let parent = self.factory;
        let f = Arc::new(f);
        Pipe::from_factory(move || {
            let (out_tx, out_rx) =
                tokio::sync::mpsc::channel::<Result<Vec<C>, crate::pull::PipeError>>(2);
            let mut outer = parent();
            let f = Arc::clone(&f);

            let handle = tokio::spawn(async move {
                let mut current_handle: Option<tokio::task::AbortHandle> = None;

                loop {
                    let chunk = tokio::select! {
                        result = outer.next_chunk() => result,
                        _ = out_tx.closed() => break,
                    };
                    match chunk {
                        Ok(Some(items)) => {
                            for item in items {
                                if let Some(h) = current_handle.take() {
                                    h.abort();
                                }
                                let inner = f(item);
                                let tx = out_tx.clone();
                                let h = tokio::spawn(async move {
                                    let mut pull = inner.into_pull();
                                    super::concurrency::drain_to_channel(&mut *pull, &tx).await;
                                });
                                current_handle = Some(h.abort_handle());
                            }
                        }
                        Ok(None) => break,
                        Err(e) => {
                            let _ = out_tx.send(Err(e)).await;
                            break;
                        }
                    }
                }
                // Let the last inner task finish naturally.
                // It holds a tx clone, so the channel stays open
                // until it completes.
                drop(current_handle);
            });

            Box::new(crate::channel::ChunkResultReceiver::new(
                out_rx,
                handle.abort_handle(),
            ))
        })
    }

    /// Alias for [`tap`](Self::tap) -- follows Rust iterator convention.
    pub fn inspect(self, f: impl Fn(&B) + Send + Sync + 'static) -> Self {
        self.tap(f)
    }

    /// Stop producing elements when the token is cancelled.
    ///
    /// The pipe returns `None` on the next pull after cancellation,
    /// allowing downstream to drain naturally instead of aborting.
    pub fn with_cancel(self, token: crate::cancel::CancelToken) -> Self {
        let parent = self.factory;
        Self::from_factory(move || {
            let child = parent();
            let token = token.clone();
            Box::new(crate::cancel::PullCancel { child, token })
        })
    }

    /// Insert a monitoring point that reports element counts and errors.
    ///
    /// The callback receives a [`MeterStats`](crate::meter::MeterStats)
    /// snapshot after each chunk, on completion, and on error.
    pub fn meter_with(
        self,
        name: impl Into<String>,
        on_chunk: impl Fn(&crate::meter::MeterStats) + Send + Sync + 'static,
    ) -> Self {
        let parent = self.factory;
        let name = name.into();
        let on_chunk: std::sync::Arc<dyn Fn(&crate::meter::MeterStats) + Send + Sync> =
            std::sync::Arc::new(on_chunk);
        Self::from_factory(move || {
            let child = parent();
            let name = name.clone();
            let on_chunk = std::sync::Arc::clone(&on_chunk);
            Box::new(crate::meter::PullMeter {
                child,
                on_chunk: Box::new(move |s| on_chunk(s)),
                stats: crate::meter::MeterStats {
                    name,
                    elements: 0,
                    chunks: 0,
                    completed: false,
                    errored: false,
                },
            })
        })
    }

    /// Take the first `n` elements.
    pub fn take(self, n: usize) -> Self {
        let parent = self.factory;
        Self::from_factory(move || {
            Box::new(PullTake {
                child: parent(),
                remaining: n,
            })
        })
    }

    /// Skip the first `n` elements.
    pub fn skip(self, n: usize) -> Self {
        let parent = self.factory;
        Self::from_factory(move || {
            Box::new(PullDrop {
                child: parent(),
                remaining: n,
            })
        })
    }

    /// Take elements while predicate holds, then stop.
    pub fn take_while(self, f: impl Fn(&B) -> bool + Send + Sync + 'static) -> Self {
        let parent = self.factory;
        let f: Arc<dyn Fn(&B) -> bool + Send + Sync> = Arc::new(f);
        Self::from_factory(move || {
            let child = parent();
            let f = Arc::clone(&f);
            Box::new(PullTakeWhile {
                child,
                f: move |b: &B| f(b),
                done: false,
            })
        })
    }

    /// Skip elements while predicate holds, then pass through.
    pub fn skip_while(self, f: impl Fn(&B) -> bool + Send + Sync + 'static) -> Self {
        let parent = self.factory;
        let f: Arc<dyn Fn(&B) -> bool + Send + Sync> = Arc::new(f);
        Self::from_factory(move || {
            let child = parent();
            let f = Arc::clone(&f);
            Box::new(PullSkipWhile {
                child,
                f: move |b: &B| f(b),
                skipping: true,
            })
        })
    }

    /// Group elements into fixed-size chunks, exposing batch boundaries.
    pub fn chunks(self, size: usize) -> Pipe<Vec<B>> {
        let size = size.max(1);
        let parent = self.factory;
        Pipe::from_factory(move || {
            Box::new(PullChunks {
                child: parent(),
                size,
            })
        })
    }

    /// Emit overlapping windows of `size` elements.
    ///
    /// Each window shares `size - 1` elements with the previous one:
    /// `[1,2,3], [2,3,4], [3,4,5], ...`
    ///
    /// Requires `B: Clone` since elements appear in multiple windows.
    pub fn sliding_window(self, size: usize) -> Pipe<Vec<B>>
    where
        B: Clone + Sync,
    {
        let size = size.max(1);
        let parent = self.factory;
        Pipe::from_factory(move || {
            Box::new(PullSlidingWindow {
                child: parent(),
                size,
                buffer: std::collections::VecDeque::with_capacity(size),
                done: false,
            })
        })
    }

    /// Apply a stream-level transform (FS2 `through`).
    ///
    /// The function receives this pipe and returns a new pipe.
    /// This is the primary composition mechanism for reusable transforms.
    ///
    /// ```ignore
    /// fn deduplicate(pipe: Pipe<i64>) -> Pipe<i64> {
    ///     pipe.scan(None, |prev, x| {
    ///         if *prev == Some(x) { None } else { *prev = Some(x); Some(x) }
    ///     }).and_then(|x| x)
    /// }
    /// let result = Pipe::from_iter(vec![1,1,2,2,3]).through(deduplicate).collect().await?;
    /// ```
    pub fn through<C: Send + 'static>(self, f: impl FnOnce(Pipe<B>) -> Pipe<C>) -> Pipe<C> {
        f(self)
    }

    /// Sequential composition: all elements of `self`, then all elements of `other`.
    pub fn chain(self, other: Pipe<B>) -> Self {
        let first = self.factory;
        let second = other.factory;
        Self::from_factory(move || {
            Box::new(PullChain {
                first: Some(first()),
                second: Some(second()),
            })
        })
    }

    /// Alternate elements from two pipes in round-robin order.
    ///
    /// Deterministic, unlike [`merge`](Self::merge) which interleaves
    /// by arrival time. When one pipe exhausts, remaining elements
    /// from the other are drained.
    pub fn interleave(self, other: Pipe<B>) -> Self {
        let left = self.factory;
        let right = other.factory;
        Self::from_factory(move || {
            Box::new(PullInterleave {
                left: left(),
                right: right(),
                left_turn: true,
                left_buf: std::collections::VecDeque::new(),
                right_buf: std::collections::VecDeque::new(),
                left_done: false,
                right_done: false,
            })
        })
    }

    /// Insert a separator between every two elements.
    ///
    /// `[1, 2, 3].intersperse(0)` -> `[1, 0, 2, 0, 3]`
    pub fn intersperse(self, separator: B) -> Self
    where
        B: Clone + Sync,
    {
        let sep = Arc::new(separator);
        let parent = self.factory;
        Self::from_factory(move || {
            let sep = Arc::clone(&sep);
            Box::new(PullIntersperse {
                child: parent(),
                separator: sep,
                buffer: Vec::new(),
                first: true,
                done: false,
            })
        })
    }

    /// Emit only when the value differs from the previous one.
    ///
    /// Uses `PartialEq` to compare consecutive elements. The first
    /// element is always emitted.
    pub fn changes(self) -> Self
    where
        B: PartialEq + Clone + Sync,
    {
        self.scan(None::<B>, |prev, item| {
            let emit = prev.as_ref() != Some(&item);
            *prev = Some(item.clone());
            if emit { Some(item) } else { None }
        })
        .and_then(|x| x)
    }

    /// Remove all duplicate elements, keeping only the first occurrence.
    ///
    /// **Memory**: holds a `HashSet` of every unique element seen so far.
    /// Memory grows linearly with the number of distinct values in the
    /// stream. For unbounded streams with high cardinality, prefer
    /// [`changes`](Self::changes) (consecutive-only, O(1) memory).
    pub fn distinct(self) -> Self
    where
        B: std::hash::Hash + Eq + Clone + Sync,
    {
        self.scan(std::collections::HashSet::<B>::new(), |seen, item| {
            if seen.insert(item.clone()) {
                Some(item)
            } else {
                None
            }
        })
        .and_then(|x| x)
    }

    /// Remove duplicates by key, keeping the first occurrence of each key.
    ///
    /// **Memory**: holds a `HashSet` of every unique key seen so far.
    /// Same memory considerations as [`distinct`](Self::distinct).
    pub fn distinct_by<K: std::hash::Hash + Eq + Clone + Send + Sync + 'static>(
        self,
        key: impl Fn(&B) -> K + Send + Sync + 'static,
    ) -> Self
    where
        B: Clone + Sync,
    {
        let key = Arc::new(key);
        self.scan(std::collections::HashSet::<K>::new(), move |seen, item| {
            if seen.insert(key(&item)) {
                Some(item)
            } else {
                None
            }
        })
        .and_then(|x| x)
    }

    /// Group consecutive elements that share the same key.
    ///
    /// Emits `(key, Vec<B>)` for each run of elements with equal keys.
    ///
    /// **Memory**: the current group is buffered in memory until the
    /// key changes. A single long run accumulates all its elements
    /// before emitting. For streams where runs can be very large,
    /// consider chunking or windowing first.
    pub fn group_adjacent_by<K: PartialEq + Send + 'static>(
        self,
        key: impl Fn(&B) -> K + Send + Sync + 'static,
    ) -> Pipe<(K, Vec<B>)> {
        let parent = self.factory;
        let key = Arc::new(key);
        Pipe::from_factory(move || {
            let child = parent();
            let key = Arc::clone(&key);
            Box::new(super::pull_ops::PullGroupAdjacentBy {
                child,
                key_fn: Box::new(move |b: &B| key(b)),
                current_key: None,
                current_group: Vec::new(),
                pending: std::collections::VecDeque::new(),
                done: false,
            })
        })
    }

    /// Drain the entire input, transform the whole buffer, then emit
    /// the result as a new stream.
    ///
    /// This is the sanctioned primitive for *blocking operators*:
    /// stages that cannot produce their first output row until every
    /// input row has been seen (sort, dedup, top-N, group-by). It
    /// replaces the hand-written
    /// `let v = child.collect().await?; Pipe::from_iter(transform(v))`
    /// idiom with a single combinator that needs no sync bridge.
    ///
    /// **Memory**: buffers 100% of the input before emitting. For
    /// operators that can emit incrementally, prefer a streaming
    /// combinator (e.g. [`group_adjacent_by`](Self::group_adjacent_by)
    /// or a bounded [`scan`](Self::scan)).
    ///
    /// See [`try_fold_collect`](Self::try_fold_collect) for a transform
    /// that can fail.
    pub fn fold_collect<C: Send + 'static>(
        self,
        transform: impl Fn(Vec<B>) -> Vec<C> + Send + Sync + 'static,
    ) -> Pipe<C> {
        self.try_fold_collect(move |buffer| Ok(transform(buffer)))
    }

    /// Like [`fold_collect`](Self::fold_collect), but the transform may
    /// fail. An `Err` aborts the pipe with that error.
    pub fn try_fold_collect<C: Send + 'static>(
        self,
        transform: impl Fn(Vec<B>) -> Result<Vec<C>, crate::pull::PipeError>
        + Send
        + Sync
        + 'static,
    ) -> Pipe<C> {
        let parent = self.factory;
        #[allow(clippy::type_complexity)]
        let transform: Arc<
            dyn Fn(Vec<B>) -> Result<Vec<C>, crate::pull::PipeError> + Send + Sync,
        > = Arc::new(transform);
        Pipe::from_factory(move || {
            Box::new(super::pull_ops::PullFoldCollect {
                child: parent(),
                transform: Arc::clone(&transform),
                done: false,
            })
        })
    }

    /// Pair each element with its index: `(0, a), (1, b), (2, c), ...`
    pub fn enumerate(self) -> Pipe<(usize, B)> {
        self.scan(0usize, |idx, item| {
            let i = *idx;
            *idx += 1;
            (i, item)
        })
    }

    /// Wrap each element in `Ok`, catching errors as `Err` elements.
    ///
    /// Unlike normal error propagation which stops the pipe, `attempt`
    /// converts errors into elements so the pipe continues.
    pub fn attempt(self) -> Pipe<Result<B, crate::pull::PipeError>> {
        let parent = self.factory;
        Pipe::from_factory(move || Box::new(super::pull_ops::PullAttempt::new(parent())))
    }

    /// Wrap each element in `Some`, then emit `None` at the end.
    ///
    /// Paired with [`un_none_terminate`](Pipe::<Option<B>>::un_none_terminate)
    /// for Option-based stream termination protocols.
    pub fn none_terminate(self) -> Pipe<Option<B>> {
        let parent = self.factory;
        Pipe::from_factory(move || {
            Box::new(super::pull_ops::PullNoneTerminate {
                child: parent(),
                done: false,
            })
        })
    }
}

/// Unwrap `Option`-wrapped elements, stopping at the first `None`.
impl<B: Send + 'static> Pipe<Option<B>> {
    pub fn un_none_terminate(self) -> Pipe<B> {
        let parent = self.factory;
        Pipe::from_factory(move || {
            Box::new(super::pull_ops::PullUnNoneTerminate { child: parent() })
        })
    }
}
