//! Internal pull operator structs used by pipeline operators.

use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, VecDeque};
use std::future::Future;
use std::sync::Arc;

use crate::pull::{ChunkFut, PipeError, PullOperator, YIELD_AFTER_EMPTY, collect_all};

use super::Pipe;

type AcquireFn<R> = Arc<
    dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<R, PipeError>> + Send>>
        + Send
        + Sync,
>;

pub(super) enum BracketState<B: Send + 'static, R: Send + Sync + 'static> {
    Pending {
        acquire: AcquireFn<R>,
        use_resource: Arc<dyn Fn(Arc<R>) -> Pipe<B> + Send + Sync>,
        release: Arc<dyn Fn(Arc<R>) + Send + Sync>,
    },
    Active {
        inner: Box<dyn PullOperator<B>>,
        resource: Arc<R>,
        release: Arc<dyn Fn(Arc<R>) + Send + Sync>,
    },
    Done,
}

pub(super) struct PullBracket<B: Send + 'static, R: Send + Sync + 'static> {
    pub(super) state: BracketState<B, R>,
}

impl<B: Send + 'static, R: Send + Sync + 'static> Drop for PullBracket<B, R> {
    fn drop(&mut self) {
        let old = std::mem::replace(&mut self.state, BracketState::Done);
        if let BracketState::Active {
            release, resource, ..
        } = old
        {
            release(resource);
        }
    }
}

impl<B: Send + 'static, R: Send + Sync + 'static> PullOperator<B> for PullBracket<B, R> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            if let BracketState::Pending { .. } = &self.state {
                let old = std::mem::replace(&mut self.state, BracketState::Done);
                if let BracketState::Pending {
                    acquire,
                    use_resource,
                    release,
                } = old
                {
                    let resource = Arc::new(acquire().await?);
                    let pipe = use_resource(Arc::clone(&resource));
                    let inner = pipe.into_pull();
                    self.state = BracketState::Active {
                        inner,
                        resource,
                        release,
                    };
                }
            }

            match &mut self.state {
                BracketState::Active {
                    inner,
                    resource,
                    release,
                } => match inner.next_chunk().await {
                    Ok(Some(chunk)) => Ok(Some(chunk)),
                    Ok(None) => {
                        let release = Arc::clone(release);
                        let resource = Arc::clone(resource);
                        self.state = BracketState::Done;
                        release(resource);
                        Ok(None)
                    }
                    Err(e) => {
                        let release = Arc::clone(release);
                        let resource = Arc::clone(resource);
                        self.state = BracketState::Done;
                        release(resource);
                        Err(e)
                    }
                },
                _ => Ok(None),
            }
        })
    }
}

pub(super) struct PullInterleave<B: Send + 'static> {
    pub(super) left: Box<dyn PullOperator<B>>,
    pub(super) right: Box<dyn PullOperator<B>>,
    pub(super) left_turn: bool,
    pub(super) left_buf: VecDeque<B>,
    pub(super) right_buf: VecDeque<B>,
    pub(super) left_done: bool,
    pub(super) right_done: bool,
}

impl<B: Send + 'static> PullOperator<B> for PullInterleave<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            let mut result = Vec::new();

            loop {
                // Refill buffers as needed
                if self.left_buf.is_empty() && !self.left_done {
                    match self.left.next_chunk().await? {
                        Some(chunk) => self.left_buf.extend(chunk),
                        None => self.left_done = true,
                    }
                }
                if self.right_buf.is_empty() && !self.right_done {
                    match self.right.next_chunk().await? {
                        Some(chunk) => self.right_buf.extend(chunk),
                        None => self.right_done = true,
                    }
                }

                if self.left_buf.is_empty() && self.right_buf.is_empty() {
                    break;
                }

                // Interleave one element at a time
                let limit = self.left_buf.len().max(self.right_buf.len()) * 2;
                for _ in 0..limit {
                    if self.left_turn {
                        if let Some(item) = self.left_buf.pop_front() {
                            result.push(item);
                        }
                    } else if let Some(item) = self.right_buf.pop_front() {
                        result.push(item);
                    }
                    self.left_turn = !self.left_turn;

                    if self.left_buf.is_empty() && self.right_buf.is_empty() {
                        break;
                    }
                }

                if !result.is_empty() {
                    return Ok(Some(result));
                }
            }

            if result.is_empty() {
                Ok(None)
            } else {
                Ok(Some(result))
            }
        })
    }
}

pub(super) struct PullIntersperse<B: Send + 'static> {
    pub(super) child: Box<dyn PullOperator<B>>,
    pub(super) separator: Arc<B>,
    pub(super) buffer: Vec<B>,
    pub(super) first: bool,
    pub(super) done: bool,
}

impl<B: Clone + Send + Sync + 'static> PullOperator<B> for PullIntersperse<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            if self.done {
                return Ok(None);
            }
            match self.child.next_chunk().await? {
                Some(chunk) => {
                    self.buffer.clear();
                    let capacity = if self.first {
                        chunk.len().saturating_mul(2).saturating_sub(1)
                    } else {
                        chunk.len().saturating_mul(2)
                    };
                    self.buffer.reserve(capacity);
                    for item in chunk {
                        if !self.first {
                            self.buffer.push((*self.separator).clone());
                        }
                        self.buffer.push(item);
                        self.first = false;
                    }
                    Ok(Some(std::mem::take(&mut self.buffer)))
                }
                None => {
                    self.done = true;
                    Ok(None)
                }
            }
        })
    }
}

pub(super) struct PullThrottle<B: Send + 'static> {
    pub(super) child: Box<dyn PullOperator<B>>,
    pub(super) duration: std::time::Duration,
    pub(super) last: Option<tokio::time::Instant>,
}

impl<B: Send + 'static> PullOperator<B> for PullThrottle<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            match self.child.next_chunk().await? {
                Some(chunk) => {
                    let mut result = Vec::with_capacity(chunk.len());
                    for item in chunk {
                        if let Some(last) = self.last {
                            let elapsed = tokio::time::Instant::now() - last;
                            if elapsed < self.duration {
                                tokio::time::sleep(self.duration - elapsed).await;
                            }
                        }
                        self.last = Some(tokio::time::Instant::now());
                        result.push(item);
                    }
                    Ok(Some(result))
                }
                None => Ok(None),
            }
        })
    }
}

pub(super) struct PullFlatten<B: Send + 'static> {
    pub(super) outer: Box<dyn PullOperator<Pipe<B>>>,
    pub(super) inner: Option<Box<dyn PullOperator<B>>>,
    pub(super) pending: VecDeque<Pipe<B>>,
}

impl<B: Send + 'static> PullOperator<B> for PullFlatten<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            loop {
                // Drain current inner pipe
                if let Some(ref mut inner) = self.inner {
                    match inner.next_chunk().await? {
                        Some(chunk) => return Ok(Some(chunk)),
                        None => {
                            self.inner = None;
                        }
                    }
                }
                // Try next pending pipe
                if let Some(pipe) = self.pending.pop_front() {
                    self.inner = Some(pipe.into_pull());
                    continue;
                }
                // Get next batch of pipes from outer
                match self.outer.next_chunk().await? {
                    Some(pipes) => {
                        self.pending.extend(pipes);
                    }
                    None => return Ok(None),
                }
            }
        })
    }
}

pub(super) struct PullSlidingWindow<B: Send + 'static> {
    pub(super) child: Box<dyn PullOperator<B>>,
    pub(super) size: usize,
    pub(super) buffer: VecDeque<B>,
    pub(super) done: bool,
}

impl<B: Clone + Send + 'static> PullOperator<Vec<B>> for PullSlidingWindow<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, Vec<B>> {
        Box::pin(async move {
            let mut windows = Vec::new();

            loop {
                // Try to emit windows from current buffer
                while self.buffer.len() >= self.size {
                    let window: Vec<B> = self.buffer.iter().take(self.size).cloned().collect();
                    windows.push(window);
                    self.buffer.pop_front();
                }

                if !windows.is_empty() {
                    return Ok(Some(windows));
                }

                if self.done {
                    return Ok(None);
                }

                // Pull more data
                match self.child.next_chunk().await? {
                    Some(chunk) => {
                        self.buffer.extend(chunk);
                    }
                    None => {
                        self.done = true;
                    }
                }
            }
        })
    }
}

pub(super) struct PullRepeatWith<B> {
    pub(super) f: Box<dyn Fn() -> B + Send>,
    pub(super) chunk_size: usize,
}

impl<B: Send + 'static> PullOperator<B> for PullRepeatWith<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            let chunk: Vec<B> = (0..self.chunk_size).map(|_| (self.f)()).collect();
            Ok(Some(chunk))
        })
    }
}

pub(super) struct PullInterval {
    pub(super) interval: tokio::time::Interval,
    pub(super) chunk_size: usize,
}

impl PullOperator<tokio::time::Instant> for PullInterval {
    fn next_chunk(&mut self) -> ChunkFut<'_, tokio::time::Instant> {
        Box::pin(async move {
            let mut chunk = Vec::with_capacity(self.chunk_size);
            for _ in 0..self.chunk_size {
                chunk.push(self.interval.tick().await);
            }
            Ok(Some(chunk))
        })
    }
}

pub(super) struct PullTimeout<B: Send + 'static> {
    pub(super) child: Box<dyn PullOperator<B>>,
    pub(super) duration: std::time::Duration,
}

impl<B: Send + 'static> PullOperator<B> for PullTimeout<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            match tokio::time::timeout(self.duration, self.child.next_chunk()).await {
                Ok(result) => result,
                Err(_) => Err(PipeError::Custom("timeout".into())),
            }
        })
    }
}

pub(super) struct PullChain<B: Send + 'static> {
    pub(super) first: Option<Box<dyn PullOperator<B>>>,
    pub(super) second: Option<Box<dyn PullOperator<B>>>,
}

impl<B: Send + 'static> PullOperator<B> for PullChain<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            if let Some(ref mut first) = self.first {
                match first.next_chunk().await? {
                    Some(chunk) => return Ok(Some(chunk)),
                    None => {
                        self.first = None;
                    }
                }
            }
            if let Some(ref mut second) = self.second {
                match second.next_chunk().await? {
                    Some(chunk) => return Ok(Some(chunk)),
                    None => {
                        self.second = None;
                    }
                }
            }
            Ok(None)
        })
    }
}

/// Shared state for lazy broadcast. Initialized on first branch access.
pub(super) struct LazyFanOut<B: Send + 'static> {
    n: usize,
    buffer_size: usize,
    source_factory: Arc<dyn Fn() -> Box<dyn PullOperator<B>> + Send + Sync>,
    state: std::sync::OnceLock<LazyFanOutState<B>>,
}

type FanOutReceivers<B> = std::sync::Mutex<Vec<Option<tokio::sync::mpsc::Receiver<Arc<Vec<B>>>>>>;

struct LazyFanOutState<B> {
    receivers: FanOutReceivers<B>,
    abort: tokio::task::AbortHandle,
}

impl<B: Clone + Send + Sync + 'static> LazyFanOut<B> {
    pub(super) fn new(
        n: usize,
        buffer_size: usize,
        source_factory: Arc<dyn Fn() -> Box<dyn PullOperator<B>> + Send + Sync>,
    ) -> Self {
        Self {
            n,
            buffer_size,
            source_factory,
            state: std::sync::OnceLock::new(),
        }
    }

    fn ensure_init(&self) -> &LazyFanOutState<B> {
        self.state.get_or_init(|| {
            let mut senders = Vec::with_capacity(self.n);
            let mut receivers = Vec::with_capacity(self.n);
            for _ in 0..self.n {
                let (tx, rx) = tokio::sync::mpsc::channel::<Arc<Vec<B>>>(self.buffer_size.max(1));
                senders.push(tx);
                receivers.push(Some(rx));
            }

            let mut root = (self.source_factory)();
            let handle = tokio::spawn(async move {
                while let Ok(Some(chunk)) = root.next_chunk().await {
                    let shared: Arc<Vec<B>> = Arc::new(chunk);
                    for tx in &senders {
                        if tx.send(Arc::clone(&shared)).await.is_err() {
                            return;
                        }
                    }
                }
            });

            LazyFanOutState {
                receivers: std::sync::Mutex::new(receivers),
                abort: handle.abort_handle(),
            }
        })
    }

    pub(super) fn take_broadcast_receiver(
        &self,
        idx: usize,
    ) -> tokio::sync::mpsc::Receiver<Arc<Vec<B>>> {
        let state = self.ensure_init();
        state.receivers.lock().unwrap()[idx]
            .take()
            .expect("broadcast branch already consumed")
    }

    pub(super) fn abort_handle(&self) -> tokio::task::AbortHandle {
        self.ensure_init().abort.clone()
    }
}

/// Shared state for lazy partition. Initialized on first branch access.
pub(super) struct LazyPartition<B: Send + 'static> {
    n: usize,
    buffer_size: usize,
    source_factory: Arc<dyn Fn() -> Box<dyn PullOperator<B>> + Send + Sync>,
    key: Arc<dyn Fn(&B) -> u64 + Send + Sync>,
    state: std::sync::OnceLock<LazyPartitionState<B>>,
}

type PartitionReceivers<B> =
    std::sync::Mutex<Vec<Option<(crate::channel::Receiver<B>, Arc<SharedAbort>)>>>;

struct LazyPartitionState<B> {
    receivers: PartitionReceivers<B>,
}

impl<B: Send + 'static> LazyPartition<B> {
    pub(super) fn new(
        n: usize,
        buffer_size: usize,
        source_factory: Arc<dyn Fn() -> Box<dyn PullOperator<B>> + Send + Sync>,
        key: impl Fn(&B) -> u64 + Send + Sync + 'static,
    ) -> Self {
        Self {
            n,
            buffer_size,
            source_factory,
            key: Arc::new(key),
            state: std::sync::OnceLock::new(),
        }
    }

    fn ensure_init(&self) -> &LazyPartitionState<B> {
        self.state.get_or_init(|| {
            let n = self.n;
            let mut senders = Vec::with_capacity(n);
            let mut receivers = Vec::with_capacity(n);
            for _ in 0..n {
                let (tx, rx) = crate::channel::bounded::<B>(self.buffer_size);
                senders.push(tx);
                receivers.push(rx);
            }

            let mut root = (self.source_factory)();
            let key = Arc::clone(&self.key);
            let handle = tokio::spawn(async move {
                let mut buckets: Vec<Vec<B>> = (0..n).map(|_| Vec::new()).collect();
                while let Ok(Some(chunk)) = root.next_chunk().await {
                    for item in chunk {
                        let idx = key(&item) as usize % n;
                        buckets[idx].push(item);
                    }
                    for (i, bucket) in buckets.iter_mut().enumerate() {
                        if !bucket.is_empty() {
                            let batch = std::mem::take(bucket);
                            if senders[i].send(batch).await.is_err() {
                                return;
                            }
                        }
                    }
                }
            });

            let abort = SharedAbort::new(handle.abort_handle());
            let receivers: Vec<_> = receivers
                .into_iter()
                .map(|rx| Some((rx, Arc::clone(&abort))))
                .collect();

            LazyPartitionState {
                receivers: std::sync::Mutex::new(receivers),
            }
        })
    }

    pub(super) fn take_receiver(
        &self,
        idx: usize,
    ) -> (crate::channel::Receiver<B>, Arc<SharedAbort>) {
        let state = self.ensure_init();
        state.receivers.lock().unwrap()[idx]
            .take()
            .expect("partition branch already consumed")
    }
}

/// Groups consecutive elements with the same key.
pub(super) struct PullGroupAdjacentBy<B: Send + 'static, K> {
    pub(super) child: Box<dyn PullOperator<B>>,
    pub(super) key_fn: Box<dyn Fn(&B) -> K + Send + Sync>,
    pub(super) current_key: Option<K>,
    pub(super) current_group: Vec<B>,
    pub(super) pending: VecDeque<B>,
    pub(super) done: bool,
}

impl<B, K> PullOperator<(K, Vec<B>)> for PullGroupAdjacentBy<B, K>
where
    B: Send + 'static,
    K: PartialEq + Send + 'static,
{
    fn next_chunk(&mut self) -> ChunkFut<'_, (K, Vec<B>)> {
        Box::pin(async {
            loop {
                // Process pending items first
                if let Some(item) = self.pending.pop_front() {
                    let k = (self.key_fn)(&item);
                    match &self.current_key {
                        Some(ck) if *ck == k => {
                            self.current_group.push(item);
                        }
                        Some(_) => {
                            let group = std::mem::take(&mut self.current_group);
                            let prev_key = self.current_key.take().unwrap();
                            self.current_key = Some(k);
                            self.current_group.push(item);
                            return Ok(Some(vec![(prev_key, group)]));
                        }
                        None => {
                            self.current_key = Some(k);
                            self.current_group.push(item);
                        }
                    }
                    continue;
                }

                if self.done {
                    if self.current_group.is_empty() {
                        return Ok(None);
                    }
                    let group = std::mem::take(&mut self.current_group);
                    let key = self.current_key.take().unwrap();
                    return Ok(Some(vec![(key, group)]));
                }

                match self.child.next_chunk().await? {
                    Some(chunk) => {
                        self.pending.extend(chunk);
                    }
                    None => {
                        self.done = true;
                    }
                }
            }
        })
    }
}

/// Partition receiver that holds a ref-counted abort guard.
/// Source task is only cancelled when ALL partition branches are dropped.
pub(super) struct PartitionReceiver<B: Send + 'static> {
    pub(super) inner: crate::channel::Receiver<B>,
    pub(super) _abort: Arc<SharedAbort>,
}

impl<B: Send + 'static> PullOperator<B> for PartitionReceiver<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        self.inner.next_chunk()
    }
}

/// Runs a finalizer when the inner pipe completes, errors, or is dropped.
pub(super) struct PullOnFinalize<B: Send + 'static> {
    pub(super) child: Box<dyn PullOperator<B>>,
    pub(super) finalizer: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl<B: Send + 'static> Drop for PullOnFinalize<B> {
    fn drop(&mut self) {
        if let Some(f) = self.finalizer.take() {
            f();
        }
    }
}

impl<B: Send + 'static> PullOperator<B> for PullOnFinalize<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async {
            match self.child.next_chunk().await {
                Ok(Some(chunk)) => Ok(Some(chunk)),
                done => {
                    if let Some(f) = self.finalizer.take() {
                        f();
                    }
                    done
                }
            }
        })
    }
}

/// Wraps a PullOperator with abort handles that cancel background tasks on drop.
pub(super) struct GuardedPull<B: Send + 'static> {
    pub(super) inner: Box<dyn PullOperator<B>>,
    pub(super) _guards: Vec<tokio::task::AbortHandle>,
}

impl<B: Send + 'static> Drop for GuardedPull<B> {
    fn drop(&mut self) {
        for handle in &self._guards {
            handle.abort();
        }
    }
}

impl<B: Send + 'static> PullOperator<B> for GuardedPull<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        self.inner.next_chunk()
    }
}

/// Aborts the background task only when the last clone is dropped.
pub(super) struct SharedAbort {
    handle: tokio::task::AbortHandle,
}

impl Drop for SharedAbort {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl SharedAbort {
    pub(super) fn new(handle: tokio::task::AbortHandle) -> Arc<Self> {
        Arc::new(Self { handle })
    }
}

/// Receives `Arc<Vec<B>>` chunks from broadcast and clones elements on pull.
///
/// Holds a ref-counted abort guard so the source task is only cancelled
/// when ALL branches are dropped.
pub(super) struct BroadcastReceiver<B> {
    pub(super) rx: tokio::sync::mpsc::Receiver<std::sync::Arc<Vec<B>>>,
    pub(super) _abort: Arc<SharedAbort>,
}

impl<B: Clone + Send + Sync + 'static> PullOperator<B> for BroadcastReceiver<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            match self.rx.recv().await {
                Some(arc) => Ok(Some(match Arc::try_unwrap(arc) {
                    Ok(vec) => vec,
                    Err(arc) => (*arc).clone(),
                })),
                None => Ok(None),
            }
        })
    }
}

pub(super) struct PullTakeWhile<B, F> {
    pub(super) child: Box<dyn PullOperator<B>>,
    pub(super) f: F,
    pub(super) done: bool,
}

impl<B: Send + 'static, F: Fn(&B) -> bool + Send + 'static> PullOperator<B>
    for PullTakeWhile<B, F>
{
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            if self.done {
                return Ok(None);
            }
            match self.child.next_chunk().await? {
                Some(chunk) => {
                    let mut taken = Vec::new();
                    for item in chunk {
                        if (self.f)(&item) {
                            taken.push(item);
                        } else {
                            self.done = true;
                            break;
                        }
                    }
                    if taken.is_empty() {
                        Ok(None)
                    } else {
                        Ok(Some(taken))
                    }
                }
                None => Ok(None),
            }
        })
    }
}

pub(super) struct PullSkipWhile<B, F> {
    pub(super) child: Box<dyn PullOperator<B>>,
    pub(super) f: F,
    pub(super) skipping: bool,
}

impl<B: Send + 'static, F: Fn(&B) -> bool + Send + 'static> PullOperator<B>
    for PullSkipWhile<B, F>
{
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            loop {
                match self.child.next_chunk().await? {
                    Some(chunk) => {
                        if !self.skipping {
                            return Ok(Some(chunk));
                        }
                        let mut rest = Vec::new();
                        for item in chunk {
                            if self.skipping {
                                if !(self.f)(&item) {
                                    self.skipping = false;
                                    rest.push(item);
                                }
                            } else {
                                rest.push(item);
                            }
                        }
                        if !rest.is_empty() {
                            return Ok(Some(rest));
                        }
                    }
                    None => return Ok(None),
                }
            }
        })
    }
}

pub(super) struct PullChunks<B> {
    pub(super) child: Box<dyn PullOperator<B>>,
    pub(super) size: usize,
}

impl<B: Send + 'static> PullOperator<Vec<B>> for PullChunks<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, Vec<B>> {
        Box::pin(async move {
            let mut buffer = Vec::with_capacity(self.size);
            while buffer.len() < self.size {
                match self.child.next_chunk().await? {
                    Some(chunk) => buffer.extend(chunk),
                    None => break,
                }
            }
            if buffer.is_empty() {
                return Ok(None);
            }
            // Split buffer into sized groups using drain to avoid re-iteration
            let num_groups = buffer.len().div_ceil(self.size);
            let mut groups = Vec::with_capacity(num_groups);
            while buffer.len() > self.size {
                let rest = buffer.split_off(self.size);
                groups.push(std::mem::replace(&mut buffer, rest));
            }
            if !buffer.is_empty() {
                groups.push(buffer);
            }
            Ok(Some(groups))
        })
    }
}

pub(super) struct PullEvalMap<B, F> {
    pub(super) child: Box<dyn PullOperator<B>>,
    pub(super) f: F,
}

impl<A: Send + 'static, B: Send + 'static, Fut, F> PullOperator<B> for PullEvalMap<A, F>
where
    Fut: Future<Output = Result<B, PipeError>> + Send,
    F: Fn(A) -> Fut + Send + 'static,
{
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            match self.child.next_chunk().await? {
                Some(chunk) => {
                    let mut result = Vec::with_capacity(chunk.len());
                    for item in chunk {
                        result.push((self.f)(item).await?);
                    }
                    Ok(Some(result))
                }
                None => Ok(None),
            }
        })
    }
}

pub(super) struct PullEvalFilter<B, F> {
    pub(super) child: Box<dyn PullOperator<B>>,
    pub(super) f: F,
}

impl<B: Send + 'static, Fut, F> PullOperator<B> for PullEvalFilter<B, F>
where
    Fut: Future<Output = Result<bool, PipeError>> + Send,
    F: Fn(&B) -> Fut + Send + 'static,
{
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            let mut empty_runs = 0usize;
            loop {
                match self.child.next_chunk().await? {
                    Some(chunk) => {
                        let mut filtered = Vec::new();
                        for item in chunk {
                            if (self.f)(&item).await? {
                                filtered.push(item);
                            }
                        }
                        if !filtered.is_empty() {
                            return Ok(Some(filtered));
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

pub(super) struct PullEvalTap<B, F> {
    pub(super) child: Box<dyn PullOperator<B>>,
    pub(super) f: F,
}

impl<B: Send + Sync + 'static, Fut, F> PullOperator<B> for PullEvalTap<B, F>
where
    Fut: Future<Output = Result<(), PipeError>> + Send,
    F: Fn(&B) -> Fut + Send + 'static,
{
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            match self.child.next_chunk().await? {
                Some(chunk) => {
                    for item in &chunk {
                        (self.f)(item).await?;
                    }
                    Ok(Some(chunk))
                }
                None => Ok(None),
            }
        })
    }
}

pub(super) struct PullHandleError<B: Send + 'static> {
    pub(super) child: Box<dyn PullOperator<B>>,
    pub(super) handler: Box<dyn Fn(PipeError) -> Pipe<B> + Send>,
    pub(super) fallback: Option<Box<dyn PullOperator<B>>>,
}

impl<B: Send + 'static> PullOperator<B> for PullHandleError<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            // If we're in fallback mode, drain the fallback pipe
            if let Some(ref mut fb) = self.fallback {
                return fb.next_chunk().await;
            }
            match self.child.next_chunk().await {
                Ok(chunk) => Ok(chunk),
                Err(e) => {
                    // Switch to fallback stream
                    let pipe = (self.handler)(e);
                    self.fallback = Some(pipe.into_pull());
                    // Pull first chunk from fallback
                    self.fallback.as_mut().unwrap().next_chunk().await
                }
            }
        })
    }
}

pub(super) struct PullRetry<B: Send + 'static> {
    pub(super) factory: Box<dyn Fn() -> Pipe<B> + Send>,
    pub(super) current: Option<Box<dyn PullOperator<B>>>,
    pub(super) remaining: usize,
}

impl<B: Send + 'static> PullOperator<B> for PullRetry<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            loop {
                if self.current.is_none() {
                    if self.remaining == 0 {
                        return Err(PipeError::RetryExhausted);
                    }
                    self.remaining -= 1;
                    self.current = Some((self.factory)().into_pull());
                }
                match self.current.as_mut().unwrap().next_chunk().await {
                    Ok(chunk) => return Ok(chunk),
                    Err(_e) if self.remaining > 0 => {
                        self.current = None; // retry
                    }
                    Err(e) => return Err(e),
                }
            }
        })
    }
}

pub(super) struct PullConcurrently<B: Send + 'static> {
    pub(super) inner: Box<dyn PullOperator<B>>,
    pub(super) bg_error: tokio::sync::watch::Receiver<Option<String>>,
    pub(super) abort: Option<tokio::task::AbortHandle>,
}

impl<B: Send + 'static> Drop for PullConcurrently<B> {
    fn drop(&mut self) {
        if let Some(handle) = self.abort.take() {
            handle.abort();
        }
    }
}

impl<B: Send + 'static> PullOperator<B> for PullConcurrently<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            // Check for background error before each pull
            if let Some(msg) = &*self.bg_error.borrow() {
                return Err(PipeError::Custom(msg.clone().into()));
            }
            self.inner.next_chunk().await
        })
    }
}

pub(super) struct PullAttempt<B: Send + 'static> {
    pub(super) child: Box<dyn PullOperator<B>>,
    done: bool,
}

impl<B: Send + 'static> PullAttempt<B> {
    pub(super) fn new(child: Box<dyn PullOperator<B>>) -> Self {
        Self { child, done: false }
    }
}

impl<B: Send + 'static> PullOperator<Result<B, PipeError>> for PullAttempt<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, Result<B, PipeError>> {
        Box::pin(async move {
            if self.done {
                return Ok(None);
            }
            match self.child.next_chunk().await {
                Ok(Some(chunk)) => Ok(Some(chunk.into_iter().map(Ok).collect())),
                Ok(None) => Ok(None),
                Err(e) => {
                    self.done = true;
                    Ok(Some(vec![Err(e)]))
                }
            }
        })
    }
}

pub(super) struct PullNoneTerminate<B: Send + 'static> {
    pub(super) child: Box<dyn PullOperator<B>>,
    pub(super) done: bool,
}

impl<B: Send + 'static> PullOperator<Option<B>> for PullNoneTerminate<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, Option<B>> {
        Box::pin(async move {
            if self.done {
                return Ok(None);
            }
            match self.child.next_chunk().await? {
                Some(chunk) => Ok(Some(chunk.into_iter().map(Some).collect())),
                None => {
                    self.done = true;
                    Ok(Some(vec![None]))
                }
            }
        })
    }
}

pub(super) struct PullUnNoneTerminate<B: Send + 'static> {
    pub(super) child: Box<dyn PullOperator<Option<B>>>,
}

impl<B: Send + 'static> PullOperator<B> for PullUnNoneTerminate<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            match self.child.next_chunk().await? {
                Some(chunk) => {
                    let mut result = Vec::with_capacity(chunk.len());
                    for item in chunk {
                        match item {
                            Some(val) => result.push(val),
                            None => {
                                return if result.is_empty() {
                                    Ok(None)
                                } else {
                                    Ok(Some(result))
                                };
                            }
                        }
                    }
                    if result.is_empty() {
                        Ok(None)
                    } else {
                        Ok(Some(result))
                    }
                }
                None => Ok(None),
            }
        })
    }
}

/// An element paired with its sort key. Ordered by key only, so the
/// element type itself need not be `Ord`.
pub(super) struct Keyed<K, B> {
    pub(super) key: K,
    pub(super) item: B,
}

impl<K: PartialEq, B> PartialEq for Keyed<K, B> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<K: Eq, B> Eq for Keyed<K, B> {}

impl<K: PartialOrd, B> PartialOrd for Keyed<K, B> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.key.partial_cmp(&other.key)
    }
}

impl<K: Ord, B> Ord for Keyed<K, B> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

/// Bounded top-N operator: keeps at most `k` elements with the largest
/// keys in a min-heap (O(k) memory), then emits them in descending key
/// order once the child is exhausted.
pub(super) struct PullTopN<B: Send + 'static, K: Ord + Send + 'static> {
    pub(super) child: Box<dyn PullOperator<B>>,
    pub(super) k: usize,
    pub(super) key_fn: Arc<dyn Fn(&B) -> K + Send + Sync>,
    pub(super) heap: BinaryHeap<Reverse<Keyed<K, B>>>,
    pub(super) done: bool,
}

impl<B: Send + 'static, K: Ord + Send + 'static> PullOperator<B> for PullTopN<B, K> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            if self.done || self.k == 0 {
                self.done = true;
                return Ok(None);
            }
            while let Some(chunk) = self.child.next_chunk().await? {
                for item in chunk {
                    let key = (self.key_fn)(&item);
                    self.heap.push(Reverse(Keyed { key, item }));
                    if self.heap.len() > self.k {
                        self.heap.pop();
                    }
                }
            }
            self.done = true;
            // `into_sorted_vec` on `Reverse<_>` yields descending key order.
            let out: Vec<B> = std::mem::take(&mut self.heap)
                .into_sorted_vec()
                .into_iter()
                .map(|Reverse(keyed)| keyed.item)
                .collect();
            if out.is_empty() { Ok(None) } else { Ok(Some(out)) }
        })
    }
}

/// Blocking operator: drains the child to completion, transforms the
/// whole buffer, then emits the result as a single chunk.
pub(super) struct PullFoldCollect<B: Send + 'static, C: Send + 'static> {
    pub(super) child: Box<dyn PullOperator<B>>,
    #[allow(clippy::type_complexity)]
    pub(super) transform: Arc<dyn Fn(Vec<B>) -> Result<Vec<C>, PipeError> + Send + Sync>,
    pub(super) done: bool,
}

impl<B: Send + 'static, C: Send + 'static> PullOperator<C> for PullFoldCollect<B, C> {
    fn next_chunk(&mut self) -> ChunkFut<'_, C> {
        Box::pin(async move {
            if self.done {
                return Ok(None);
            }
            self.done = true;
            let buffer = collect_all(&mut *self.child).await?;
            let out = (self.transform)(buffer)?;
            if out.is_empty() { Ok(None) } else { Ok(Some(out)) }
        })
    }
}
