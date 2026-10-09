//! Constructors for `Pipe<B>`.

use std::future::Future;
use std::sync::Arc;

use crate::pull::{ArcSource, PipeError, PullIterate, PullOperator, PullSource, PullUnfold};

use super::pull_ops::PullRepeatWith;
use super::{Emitter, Pipe};

impl<B: Send + 'static> Pipe<B> {
    /// Create a stream from an iterator of elements.
    #[allow(clippy::should_implement_trait)]
    pub fn from_iter(items: impl IntoIterator<Item = B>) -> Self
    where
        B: Clone + Sync,
    {
        let data: Arc<[B]> = items.into_iter().collect::<Vec<B>>().into();
        Self::from_factory(move || Box::new(ArcSource::new(Arc::clone(&data))))
    }

    /// Create a stream of one element.
    pub fn once(item: B) -> Self
    where
        B: Clone + Sync,
    {
        let data: Arc<[B]> = Arc::from(vec![item]);
        Self::from_factory(move || Box::new(ArcSource::new(Arc::clone(&data))))
    }

    /// Create an empty stream.
    pub fn empty() -> Self {
        Self::from_factory(|| Box::new(PullSource::<B>::empty()))
    }

    /// Lazy generation from a seed. Produces elements until `step` returns `None`.
    pub fn unfold<S: Clone + Send + Sync + 'static>(
        seed: S,
        step: impl Fn(&mut S) -> Option<B> + Send + Sync + 'static,
    ) -> Self {
        type StepFn<S, B> = Arc<dyn Fn(&mut S) -> Option<B> + Send + Sync>;
        let step: StepFn<S, B> = Arc::new(step);
        Self::from_factory(move || {
            let seed = seed.clone();
            let step = Arc::clone(&step);
            Box::new(PullUnfold::new(seed, move |s: &mut S| step(s)))
        })
    }

    /// Infinite stream: `init, f(init), f(f(init)), ...`
    pub fn iterate(init: B, step: impl Fn(&B) -> B + Send + Sync + 'static) -> Self
    where
        B: Clone + Sync,
    {
        let step: Arc<dyn Fn(&B) -> B + Send + Sync> = Arc::new(step);
        Self::from_factory(move || {
            let init = init.clone();
            let step = Arc::clone(&step);
            Box::new(PullIterate::new(init, move |b: &B| step(b)))
        })
    }

    /// Infinite stream repeating the same value.
    pub fn repeat(item: B) -> Self
    where
        B: Clone + Sync,
    {
        Self::iterate(item, |x| x.clone())
    }

    /// Infinite stream from a factory function called for each element.
    pub fn repeat_with(f: impl Fn() -> B + Send + Sync + 'static) -> Self {
        let f: Arc<dyn Fn() -> B + Send + Sync> = Arc::new(f);
        Self::from_factory(move || {
            let f = Arc::clone(&f);
            Box::new(PullRepeatWith {
                f: Box::new(move || f()),
                chunk_size: 256,
            })
        })
    }

    /// Create a stream from a factory that produces a [`PullOperator`].
    ///
    /// The factory is called each time the pipe is materialized (i.e.,
    /// when a terminal like `.collect()` is invoked), producing an
    /// independent operator chain. This makes the pipe cloneable.
    pub fn from_pull(
        factory: impl Fn() -> Box<dyn PullOperator<B>> + Send + Sync + 'static,
    ) -> Self {
        Self::from_factory(factory)
    }

    /// Create a stream from a single [`PullOperator`] instance.
    ///
    /// The resulting pipe can be cloned, but only one clone may be
    /// materialized (executed). A second materialization returns
    /// `PipeError::Closed` on first pull.
    pub fn from_pull_once(op: impl PullOperator<B> + 'static) -> Self {
        let slot: Arc<std::sync::Mutex<Option<Box<dyn PullOperator<B>>>>> =
            Arc::new(std::sync::Mutex::new(Some(Box::new(op))));
        Self::from_factory(move || match slot.lock().unwrap().take() {
            Some(op) => op,
            None => Box::new(crate::pull::ErrorSource::new(PipeError::Custom(
                "from_pull_once: operator already consumed (Pipe was cloned and both materialized)"
                    .into(),
            ))),
        })
    }

    /// Create a pipe from an async generator function.
    ///
    /// The function receives a [`Emitter`] and yields elements by
    /// calling `emitter.emit(item)`. This avoids implementing
    /// [`PullOperator`] manually.
    ///
    /// ```ignore
    /// let pipe = Pipe::generate(|mut tx| async move {
    ///     tx.emit(1).await;
    ///     tx.emit(2).await;
    ///     tx.emit(3).await;
    ///     Ok(())
    /// });
    /// ```
    pub fn generate<Fut>(f: impl Fn(Emitter<B>) -> Fut + Send + Sync + 'static) -> Self
    where
        Fut: Future<Output = Result<(), PipeError>> + Send + 'static,
    {
        let f = Arc::new(f);
        Self::from_factory(move || {
            let (tx, rx) = tokio::sync::mpsc::channel::<Result<Vec<B>, PipeError>>(2);
            let f = Arc::clone(&f);
            let handle = tokio::spawn(async move {
                crate::panic::report_panic(&tx, async {
                    let emitter = Emitter { tx: tx.clone() };
                    if let Err(e) = f(emitter).await {
                        let _ = tx.send(Err(e)).await;
                    }
                })
                .await;
            });
            Box::new(crate::channel::ChunkResultReceiver::new(
                rx,
                handle.abort_handle(),
            ))
        })
    }

    /// Like [`generate`](Self::generate) but takes a `FnOnce` closure,
    /// allowing owned (non-cloneable) resources to be moved in.
    ///
    /// The resulting pipe can be cloned, but only one clone may be
    /// materialized. Panics if a second clone tries to execute.
    ///
    /// ```ignore
    /// let listener = TcpListener::bind("0.0.0.0:8080").await?;
    /// let pipe = Pipe::generate_once(|tx| async move {
    ///     loop {
    ///         let (stream, _) = listener.accept().await?;
    ///         tx.emit(stream).await?;
    ///     }
    /// });
    /// ```
    pub fn generate_once<Fut>(f: impl FnOnce(Emitter<B>) -> Fut + Send + 'static) -> Self
    where
        Fut: Future<Output = Result<(), PipeError>> + Send + 'static,
    {
        type BoxedGen<B> = Box<
            dyn FnOnce(
                    Emitter<B>,
                )
                    -> std::pin::Pin<Box<dyn Future<Output = Result<(), PipeError>> + Send>>
                + Send,
        >;
        let slot: Arc<std::sync::Mutex<Option<BoxedGen<B>>>> =
            Arc::new(std::sync::Mutex::new(Some(Box::new(move |emitter| {
                Box::pin(f(emitter))
            }))));
        Self::from_factory(move || {
            let f = match slot.lock().unwrap().take() {
                Some(f) => f,
                None => return Box::new(crate::pull::ErrorSource::new(
                    PipeError::Custom("generate_once: generator already consumed (Pipe was cloned and both materialized)".into()),
                )) as Box<dyn PullOperator<B>>,
            };
            let (tx, rx) = tokio::sync::mpsc::channel::<Result<Vec<B>, PipeError>>(2);
            let handle = tokio::spawn(async move {
                crate::panic::report_panic(&tx, async {
                    let emitter = Emitter { tx: tx.clone() };
                    if let Err(e) = f(emitter).await {
                        let _ = tx.send(Err(e)).await;
                    }
                })
                .await;
            });
            Box::new(crate::channel::ChunkResultReceiver::new(
                rx,
                handle.abort_handle(),
            ))
        })
    }
}

impl Pipe<tokio::time::Instant> {
    /// Infinite stream that emits the current instant at fixed intervals.
    ///
    /// The first element is emitted immediately.
    pub fn interval(period: std::time::Duration) -> Self {
        use super::pull_ops::PullInterval;
        Self::from_factory(move || {
            Box::new(PullInterval {
                interval: tokio::time::interval(period),
                chunk_size: 1,
            })
        })
    }
}
