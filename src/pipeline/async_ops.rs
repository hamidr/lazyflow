//! Async element operations (eval_map, eval_filter, eval_tap, par_eval_map).

use std::future::Future;
use std::sync::Arc;

use crate::pull::{PipeError, PullOperator};
use crate::task::OwnedTask;

use super::Pipe;
use super::pull_ops::{PullEvalFilter, PullEvalMap, PullEvalTap};

impl<B: Send + 'static> Pipe<B> {
    /// Async per-element transform (like `map` but with `Future`).
    pub fn eval_map<
        C: Send + 'static,
        Fut: Future<Output = Result<C, PipeError>> + Send + 'static,
    >(
        self,
        f: impl Fn(B) -> Fut + Send + Sync + 'static,
    ) -> Pipe<C> {
        let parent = self.factory;
        let f = Arc::new(f);
        Pipe::from_factory(move || {
            let child = parent();
            let f = Arc::clone(&f);
            Box::new(PullEvalMap {
                child,
                f: move |b| f(b),
            })
        })
    }

    /// Async per-element filter.
    pub fn eval_filter<Fut: Future<Output = Result<bool, PipeError>> + Send + 'static>(
        self,
        f: impl Fn(&B) -> Fut + Send + Sync + 'static,
    ) -> Self {
        let parent = self.factory;
        let f = Arc::new(f);
        Self::from_factory(move || {
            let child = parent();
            let f = Arc::clone(&f);
            Box::new(PullEvalFilter {
                child,
                f: move |b: &B| f(b),
            })
        })
    }

    /// Async side-effect on each element, pass through unchanged.
    ///
    /// Requires `B: Sync` because elements are borrowed across await points.
    pub fn eval_tap<Fut: Future<Output = Result<(), PipeError>> + Send + 'static>(
        self,
        f: impl Fn(&B) -> Fut + Send + Sync + 'static,
    ) -> Self
    where
        B: Sync,
    {
        let parent = self.factory;
        let f = Arc::new(f);
        Self::from_factory(move || {
            let child = parent();
            let f = Arc::clone(&f);
            Box::new(PullEvalTap {
                child,
                f: move |b: &B| f(b),
            })
        })
    }

    /// Bounded-parallel async transform with ordered output.
    ///
    /// Processes up to `concurrency` elements concurrently. Results are
    /// emitted in the same order as the input. Uses a background task
    /// pool connected by channels.
    pub fn par_eval_map<
        C: Send + 'static,
        Fut: Future<Output = Result<C, PipeError>> + Send + 'static,
    >(
        self,
        concurrency: usize,
        f: impl Fn(B) -> Fut + Send + Sync + 'static,
    ) -> Pipe<C> {
        let parent = self.factory;
        let f = Arc::new(f);
        let concurrency = concurrency.max(1);
        Pipe::from_factory(move || {
            let (out_tx, out_rx) = tokio::sync::mpsc::channel::<Result<C, PipeError>>(concurrency);
            let mut root = parent();
            let f = Arc::clone(&f);

            let handle = tokio::spawn(async move {
                let report_tx = out_tx.clone();
                crate::panic::report_panic(&report_tx, async move {
                    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
                    let mut pending: std::collections::VecDeque<OwnedTask<Result<C, PipeError>>> =
                        std::collections::VecDeque::new();
                    let mut items: std::collections::VecDeque<B> =
                        std::collections::VecDeque::new();
                    let mut source_done = false;

                    loop {
                        if items.is_empty() && !source_done {
                            match root.next_chunk().await {
                                Ok(Some(chunk)) => items.extend(chunk),
                                Ok(None) => source_done = true,
                                Err(e) => {
                                    let _ = out_tx.send(Err(e)).await;
                                    return;
                                }
                            }
                        }

                        while let Some(item) = items.pop_front() {
                            let permit = match semaphore.clone().acquire_owned().await {
                                Ok(p) => p,
                                Err(_) => return,
                            };
                            let f = Arc::clone(&f);
                            pending.push_back(OwnedTask::spawn(async move {
                                let result = f(item).await;
                                drop(permit);
                                result
                            }));
                        }

                        if pending.is_empty() {
                            return;
                        }

                        let front = pending.pop_front().unwrap();
                        match front.await {
                            Ok(result) => {
                                if out_tx.send(result).await.is_err() {
                                    return;
                                }
                            }
                            Err(e) => {
                                let _ = out_tx.send(Err(crate::panic::join_error(e))).await;
                                return;
                            }
                        }
                    }
                })
                .await;
            });

            Box::new(crate::channel::TaskResultReceiver::new(
                out_rx,
                handle.abort_handle(),
            ))
        })
    }

    /// Bounded-parallel async transform with unordered output.
    ///
    /// Like [`par_eval_map`](Self::par_eval_map) but results are
    /// emitted as soon as they complete, not in input order.
    /// Higher throughput when processing times vary.
    pub fn par_eval_map_unordered<
        C: Send + 'static,
        Fut: Future<Output = Result<C, PipeError>> + Send + 'static,
    >(
        self,
        concurrency: usize,
        f: impl Fn(B) -> Fut + Send + Sync + 'static,
    ) -> Pipe<C> {
        let parent = self.factory;
        let f = Arc::new(f);
        let concurrency = concurrency.max(1);
        Pipe::from_factory(move || {
            let (out_tx, out_rx) = tokio::sync::mpsc::channel::<Result<C, PipeError>>(concurrency);
            let mut root = parent();
            let f = Arc::clone(&f);

            let handle = tokio::spawn(async move {
                let report_tx = out_tx.clone();
                crate::panic::report_panic(&report_tx, async move {
                    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
                    let mut workers = tokio::task::JoinSet::new();

                    loop {
                        match root.next_chunk().await {
                            Ok(Some(chunk)) => {
                                for item in chunk {
                                    let f = Arc::clone(&f);
                                    let sem = Arc::clone(&semaphore);
                                    let tx = out_tx.clone();
                                    let permit = match sem.acquire_owned().await {
                                        Ok(p) => p,
                                        Err(_) => {
                                            workers.abort_all();
                                            return;
                                        }
                                    };
                                    workers.spawn(async move {
                                        crate::panic::report_panic(&tx, async {
                                            let result = f(item).await;
                                            let _ = tx.send(result).await;
                                        })
                                        .await;
                                        drop(permit);
                                    });
                                }
                            }
                            Ok(None) => {
                                // Source exhausted -- wait for remaining workers
                                drop(out_tx);
                                while workers.join_next().await.is_some() {}
                                return;
                            }
                            Err(e) => {
                                let _ = out_tx.send(Err(e)).await;
                                workers.abort_all();
                                return;
                            }
                        }
                    }
                })
                .await;
            });

            Box::new(crate::channel::TaskResultReceiver::new(
                out_rx,
                handle.abort_handle(),
            ))
        })
    }
}
