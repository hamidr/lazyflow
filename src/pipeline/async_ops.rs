//! Async element operations (eval_map, eval_filter, eval_tap, par_eval_map).

use std::collections::VecDeque;
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
    /// emitted in the same order as the input. At most `concurrency`
    /// results are in flight, running or finished but not yet emitted, so
    /// a slow item holds back later work instead of piling up results.
    /// Uses a background task pool connected by channels.
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
            let root = parent();
            let f = Arc::clone(&f);

            let handle = tokio::spawn(async move {
                let report_tx = out_tx.clone();
                crate::panic::report_panic(
                    &report_tx,
                    ordered_window(root, f, concurrency, out_tx),
                )
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

/// Runs `f` over `root` with at most `concurrency` results in flight and
/// sends them to `out_tx` in input order.
///
/// A finished result is emitted while the next upstream pull is still
/// pending, and that pull is resumed afterwards rather than restarted:
/// it may already own consumed data. An upstream failure is reported
/// after the results of the items that preceded it.
async fn ordered_window<B, C, F, Fut>(
    mut root: Box<dyn PullOperator<B>>,
    f: Arc<F>,
    concurrency: usize,
    out_tx: tokio::sync::mpsc::Sender<Result<C, PipeError>>,
) where
    B: Send + 'static,
    C: Send + 'static,
    F: Fn(B) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<C, PipeError>> + Send + 'static,
{
    let mut window: VecDeque<OwnedTask<Result<C, PipeError>>> = VecDeque::new();
    let mut items: VecDeque<B> = VecDeque::new();
    let mut upstream_open = true;
    let mut upstream_error = None;

    loop {
        while window.len() < concurrency {
            let Some(item) = items.pop_front() else { break };
            let f = Arc::clone(&f);
            window.push_back(OwnedTask::spawn(async move { f(item).await }));
        }

        if upstream_open && items.is_empty() && window.len() < concurrency {
            let mut pull = root.next_chunk();
            let pulled = loop {
                let Some(front) = window.front_mut() else {
                    break (&mut pull).await;
                };
                tokio::select! {
                    biased;
                    done = front => {
                        window.pop_front();
                        if !emit(&out_tx, done).await {
                            return;
                        }
                    }
                    pulled = &mut pull => break pulled,
                }
            };
            match pulled {
                Ok(Some(chunk)) => items.extend(chunk),
                Ok(None) => upstream_open = false,
                Err(e) => {
                    upstream_open = false;
                    upstream_error = Some(e);
                }
            }
            continue;
        }

        match window.pop_front() {
            Some(task) => {
                if !emit(&out_tx, task.await).await {
                    return;
                }
            }
            None => {
                if let Some(e) = upstream_error {
                    let _ = out_tx.send(Err(e)).await;
                }
                return;
            }
        }
    }
}

/// Sends one worker's outcome downstream. Returns whether to keep going:
/// a failed worker or a departed consumer ends the map.
async fn emit<C>(
    out_tx: &tokio::sync::mpsc::Sender<Result<C, PipeError>>,
    done: Result<Result<C, PipeError>, tokio::task::JoinError>,
) -> bool {
    let result = done.unwrap_or_else(|e| Err(crate::panic::join_error(e)));
    let keep_going = result.is_ok();
    out_tx.send(result).await.is_ok() && keep_going
}
