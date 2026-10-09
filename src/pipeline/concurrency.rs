//! Concurrency operators: prefetch, merge, broadcast, zip, partition, par_join.

use std::sync::Arc;

use crate::pull::{PipeError, PullOperator, PullZip};

use super::Pipe;

type BroadcastTransform<B> = Box<dyn FnOnce(Pipe<B>) -> Pipe<B> + Send>;

use super::pull_ops::{
    BroadcastReceiver, GuardedPull, LazyFanOut, LazyPartition, PartitionReceiver,
};

/// Drain a PullOperator into a chunk-level channel, cancelling on consumer drop.
pub(crate) async fn drain_to_channel<B: Send + 'static>(
    root: &mut dyn PullOperator<B>,
    tx: &tokio::sync::mpsc::Sender<Result<Vec<B>, PipeError>>,
) {
    loop {
        let result = tokio::select! {
            r = root.next_chunk() => r,
            _ = tx.closed() => break,
        };
        match result {
            Ok(Some(chunk)) => {
                if tx.send(Ok(chunk)).await.is_err() {
                    break;
                }
            }
            Ok(None) => break,
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                break;
            }
        }
    }
}

impl<B: Send + 'static> Pipe<B> {
    /// Buffer up to `n` chunks ahead of the consumer.
    ///
    /// Spawns the upstream on a background task at materialization time.
    /// Backpressure: the task awaits when the buffer is full.
    /// Task is cancelled when Pipe is dropped.
    pub fn prefetch(self, n: usize) -> Self {
        let parent = self.factory;
        Self::from_factory(move || {
            let (tx, rx) = tokio::sync::mpsc::channel::<Result<Vec<B>, PipeError>>(n.max(1));
            let mut root = parent();
            let handle = tokio::spawn(async move {
                crate::panic::report_panic(&tx, drain_to_channel(&mut *root, &tx)).await;
            });
            Box::new(crate::channel::ChunkResultReceiver::new(
                rx,
                handle.abort_handle(),
            ))
        })
    }

    /// Merge multiple pipes into one, interleaving in arrival order.
    ///
    /// Each source runs concurrently. Backpressure via bounded buffers.
    /// Background tasks are cancelled when the merged Pipe is dropped.
    pub fn merge(pipes: Vec<Pipe<B>>) -> Self {
        if pipes.is_empty() {
            return Pipe::empty();
        }
        if pipes.len() == 1 {
            return pipes.into_iter().next().unwrap();
        }

        let factories: Vec<_> = pipes.into_iter().map(|p| p.factory).collect();
        Self::from_factory(move || {
            let (merged_tx, merged_rx) =
                tokio::sync::mpsc::channel::<Result<Vec<B>, PipeError>>(factories.len().max(1));
            let mut handles = Vec::with_capacity(factories.len());

            for factory in &factories {
                let tx = merged_tx.clone();
                let mut root = factory();
                let h = tokio::spawn(async move {
                    crate::panic::report_panic(&tx, drain_to_channel(&mut *root, &tx)).await;
                });
                handles.push(h.abort_handle());
            }
            drop(merged_tx);

            Box::new(GuardedPull {
                inner: Box::new(crate::channel::ChunkResultReceiver::new(
                    merged_rx,
                    handles.remove(0),
                )),
                _guards: handles,
            })
        })
    }

    /// Merge this pipe with another, interleaving in arrival order.
    ///
    /// Shorthand for `Pipe::merge(vec![self, other])`.
    pub fn merge_with(self, other: Pipe<B>) -> Self {
        Pipe::merge(vec![self, other])
    }

    /// Fan-out: clone each element to `n` branches.
    ///
    /// Each branch has a bounded buffer. The source blocks if ANY branch
    /// is full. Background task cancelled when ALL branches are dropped.
    ///
    /// Materialization is lazy -- the source is not started until the
    /// first branch is consumed.
    pub fn broadcast(self, n: usize, buffer_size: usize) -> Vec<Pipe<B>>
    where
        B: Clone + Sync,
    {
        if n == 0 {
            return vec![];
        }
        if n == 1 {
            return vec![self];
        }

        // Shared state: initialized once when any branch first materializes.
        // Each branch gets a pre-assigned index and takes its receiver.
        let shared = Arc::new(LazyFanOut::new(n, buffer_size, self.factory));

        (0..n)
            .map(|idx| {
                let shared = Arc::clone(&shared);
                Pipe::from_factory(move || {
                    let (rx, _abort) = shared.take_broadcast_receiver(idx);
                    Box::new(BroadcastReceiver { rx, _abort })
                })
            })
            .collect()
    }

    /// Pair elements positionally with another pipe.
    ///
    /// Buffers leftover elements across chunk boundaries so no data is
    /// lost when left and right chunks have different sizes.
    pub fn zip<C: Send + 'static>(self, other: Pipe<C>) -> Pipe<(B, C)> {
        let left = self.factory;
        let right = other.factory;
        Pipe::from_factory(move || Box::new(PullZip::new(left(), right())))
    }

    /// Run `background` concurrently with `self`, taking output only
    /// from `self`. If the background pipe errors, the error propagates
    /// to the main pipe. The background is cancelled when `self`
    /// completes or is dropped.
    pub fn concurrently(self, background: Pipe<B>) -> Self {
        let main_factory = self.factory;
        let bg_factory = background.factory;
        Self::from_factory(move || {
            // Channel for background errors
            let (err_tx, err_rx) = tokio::sync::watch::channel::<Option<String>>(None);

            let mut bg_root = bg_factory();
            let bg_handle = tokio::spawn(async move {
                let run = crate::panic::catch_panic(async {
                    // Output is discarded; only the end or the error matters.
                    while bg_root.next_chunk().await?.is_some() {}
                    Ok(())
                });
                if let Err(e) = run.await.and_then(|r| r) {
                    let _ = err_tx.send(Some(e.to_string()));
                }
            });

            let main_root = main_factory();
            Box::new(super::pull_ops::PullConcurrently {
                inner: main_root,
                bg_error: err_rx,
                abort: Some(bg_handle.abort_handle()),
            })
        })
    }

    /// Zip with a custom combiner function instead of producing tuples.
    pub fn zip_with<C: Send + 'static, D: Send + 'static>(
        self,
        other: Pipe<C>,
        f: impl Fn(B, C) -> D + Send + Sync + 'static,
    ) -> Pipe<D> {
        self.zip(other).map(move |(a, b)| f(a, b))
    }

    /// Broadcast to N branches and apply a different transform to each.
    ///
    /// Each transform receives one branch of the broadcast. Results
    /// from all branches are merged (interleaved by arrival order).
    pub fn broadcast_through(
        self,
        buffer_size: usize,
        transforms: Vec<BroadcastTransform<B>>,
    ) -> Self
    where
        B: Clone + Sync,
    {
        let n = transforms.len();
        let branches = self.broadcast(n, buffer_size);
        let transformed: Vec<Pipe<B>> = branches
            .into_iter()
            .zip(transforms)
            .map(|(branch, f)| f(branch))
            .collect();
        Pipe::merge(transformed)
    }

    /// Hash-partition elements across N branches.
    ///
    /// Each element goes to exactly one branch: `key(&element) % n`.
    /// No cloning -- elements are moved. Each branch has a bounded buffer
    /// of `buffer_size` chunks. Backpressure: source blocks if any branch
    /// is full. Background task cancelled when all branches are dropped.
    ///
    /// Materialization is lazy -- the source is not started until the
    /// first branch is consumed.
    ///
    /// ```ignore
    /// let partitions = pipe.partition(4, 2, |x| *x as u64);
    /// // partitions[0] gets elements where key % 4 == 0
    /// // partitions[1] gets elements where key % 4 == 1
    /// // etc.
    /// ```
    pub fn partition(
        self,
        n: usize,
        buffer_size: usize,
        key: impl Fn(&B) -> u64 + Send + Sync + 'static,
    ) -> Vec<Pipe<B>> {
        if n == 0 {
            return vec![];
        }
        if n == 1 {
            return vec![self];
        }

        let shared = Arc::new(LazyPartition::new(n, buffer_size, self.factory, key));

        (0..n)
            .map(|idx| {
                let shared = Arc::clone(&shared);
                Pipe::from_factory(move || {
                    let (rx, _abort) = shared.take_receiver(idx);
                    Box::new(PartitionReceiver { inner: rx, _abort })
                })
            })
            .collect()
    }
}

impl<B: Send + 'static> Pipe<Pipe<B>> {
    /// Concurrently drain up to `max_open` inner pipes, merging outputs.
    ///
    /// The outer pipe produces inner pipes dynamically (e.g., one per TCP
    /// connection). Each inner pipe runs on its own task. At most `max_open`
    /// inner pipes run concurrently -- backpressure stalls the outer pipe
    /// until a slot opens.
    ///
    /// This is the concurrent counterpart of [`flatten`](Pipe::flatten)
    /// and mirrors FS2's `parJoin(maxOpen)`.
    ///
    /// ```ignore
    /// // Accept TCP connections, handle each as its own pipe
    /// accept_pipe                      // Pipe<Pipe<String>>
    ///     .par_join(100)               // Pipe<String>  -- up to 100 concurrent
    ///     .for_each(|msg| println!("{msg}"))
    ///     .await?;
    /// ```
    pub fn par_join(self, max_open: usize) -> Pipe<B> {
        let parent = self.factory;
        let max_open = max_open.max(1);
        Pipe::from_factory(move || {
            let (out_tx, out_rx) =
                tokio::sync::mpsc::channel::<Result<Vec<B>, PipeError>>(max_open);
            let mut outer = parent();

            let handle = tokio::spawn(async move {
                let report_tx = out_tx.clone();
                crate::panic::report_panic(&report_tx, async move {
                    let semaphore = Arc::new(tokio::sync::Semaphore::new(max_open));

                    loop {
                        let pipes = tokio::select! {
                            result = outer.next_chunk() => result,
                            _ = out_tx.closed() => break,
                        };
                        match pipes {
                            Ok(Some(pipes)) => {
                                for pipe in pipes {
                                    let permit = match semaphore.clone().acquire_owned().await {
                                        Ok(p) => p,
                                        Err(_) => return,
                                    };
                                    let tx = out_tx.clone();
                                    tokio::spawn(async move {
                                        crate::panic::report_panic(&tx, async {
                                            let mut pull = pipe.into_pull();
                                            drain_to_channel(&mut *pull, &tx).await;
                                        })
                                        .await;
                                        drop(permit);
                                    });
                                }
                            }
                            Ok(None) => break,
                            Err(e) => {
                                let _ = out_tx.send(Err(e)).await;
                                break;
                            }
                        }
                    }
                })
                .await;
            });

            Box::new(crate::channel::ChunkResultReceiver::new(
                out_rx,
                handle.abort_handle(),
            ))
        })
    }

    /// Concurrently drain all inner pipes without a concurrency limit.
    ///
    /// Like [`par_join`](Self::par_join) but spawns every inner pipe
    /// immediately. Use when the number of inner pipes is bounded by the
    /// problem (e.g., TCP connections behind a load balancer) rather than
    /// needing explicit throttling.
    ///
    /// Mirrors FS2's `parJoinUnbounded`.
    pub fn par_join_unbounded(self) -> Pipe<B> {
        let parent = self.factory;
        Pipe::from_factory(move || {
            let (out_tx, out_rx) = tokio::sync::mpsc::channel::<Result<Vec<B>, PipeError>>(16);
            let mut outer = parent();

            let handle = tokio::spawn(async move {
                let report_tx = out_tx.clone();
                crate::panic::report_panic(&report_tx, async move {
                    loop {
                        let pipes = tokio::select! {
                            result = outer.next_chunk() => result,
                            _ = out_tx.closed() => break,
                        };
                        match pipes {
                            Ok(Some(pipes)) => {
                                for pipe in pipes {
                                    let tx = out_tx.clone();
                                    tokio::spawn(async move {
                                        crate::panic::report_panic(&tx, async {
                                            let mut inner = pipe.into_pull();
                                            drain_to_channel(&mut *inner, &tx).await;
                                        })
                                        .await;
                                    });
                                }
                            }
                            Ok(None) => break,
                            Err(e) => {
                                let _ = out_tx.send(Err(e)).await;
                                break;
                            }
                        }
                    }
                })
                .await;
            });

            Box::new(crate::channel::ChunkResultReceiver::new(
                out_rx,
                handle.abort_handle(),
            ))
        })
    }
}
