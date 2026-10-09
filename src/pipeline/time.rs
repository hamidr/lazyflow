//! Time-based operators: timeout, throttle, debounce, chunks_timeout.

use super::Pipe;
use super::pull_ops::{PullThrottle, PullTimeout};

impl<B: Send + 'static> Pipe<B> {
    /// Fail if a single pull takes longer than `duration`.
    pub fn timeout(self, duration: std::time::Duration) -> Self {
        let parent = self.factory;
        Self::from_factory(move || {
            Box::new(PullTimeout {
                child: parent(),
                duration,
            })
        })
    }

    /// Delay each element by `duration` before emitting.
    pub fn delay_by(self, duration: std::time::Duration) -> Self {
        self.eval_map(move |x| async move {
            tokio::time::sleep(duration).await;
            Ok(x)
        })
    }

    /// Emit at most one element per `duration`.
    ///
    /// Elements arriving faster than the rate are delayed, not dropped.
    /// Runs inline (no background task) -- sleeps during pull if the
    /// rate limit has not elapsed.
    pub fn throttle(self, duration: std::time::Duration) -> Self {
        let parent = self.factory;
        Self::from_factory(move || {
            Box::new(PullThrottle {
                child: parent(),
                duration,
                last: None,
            })
        })
    }

    /// Emit an element only after `duration` of silence.
    ///
    /// If new elements keep arriving within `duration`, only the
    /// latest is emitted once the quiet period elapses. Spawns a
    /// background task at materialization.
    pub fn debounce(self, duration: std::time::Duration) -> Self {
        let parent = self.factory;
        Self::from_factory(move || {
            let (tx, rx) = tokio::sync::mpsc::channel::<Result<B, crate::pull::PipeError>>(1);
            let mut root = parent();

            let handle = tokio::spawn(async move {
                let report_tx = tx.clone();
                crate::panic::report_panic(&report_tx, async move {
                    let mut latest: Option<B> = None;

                    loop {
                        if latest.is_some() {
                            tokio::select! {
                                biased;
                                result = root.next_chunk() => {
                                    match result {
                                        Ok(Some(chunk)) => {
                                            latest = chunk.into_iter().last();
                                        }
                                        Ok(None) => {
                                            if let Some(item) = latest.take() {
                                                let _ = tx.send(Ok(item)).await;
                                            }
                                            return;
                                        }
                                        Err(e) => {
                                            if let Some(item) = latest.take() {
                                                let _ = tx.send(Ok(item)).await;
                                            }
                                            let _ = tx.send(Err(e)).await;
                                            return;
                                        }
                                    }
                                }
                                _ = tokio::time::sleep(duration) => {
                                    if let Some(item) = latest.take() {
                                        if tx.send(Ok(item)).await.is_err() { return; }
                                    }
                                }
                            }
                        } else {
                            match root.next_chunk().await {
                                Ok(Some(chunk)) => {
                                    latest = chunk.into_iter().last();
                                }
                                Ok(None) => return,
                                Err(e) => {
                                    let _ = tx.send(Err(e)).await;
                                    return;
                                }
                            }
                        }
                    }
                })
                .await;
            });

            Box::new(crate::channel::TaskResultReceiver::new(
                rx,
                handle.abort_handle(),
            ))
        })
    }

    /// Batch elements by count OR time, whichever triggers first.
    ///
    /// Collects up to `max_size` elements into a `Vec<B>`. If the
    /// deadline elapses before the batch is full, flushes whatever
    /// has accumulated. Spawns a background task at materialization.
    pub fn chunks_timeout(self, max_size: usize, timeout: std::time::Duration) -> Pipe<Vec<B>> {
        let parent = self.factory;
        Pipe::from_factory(move || {
            let (tx, rx) = tokio::sync::mpsc::channel::<Result<Vec<B>, crate::pull::PipeError>>(2);
            let mut root = parent();
            let handle = tokio::spawn(async move {
                let report_tx = tx.clone();
                crate::panic::report_panic(&report_tx, async move {
                    let mut batch = Vec::with_capacity(max_size);
                    loop {
                        let deadline = tokio::time::sleep(timeout);
                        tokio::pin!(deadline);

                        // Pull chunks until batch is full or deadline fires
                        loop {
                            // Flush any complete batches already buffered
                            while batch.len() >= max_size {
                                let ready = batch.drain(..max_size).collect();
                                if tx.send(Ok(ready)).await.is_err() {
                                    return;
                                }
                            }

                            tokio::select! {
                                biased;
                                result = root.next_chunk() => {
                                    match result {
                                        Ok(Some(chunk)) => {
                                            batch.extend(chunk);
                                            // Loop back to flush any full batches
                                        }
                                        Ok(None) => {
                                            // Source exhausted -- flush remainder
                                            if !batch.is_empty() {
                                                let _ = tx.send(Ok(std::mem::take(&mut batch))).await;
                                            }
                                            return;
                                        }
                                        Err(e) => {
                                            if !batch.is_empty() {
                                                let _ = tx.send(Ok(std::mem::take(&mut batch))).await;
                                            }
                                            let _ = tx.send(Err(e)).await;
                                            return;
                                        }
                                    }
                                }
                                _ = &mut deadline => {
                                    // Timer fired -- flush partial batch
                                    if !batch.is_empty() {
                                        if tx.send(Ok(std::mem::take(&mut batch))).await.is_err() { return; }
                                        batch = Vec::with_capacity(max_size);
                                    }
                                    break; // Reset timer
                                }
                            }
                        }
                    }
                })
                .await;
            });

            Box::new(crate::channel::TaskResultReceiver::new(
                rx,
                handle.abort_handle(),
            ))
        })
    }
}
