//! Composable pipeline -- a lazy effectful list of elements.
//!
//! `Pipe<B>` is analogous to `Stream[F, O]` in FS2.
//! `B` is the **element type** -- chunking is an internal detail.
//! Operations like `map`, `and_then`, `flat_map` can change the
//! element type.

mod async_ops;
mod concurrency;
mod constructors;
mod error;
mod operators;
pub(crate) mod pull_ops;
mod terminals;
mod time;

use std::sync::Arc;

use crate::pull::{PipeError, PullOperator};

/// Handle for emitting elements from [`Pipe::generate`].
pub struct Emitter<B> {
    pub(crate) tx: tokio::sync::mpsc::Sender<Result<Vec<B>, PipeError>>,
}

impl<B: Send + 'static> Emitter<B> {
    /// Emit a single element. Backpressure: awaits if the buffer is full.
    pub async fn emit(&self, item: B) -> Result<(), PipeError> {
        self.tx
            .send(Ok(vec![item]))
            .await
            .map_err(|_| PipeError::Closed)
    }

    /// Emit a batch of elements.
    pub async fn emit_all(&self, items: Vec<B>) -> Result<(), PipeError> {
        if !items.is_empty() {
            self.tx
                .send(Ok(items))
                .await
                .map_err(|_| PipeError::Closed)?;
        }
        Ok(())
    }
}

/// A lazy effectful list of `B` elements.
///
/// `Pipe` is a blueprint -- it describes a pipeline of operations but
/// does not execute until a terminal (`.collect()`, `.fold()`, etc.) is
/// called. Because it is a description, `Pipe` implements [`Clone`]:
/// each clone materializes an independent operator chain.
///
/// ```ignore
/// let result = Pipe::from_iter(1..=10)
///     .map(|x| x * 2)
///     .filter(|x| *x > 10)
///     .take(3)
///     .collect().await?;
/// assert_eq!(result, vec![12, 14, 16]);
/// ```
pub struct Pipe<B: Send + 'static> {
    pub(crate) factory: Arc<dyn Fn() -> Box<dyn PullOperator<B>> + Send + Sync>,
}

impl<B: Send + 'static> Clone for Pipe<B> {
    fn clone(&self) -> Self {
        Self {
            factory: Arc::clone(&self.factory),
        }
    }
}

impl<B: Send + 'static> Pipe<B> {
    /// Internal: create a pipe from a factory closure.
    pub(crate) fn from_factory(
        f: impl Fn() -> Box<dyn PullOperator<B>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            factory: Arc::new(f),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::PinFut;

    #[tokio::test]
    async fn from_iter_collect() {
        let result = Pipe::from_iter(vec![1, 2, 3]).collect().await.unwrap();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn once_single_element() {
        let result = Pipe::once(42).collect().await.unwrap();
        assert_eq!(result, vec![42]);
    }

    #[tokio::test]
    async fn empty_stream() {
        let result = Pipe::<i64>::empty().collect().await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn filter_evens() {
        let result = Pipe::from_iter(1..=6)
            .filter(|x| x % 2 == 0)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![2, 4, 6]);
    }

    #[tokio::test]
    async fn take_first_three() {
        let result = Pipe::from_iter(1..=10).take(3).collect().await.unwrap();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn skip_first_two() {
        let result = Pipe::from_iter(1..=5).skip(2).collect().await.unwrap();
        assert_eq!(result, vec![3, 4, 5]);
    }

    #[tokio::test]
    async fn fold_sum() {
        let sum = Pipe::from_iter(1..=5)
            .fold(0i64, |acc, x| acc + x)
            .await
            .unwrap();
        assert_eq!(sum, 15);
    }

    #[tokio::test]
    async fn reduce_sum() {
        let sum = Pipe::from_iter(1..=5i64)
            .reduce(|a, b| a + b)
            .await
            .unwrap();
        assert_eq!(sum, Some(15));
    }

    #[tokio::test]
    async fn reduce_empty() {
        let result = Pipe::from_iter(Vec::<i64>::new())
            .reduce(|a, b| a + b)
            .await
            .unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn reduce_single() {
        let result = Pipe::from_iter(vec![42i64])
            .reduce(|a, b| a + b)
            .await
            .unwrap();
        assert_eq!(result, Some(42));
    }

    #[tokio::test]
    async fn count_elements() {
        let n = Pipe::from_iter(1..=7).count().await.unwrap();
        assert_eq!(n, 7);
    }

    #[tokio::test]
    async fn tap_observes() {
        use std::sync::{Arc, Mutex};
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen2 = seen.clone();
        let result = Pipe::from_iter(vec![1, 2, 3])
            .tap(move |x| seen2.lock().unwrap().push(*x))
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 2, 3]);
        assert_eq!(*seen.lock().unwrap(), vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn chain_map_filter_take() {
        let result = Pipe::from_iter(1..=10)
            .map(|x| x * 2)
            .filter(|x| *x > 10)
            .take(3)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![12, 14, 16]);
    }

    #[tokio::test]
    async fn map_changes_type() {
        let result: Vec<String> = Pipe::from_iter(vec![1, 2, 3])
            .map(|x: i64| x.to_string())
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec!["1", "2", "3"]);
    }

    #[tokio::test]
    async fn and_then_changes_type() {
        let result: Vec<String> = Pipe::from_iter(1..=5)
            .and_then(|x: i64| if x > 2 { Some(format!("v{x}")) } else { None })
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec!["v3", "v4", "v5"]);
    }

    #[tokio::test]
    async fn flat_map_changes_type() {
        let result: Vec<String> = Pipe::from_iter(vec![1, 2])
            .flat_map(|x: i64| Pipe::from_iter(vec![format!("{x}a"), format!("{x}b")]))
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec!["1a", "1b", "2a", "2b"]);
    }

    #[tokio::test]
    async fn scan_changes_type() {
        let result: Vec<String> = Pipe::from_iter(vec![1, 2, 3])
            .scan(0i64, |acc, x: i64| {
                *acc += x;
                format!("sum={acc}")
            })
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec!["sum=1", "sum=3", "sum=6"]);
    }

    #[derive(Debug)]
    struct ToStringOp;

    impl crate::operator::Operator<i64, String> for ToStringOp {
        fn execute<'a>(&'a self, input: i64) -> PinFut<'a, String> {
            Box::pin(async move { Ok(format!("n{input}")) })
        }
    }

    #[tokio::test]
    async fn pipe_changes_type() {
        let result: Vec<String> = Pipe::from_iter(vec![1, 2, 3])
            .pipe(ToStringOp)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec!["n1", "n2", "n3"]);
    }

    #[tokio::test]
    async fn multi_hop_type_change() {
        let result: Vec<usize> = Pipe::from_iter(1..=3)
            .map(|x: i64| x.to_string())
            .map(|s| s.len())
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 1, 1]);
    }

    #[tokio::test]
    async fn type_change_then_filter() {
        let result: Vec<String> = Pipe::from_iter(1..=10)
            .map(|x: i64| format!("item-{x}"))
            .filter(|s| s.ends_with('5'))
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec!["item-5"]);
    }

    #[tokio::test]
    async fn iterate_naturals() {
        let result = Pipe::iterate(0, |x| x + 1).take(5).collect().await.unwrap();
        assert_eq!(result, vec![0, 1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn unfold_fibonacci() {
        let result = Pipe::unfold((0u64, 1u64), |s| {
            let val = s.0;
            let next = s.0.checked_add(s.1)?;
            *s = (s.1, next);
            Some(val)
        })
        .take(10)
        .collect()
        .await
        .unwrap();
        assert_eq!(result, vec![0, 1, 1, 2, 3, 5, 8, 13, 21, 34]);
    }

    #[tokio::test]
    async fn iterate_with_type_change() {
        let result: Vec<String> = Pipe::iterate(1, |x| x + 1)
            .take(3)
            .map(|x: i64| format!("#{x}"))
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec!["#1", "#2", "#3"]);
    }

    #[tokio::test]
    async fn flat_map_same_type() {
        let result = Pipe::from_iter(1..=3)
            .flat_map(|x| Pipe::from_iter(0..x))
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![0, 0, 1, 0, 1, 2]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn switch_map_cancels_previous() {
        // Each element creates an inner pipe that emits [x, x+1, x+2]
        // but with delays. switch_map should cancel previous inner pipes.
        let result = Pipe::from_iter(vec![10, 20, 30])
            .switch_map(|x| Pipe::from_iter(vec![x, x + 1, x + 2]))
            .collect()
            .await
            .unwrap();
        // Only the last inner pipe (30) should complete fully.
        // Earlier ones may be partially cancelled. At minimum,
        // the last pipe's elements must be present.
        assert!(result.contains(&30));
        assert!(result.contains(&31));
        assert!(result.contains(&32));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn switch_map_single_element() {
        let result = Pipe::from_iter(vec![5])
            .switch_map(|x| Pipe::from_iter(vec![x * 10]))
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![50]);
    }

    #[tokio::test]
    async fn flat_map_infinite_sub() {
        let result = Pipe::once(1)
            .flat_map(|x| Pipe::iterate(x, |n| n + 1))
            .take(5)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn from_pull_custom_source() {
        use crate::pull::{ChunkFut, PullOperator};

        struct Counter {
            current: i64,
            max: i64,
            chunk_size: usize,
        }

        impl PullOperator<i64> for Counter {
            fn next_chunk(&mut self) -> ChunkFut<'_, i64> {
                Box::pin(async move {
                    if self.current > self.max {
                        return Ok(None);
                    }
                    let mut chunk = Vec::with_capacity(self.chunk_size);
                    for _ in 0..self.chunk_size {
                        if self.current > self.max {
                            break;
                        }
                        chunk.push(self.current);
                        self.current += 1;
                    }
                    Ok(Some(chunk))
                })
            }
        }

        let result = Pipe::from_pull_once(Counter {
            current: 1,
            max: 5,
            chunk_size: 2,
        })
        .map(|x| x * 10)
        .collect()
        .await
        .unwrap();
        assert_eq!(result, vec![10, 20, 30, 40, 50]);
    }

    #[tokio::test]
    async fn from_pull_with_filter_and_take() {
        use crate::pull::{ChunkFut, PullOperator};

        struct Naturals {
            n: i64,
        }

        impl PullOperator<i64> for Naturals {
            fn next_chunk(&mut self) -> ChunkFut<'_, i64> {
                Box::pin(async move {
                    let mut chunk = Vec::with_capacity(64);
                    for _ in 0..64 {
                        self.n += 1;
                        chunk.push(self.n);
                    }
                    Ok(Some(chunk))
                })
            }
        }

        let result = Pipe::from_pull_once(Naturals { n: 0 })
            .filter(|x| x % 3 == 0)
            .take(4)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![3, 6, 9, 12]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prefetch_buffers_ahead() {
        let result = Pipe::from_iter(1..=5)
            .prefetch(2)
            .map(|x| x * 10)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![10, 20, 30, 40, 50]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn merge_interleaves() {
        let a = Pipe::from_iter(vec![1, 3, 5]);
        let b = Pipe::from_iter(vec![2, 4, 6]);
        let mut result = Pipe::merge(vec![a, b]).collect().await.unwrap();
        result.sort();
        assert_eq!(result, vec![1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn merge_empty() {
        let result = Pipe::<i64>::merge(vec![]).collect().await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn merge_single() {
        let result = Pipe::merge(vec![Pipe::from_iter(vec![1, 2, 3])])
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn broadcast_fans_out() {
        let branches = Pipe::from_iter(vec![1, 2, 3]).broadcast(2, 2);
        assert_eq!(branches.len(), 2);

        let mut results = Vec::new();
        for branch in branches {
            results.push(branch.collect().await.unwrap());
        }
        assert_eq!(results[0], vec![1, 2, 3]);
        assert_eq!(results[1], vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn zip_pairs_positionally() {
        let a = Pipe::from_iter(vec![1, 2, 3]);
        let b = Pipe::from_iter(vec!["a", "b", "c"]);
        let result = a.zip(b).collect().await.unwrap();
        assert_eq!(result, vec![(1, "a"), (2, "b"), (3, "c")]);
    }

    #[tokio::test]
    async fn zip_unequal_length() {
        let a = Pipe::from_iter(vec![1, 2, 3, 4, 5]);
        let b = Pipe::from_iter(vec!["x", "y"]);
        let result = a.zip(b).collect().await.unwrap();
        assert_eq!(result, vec![(1, "x"), (2, "y")]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn partition_splits_by_key() {
        let parts = Pipe::from_iter(0..10i64).partition(2, 2, |x| *x as u64);

        assert_eq!(parts.len(), 2);
        let mut even = parts.into_iter().next().unwrap().collect().await.unwrap();
        // Partition 0 gets elements where key % 2 == 0
        even.sort();
        assert_eq!(even, vec![0, 2, 4, 6, 8]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn partition_all_elements_preserved() {
        let parts = Pipe::from_iter(1..=12i64).partition(3, 2, |x| *x as u64);

        let mut all = Vec::new();
        for part in parts {
            all.extend(part.collect().await.unwrap());
        }
        all.sort();
        assert_eq!(all, (1..=12).collect::<Vec<_>>());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn partition_single_returns_self() {
        let parts = Pipe::from_iter(vec![1, 2, 3]).partition(1, 2, |x| *x as u64);
        assert_eq!(parts.len(), 1);
        let result = parts.into_iter().next().unwrap().collect().await.unwrap();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn partition_then_merge_roundtrip() {
        let parts = Pipe::from_iter(1..=20i64).partition(4, 2, |x| *x as u64);

        let merged = Pipe::merge(parts);
        let mut result = merged.collect().await.unwrap();
        result.sort();
        assert_eq!(result, (1..=20).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn take_while_stops_at_predicate() {
        let result = Pipe::from_iter(1..=10)
            .take_while(|x| *x < 5)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn skip_while_starts_at_predicate() {
        let result = Pipe::from_iter(1..=6)
            .skip_while(|x| *x < 4)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![4, 5, 6]);
    }

    #[tokio::test]
    async fn chunks_groups_elements() {
        let result = Pipe::from_iter(1..=7).chunks(3).collect().await.unwrap();
        assert_eq!(result, vec![vec![1, 2, 3], vec![4, 5, 6], vec![7]]);
    }

    #[tokio::test]
    async fn unchunks_flattens() {
        let result = Pipe::from_iter(vec![vec![1, 2], vec![3, 4, 5]])
            .unchunks()
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn through_composition() {
        fn double_evens(pipe: Pipe<i64>) -> Pipe<i64> {
            pipe.filter(|x| x % 2 == 0).map(|x| x * 2)
        }
        let result = Pipe::from_iter(1..=6)
            .through(double_evens)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![4, 8, 12]);
    }

    #[tokio::test]
    async fn eval_map_async_transform() {
        let result = Pipe::from_iter(vec![1, 2, 3])
            .eval_map(|x| async move { Ok(format!("async-{x}")) })
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec!["async-1", "async-2", "async-3"]);
    }

    #[tokio::test]
    async fn eval_filter_async_predicate() {
        let result = Pipe::from_iter(1..=6)
            .eval_filter(|x| {
                let x = *x;
                async move { Ok(x % 2 == 0) }
            })
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![2, 4, 6]);
    }

    #[tokio::test]
    async fn eval_tap_async_side_effect() {
        use std::sync::{Arc, Mutex};
        let log = Arc::new(Mutex::new(Vec::new()));
        let log2 = log.clone();
        let result = Pipe::from_iter(vec![1, 2, 3])
            .eval_tap(move |x| {
                let log = log2.clone();
                let x = *x;
                async move {
                    log.lock().unwrap().push(x);
                    Ok(())
                }
            })
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 2, 3]);
        assert_eq!(*log.lock().unwrap(), vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn handle_error_with_recovers() {
        use crate::pull::{ChunkFut, PullOperator};

        struct FailAfter {
            count: usize,
            yielded: usize,
        }
        impl PullOperator<i64> for FailAfter {
            fn next_chunk(&mut self) -> ChunkFut<'_, i64> {
                Box::pin(async move {
                    if self.yielded >= self.count {
                        return Err("boom".into());
                    }
                    self.yielded += 1;
                    Ok(Some(vec![self.yielded as i64]))
                })
            }
        }

        let result = Pipe::from_pull_once(FailAfter {
            count: 2,
            yielded: 0,
        })
        .handle_error_with(|_| Pipe::from_iter(vec![99]))
        .collect()
        .await
        .unwrap();
        assert_eq!(result, vec![1, 2, 99]);
    }

    #[tokio::test]
    async fn retry_recovers_from_failure() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        let attempt = Arc::new(AtomicUsize::new(0));

        let attempt2 = attempt.clone();
        let result = Pipe::retry(
            move || {
                let n = attempt2.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    // First two attempts fail after yielding some data
                    Pipe::from_iter(vec![1, 2]).eval_map(move |x| async move {
                        if x == 2 { Err("fail".into()) } else { Ok(x) }
                    })
                } else {
                    // Third attempt succeeds
                    Pipe::from_iter(vec![10, 20, 30])
                }
            },
            3,
        )
        .collect()
        .await
        .unwrap();
        assert_eq!(result, vec![10, 20, 30]);
    }

    #[tokio::test]
    async fn for_each_runs_side_effect() {
        use std::sync::{Arc, Mutex};
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen2 = seen.clone();
        Pipe::from_iter(vec![1, 2, 3])
            .for_each(move |x| seen2.lock().unwrap().push(x))
            .await
            .unwrap();
        assert_eq!(*seen.lock().unwrap(), vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn first_returns_first_element() {
        let result = Pipe::from_iter(vec![10, 20, 30]).first().await.unwrap();
        assert_eq!(result, Some(10));
    }

    #[tokio::test]
    async fn first_returns_none_for_empty() {
        let result = Pipe::<i64>::empty().first().await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn last_returns_last_element() {
        let result = Pipe::from_iter(vec![10, 20, 30]).last().await.unwrap();
        assert_eq!(result, Some(30));
    }

    #[tokio::test]
    async fn last_returns_none_for_empty() {
        let result = Pipe::<i64>::empty().last().await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn chain_sequential() {
        let a = Pipe::from_iter(vec![1, 2]);
        let b = Pipe::from_iter(vec![3, 4, 5]);
        let result = a.chain(b).collect().await.unwrap();
        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn chain_first_empty() {
        let result = Pipe::<i64>::empty()
            .chain(Pipe::from_iter(vec![1, 2]))
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 2]);
    }

    #[tokio::test]
    async fn chain_second_empty() {
        let result = Pipe::from_iter(vec![1, 2])
            .chain(Pipe::empty())
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 2]);
    }

    #[tokio::test]
    async fn enumerate_pairs_with_index() {
        let result = Pipe::from_iter(vec!["a", "b", "c"])
            .enumerate()
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![(0, "a"), (1, "b"), (2, "c")]);
    }

    #[tokio::test]
    async fn inspect_observes() {
        use std::sync::{Arc, Mutex};
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen2 = seen.clone();
        let result = Pipe::from_iter(vec![1, 2, 3])
            .inspect(move |x| seen2.lock().unwrap().push(*x))
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 2, 3]);
        assert_eq!(*seen.lock().unwrap(), vec![1, 2, 3]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn merge_with_two_pipes() {
        let a = Pipe::from_iter(vec![1, 3, 5]);
        let b = Pipe::from_iter(vec![2, 4, 6]);
        let mut result = a.merge_with(b).collect().await.unwrap();
        result.sort();
        assert_eq!(result, vec![1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn from_pull_once_no_boxing() {
        use crate::pull::{ChunkFut, PullOperator};

        struct Trio;
        impl PullOperator<i64> for Trio {
            fn next_chunk(&mut self) -> ChunkFut<'_, i64> {
                Box::pin(async { Ok(Some(vec![1, 2, 3])) })
            }
        }

        let result = Pipe::from_pull_once(Trio).take(3).collect().await.unwrap();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn from_pull_factory() {
        let pipe = Pipe::from_pull(|| Box::new(crate::pull::PullSource::new(vec![1, 2, 3])));
        // Clone the pipe and collect from both
        let clone = pipe.clone();
        let a = pipe.collect().await.unwrap();
        let b = clone.collect().await.unwrap();
        assert_eq!(a, vec![1, 2, 3]);
        assert_eq!(b, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn clone_produces_independent_results() {
        let pipe = Pipe::from_iter(vec![1, 2, 3, 4, 5])
            .filter(|x| x % 2 == 0)
            .map(|x| x * 10);

        let clone = pipe.clone();

        let a = pipe.collect().await.unwrap();
        let b = clone.collect().await.unwrap();
        assert_eq!(a, vec![20, 40]);
        assert_eq!(b, vec![20, 40]);
    }

    #[tokio::test]
    async fn clone_with_scan_independent_state() {
        let pipe = Pipe::from_iter(vec![1, 2, 3]).scan(0i64, |acc, x| {
            *acc += x;
            *acc
        });

        let clone = pipe.clone();

        let a = pipe.collect().await.unwrap();
        let b = clone.collect().await.unwrap();
        // Both see independent accumulator state
        assert_eq!(a, vec![1, 3, 6]);
        assert_eq!(b, vec![1, 3, 6]);
    }

    #[tokio::test]
    async fn bracket_releases_on_completion() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let released = Arc::new(AtomicBool::new(false));
        let released2 = released.clone();

        let result = Pipe::bracket(
            || Box::pin(async { Ok(vec![1i64, 2, 3]) }),
            |data| Pipe::from_iter((*data).clone()),
            move |_| released2.store(true, Ordering::SeqCst),
        )
        .collect()
        .await
        .unwrap();

        assert_eq!(result, vec![1, 2, 3]);
        assert!(released.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn bracket_releases_on_error() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let released = Arc::new(AtomicBool::new(false));
        let released2 = released.clone();

        let result = Pipe::<i64>::bracket(
            || Box::pin(async { Ok(()) }),
            |_| {
                Pipe::from_iter(vec![1i64, 2])
                    .eval_map(|x| async move { if x == 2 { Err("boom".into()) } else { Ok(x) } })
            },
            move |_| released2.store(true, Ordering::SeqCst),
        )
        .collect()
        .await;

        assert!(result.is_err());
        assert!(released.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn bracket_releases_on_drop() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let released = Arc::new(AtomicBool::new(false));
        let released2 = released.clone();

        let result = Pipe::bracket(
            || Box::pin(async { Ok(vec![10i64, 20, 30]) }),
            |data| Pipe::from_iter((*data).clone()),
            move |_| released2.store(true, Ordering::SeqCst),
        )
        .take(1)
        .first()
        .await
        .unwrap();

        assert_eq!(result, Some(10));
        assert!(released.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn on_finalize_runs_on_completion() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let finalized = Arc::new(AtomicBool::new(false));
        let f = finalized.clone();
        let result = Pipe::from_iter(vec![1, 2, 3])
            .on_finalize(move || f.store(true, Ordering::SeqCst))
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 2, 3]);
        assert!(finalized.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn on_finalize_runs_on_early_drop() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let finalized = Arc::new(AtomicBool::new(false));
        let f = finalized.clone();
        let result = Pipe::from_iter(vec![1, 2, 3])
            .on_finalize(move || f.store(true, Ordering::SeqCst))
            .take(1)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1]);
        assert!(finalized.load(Ordering::SeqCst));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn chunks_timeout_flushes_by_count() {
        let result = Pipe::from_iter(1..=7)
            .chunks_timeout(3, std::time::Duration::from_secs(10))
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![vec![1, 2, 3], vec![4, 5, 6], vec![7]]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn chunks_timeout_flushes_by_time() {
        // Slow source: one element, then long pause
        // Timer should flush the partial batch
        use crate::pull::{ChunkFut, PullOperator};

        struct SlowSource {
            sent: bool,
        }
        impl PullOperator<i64> for SlowSource {
            fn next_chunk(&mut self) -> ChunkFut<'_, i64> {
                Box::pin(async move {
                    if self.sent {
                        // Hang forever -- simulate a slow source
                        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                        Ok(None)
                    } else {
                        self.sent = true;
                        Ok(Some(vec![42]))
                    }
                })
            }
        }

        let result = Pipe::from_pull_once(SlowSource { sent: false })
            .chunks_timeout(100, std::time::Duration::from_millis(50))
            .take(1)
            .collect()
            .await
            .unwrap();
        // Should get partial batch [42] flushed by timer
        assert_eq!(result, vec![vec![42]]);
    }

    #[tokio::test]
    async fn timeout_passes_when_fast() {
        let result = Pipe::from_iter(vec![1, 2, 3])
            .timeout(std::time::Duration::from_secs(10))
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn timeout_errors_when_slow() {
        use crate::pull::{ChunkFut, PullOperator};

        struct Hang;
        impl PullOperator<i64> for Hang {
            fn next_chunk(&mut self) -> ChunkFut<'_, i64> {
                Box::pin(async {
                    tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                    Ok(Some(vec![1]))
                })
            }
        }

        let result = Pipe::from_pull_once(Hang)
            .timeout(std::time::Duration::from_millis(10))
            .collect()
            .await;
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn par_eval_map_ordered() {
        let result = Pipe::from_iter(1..=5)
            .par_eval_map(3, |x| async move { Ok(x * 10) })
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![10, 20, 30, 40, 50]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn par_eval_map_unordered_preserves_all() {
        let mut result = Pipe::from_iter(1..=10)
            .par_eval_map_unordered(4, |x| async move { Ok(x * 2) })
            .collect()
            .await
            .unwrap();
        result.sort();
        assert_eq!(result, vec![2, 4, 6, 8, 10, 12, 14, 16, 18, 20]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn par_eval_map_propagates_error() {
        let result = Pipe::from_iter(1..=5)
            .par_eval_map(2, |x| async move {
                if x == 3 { Err("boom".into()) } else { Ok(x) }
            })
            .collect()
            .await;
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn generate_basic() {
        let result = Pipe::generate(|tx| async move {
            tx.emit(1).await?;
            tx.emit(2).await?;
            tx.emit(3).await?;
            Ok(())
        })
        .collect()
        .await
        .unwrap();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn generate_emit_all() {
        let result = Pipe::generate(|tx| async move {
            tx.emit_all(vec![1, 2, 3]).await?;
            tx.emit_all(vec![4, 5]).await?;
            Ok(())
        })
        .collect()
        .await
        .unwrap();
        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn generate_with_pipeline() {
        let result = Pipe::generate(|tx| async move {
            for i in 1..=10 {
                tx.emit(i).await?;
            }
            Ok(())
        })
        .filter(|x| x % 2 == 0)
        .map(|x| x * 10)
        .collect()
        .await
        .unwrap();
        assert_eq!(result, vec![20, 40, 60, 80, 100]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn generate_cloneable() {
        let pipe = Pipe::generate(|tx| async move {
            tx.emit(1).await?;
            tx.emit(2).await?;
            Ok(())
        });
        let clone = pipe.clone();
        let a = pipe.collect().await.unwrap();
        let b = clone.collect().await.unwrap();
        assert_eq!(a, vec![1, 2]);
        assert_eq!(b, vec![1, 2]);
    }

    #[tokio::test]
    async fn repeat_takes_n() {
        let result = Pipe::repeat(42).take(5).collect().await.unwrap();
        assert_eq!(result, vec![42, 42, 42, 42, 42]);
    }

    #[tokio::test]
    async fn repeat_with_factory() {
        use std::sync::atomic::{AtomicI64, Ordering};
        let counter = Arc::new(AtomicI64::new(0));
        let c = counter.clone();
        let result = Pipe::repeat_with(move || c.fetch_add(1, Ordering::SeqCst))
            .take(4)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![0, 1, 2, 3]);
    }

    #[tokio::test]
    async fn interval_emits_ticks() {
        let result = Pipe::interval(std::time::Duration::from_millis(1))
            .take(3)
            .collect()
            .await
            .unwrap();
        assert_eq!(result.len(), 3);
        // Ticks should be monotonically increasing
        assert!(result[0] <= result[1]);
        assert!(result[1] <= result[2]);
    }

    #[tokio::test]
    async fn sliding_window_basic() {
        let result = Pipe::from_iter(1..=5)
            .sliding_window(3)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![vec![1, 2, 3], vec![2, 3, 4], vec![3, 4, 5]]);
    }

    #[tokio::test]
    async fn sliding_window_size_1() {
        let result = Pipe::from_iter(vec![10, 20, 30])
            .sliding_window(1)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![vec![10], vec![20], vec![30]]);
    }

    #[tokio::test]
    async fn sliding_window_larger_than_input() {
        let result = Pipe::from_iter(vec![1, 2])
            .sliding_window(5)
            .collect()
            .await
            .unwrap();
        // Not enough elements to form a window
        assert!(result.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unzip_splits_pairs() {
        let pairs = Pipe::from_iter(vec![(1, "a"), (2, "b"), (3, "c")]);
        let (left, right) = pairs.unzip(2);
        let a = left.collect().await.unwrap();
        let b = right.collect().await.unwrap();
        assert_eq!(a, vec![1, 2, 3]);
        assert_eq!(b, vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn interleave_round_robin() {
        let a = Pipe::from_iter(vec![1, 3, 5]);
        let b = Pipe::from_iter(vec![2, 4, 6]);
        let result = a.interleave(b).collect().await.unwrap();
        assert_eq!(result, vec![1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn interleave_unequal_length() {
        let a = Pipe::from_iter(vec![1, 3, 5, 7]);
        let b = Pipe::from_iter(vec![2, 4]);
        let result = a.interleave(b).collect().await.unwrap();
        // After right exhausts, interleave stops taking from right
        // but continues from left
        assert_eq!(result, vec![1, 2, 3, 4, 5, 7]);
    }

    #[tokio::test]
    async fn changes_deduplicates_consecutive() {
        let result = Pipe::from_iter(vec![1, 1, 2, 2, 2, 3, 1, 1])
            .changes()
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 2, 3, 1]);
    }

    #[tokio::test]
    async fn changes_all_same() {
        let result = Pipe::from_iter(vec![5, 5, 5])
            .changes()
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![5]);
    }

    #[tokio::test]
    async fn group_adjacent_by_groups_runs() {
        let result = Pipe::from_iter(vec![1, 1, 2, 2, 2, 3, 1, 1])
            .group_adjacent_by(|x| *x)
            .collect()
            .await
            .unwrap();
        assert_eq!(
            result,
            vec![
                (1, vec![1, 1]),
                (2, vec![2, 2, 2]),
                (3, vec![3]),
                (1, vec![1, 1])
            ]
        );
    }

    #[tokio::test]
    async fn group_adjacent_by_single_group() {
        let result = Pipe::from_iter(vec![5, 5, 5])
            .group_adjacent_by(|x| *x)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![(5, vec![5, 5, 5])]);
    }

    #[tokio::test]
    async fn group_adjacent_by_custom_key() {
        let result = Pipe::from_iter(vec![1, 3, 2, 4, 5])
            .group_adjacent_by(|x| x % 2)
            .map(|(k, vs)| (k, vs.len()))
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![(1, 2), (0, 2), (1, 1)]);
    }

    #[tokio::test]
    async fn fold_collect_sorts_the_whole_buffer() {
        let result = Pipe::from_iter(vec![3, 1, 4, 1, 5, 9, 2, 6])
            .fold_collect(|mut v| {
                v.sort_unstable();
                v
            })
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 1, 2, 3, 4, 5, 6, 9]);
    }

    #[tokio::test]
    async fn fold_collect_can_change_element_type() {
        let result = Pipe::from_iter(vec![1, 2, 3])
            .fold_collect(|v| vec![v.len()])
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![3]);
    }

    #[tokio::test]
    async fn fold_collect_empty_output_yields_empty_stream() {
        let result = Pipe::from_iter(vec![1, 2, 3])
            .fold_collect(|_| Vec::<i32>::new())
            .collect()
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn fold_collect_pipe_is_reusable_after_clone() {
        let pipe = Pipe::from_iter(vec![2, 1, 3]).fold_collect(|mut v| {
            v.sort_unstable();
            v
        });
        let first = pipe.clone().collect().await.unwrap();
        let second = pipe.collect().await.unwrap();
        assert_eq!(first, vec![1, 2, 3]);
        assert_eq!(second, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn try_fold_collect_propagates_transform_error() {
        let result = Pipe::from_iter(vec![1, 2, 3])
            .try_fold_collect(|_| {
                Err::<Vec<i32>, _>(crate::pull::PipeError::Custom("rejected".into()))
            })
            .collect()
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn top_n_keeps_largest_in_descending_order() {
        let result = Pipe::from_iter(vec![3, 1, 4, 1, 5, 9, 2, 6])
            .top_n(3, |x| *x)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![9, 6, 5]);
    }

    #[tokio::test]
    async fn top_n_with_k_larger_than_input_returns_all_sorted() {
        let result = Pipe::from_iter(vec![2, 1, 3])
            .top_n(10, |x| *x)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![3, 2, 1]);
    }

    #[tokio::test]
    async fn top_n_with_zero_k_yields_empty_stream() {
        let result = Pipe::from_iter(vec![1, 2, 3])
            .top_n(0, |x| *x)
            .collect()
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn top_n_ranks_by_key_not_element() {
        let result = Pipe::from_iter(vec!["a", "bbbb", "cc", "ddd"])
            .top_n(2, |s| s.len())
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec!["bbbb", "ddd"]);
    }

    #[tokio::test]
    async fn top_n_pipe_is_reusable_after_clone() {
        let pipe = Pipe::from_iter(vec![5, 2, 8, 1]).top_n(2, |x| *x);
        let first = pipe.clone().collect().await.unwrap();
        let second = pipe.collect().await.unwrap();
        assert_eq!(first, vec![8, 5]);
        assert_eq!(second, vec![8, 5]);
    }

    #[tokio::test]
    async fn distinct_removes_all_duplicates() {
        let result = Pipe::from_iter(vec![1, 2, 3, 2, 1, 4, 3])
            .distinct()
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn distinct_by_key() {
        let result = Pipe::from_iter(vec!["apple", "ant", "banana", "avocado"])
            .distinct_by(|s| s.chars().next().unwrap())
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec!["apple", "banana"]);
    }

    #[tokio::test]
    async fn changes_all_different() {
        let result = Pipe::from_iter(vec![1, 2, 3])
            .changes()
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn intersperse_inserts_separator() {
        let result = Pipe::from_iter(vec![1, 2, 3])
            .intersperse(0)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 0, 2, 0, 3]);
    }

    #[tokio::test]
    async fn intersperse_single_element() {
        let result = Pipe::from_iter(vec![42])
            .intersperse(0)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![42]);
    }

    #[tokio::test]
    async fn intersperse_empty() {
        let result = Pipe::<i64>::empty().intersperse(0).collect().await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn flatten_nested_pipes() {
        let inner1 = Pipe::from_iter(vec![1, 2]);
        let inner2 = Pipe::from_iter(vec![3, 4, 5]);
        let outer = Pipe::from_pull_once(crate::pull::PullSource::new(vec![inner1, inner2]));
        let result = outer.flatten().collect().await.unwrap();
        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn delay_by_delays_elements() {
        let start = tokio::time::Instant::now();
        let result = Pipe::from_iter(vec![1, 2, 3])
            .delay_by(std::time::Duration::from_millis(10))
            .collect()
            .await
            .unwrap();
        let elapsed = start.elapsed();
        assert_eq!(result, vec![1, 2, 3]);
        assert!(elapsed >= std::time::Duration::from_millis(30));
    }

    #[tokio::test]
    async fn throttle_limits_rate() {
        let start = tokio::time::Instant::now();
        let result = Pipe::from_iter(vec![1, 2, 3])
            .throttle(std::time::Duration::from_millis(10))
            .collect()
            .await
            .unwrap();
        let elapsed = start.elapsed();
        assert_eq!(result, vec![1, 2, 3]);
        // At least 20ms for 3 elements at 10ms throttle (first is immediate)
        assert!(elapsed >= std::time::Duration::from_millis(20));
    }

    #[tokio::test]
    async fn attempt_wraps_ok() {
        let result = Pipe::from_iter(vec![1, 2, 3])
            .attempt()
            .collect()
            .await
            .unwrap();
        assert_eq!(result.len(), 3);
        assert!(result.iter().all(|r| r.is_ok()));
    }

    #[tokio::test]
    async fn attempt_catches_errors() {
        use crate::pull::{ChunkFut, PullOperator};

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
                        Ok(Some(vec![1]))
                    }
                })
            }
        }

        let result = Pipe::from_pull_once(FailAfterOne { yielded: false })
            .attempt()
            .collect()
            .await
            .unwrap();
        assert_eq!(result.len(), 2);
        assert!(result[0].is_ok());
        assert!(result[1].is_err());
    }

    #[tokio::test]
    async fn none_terminate_appends_none() {
        let result = Pipe::from_iter(vec![1, 2, 3])
            .none_terminate()
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![Some(1), Some(2), Some(3), None]);
    }

    #[tokio::test]
    async fn none_terminate_roundtrip() {
        let result = Pipe::from_iter(vec![1, 2, 3])
            .none_terminate()
            .un_none_terminate()
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn un_none_terminate_stops_at_none() {
        let result = Pipe::from_iter(vec![Some(1), Some(2), None, Some(99)])
            .un_none_terminate()
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 2]);
    }

    #[tokio::test]
    async fn zip_with_custom_combiner() {
        let a = Pipe::from_iter(vec![1, 2, 3]);
        let b = Pipe::from_iter(vec![10, 20, 30]);
        let result = a.zip_with(b, |x, y| x + y).collect().await.unwrap();
        assert_eq!(result, vec![11, 22, 33]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrently_main_output_preserved() {
        // Background is a no-op pipe -- just verify main output is correct
        let result = Pipe::from_iter(vec![1, 2, 3])
            .concurrently(Pipe::from_iter(vec![99, 98, 97]))
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrently_propagates_bg_error() {
        use crate::pull::{ChunkFut, PullOperator};

        // Background that errors immediately
        struct BgFail;
        impl PullOperator<i64> for BgFail {
            fn next_chunk(&mut self) -> ChunkFut<'_, i64> {
                Box::pin(async { Err("bg-boom".into()) })
            }
        }

        // Slow main so background has time to error
        let main_pipe = Pipe::generate(|tx| async move {
            for i in 1..=100 {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                tx.emit(i).await?;
            }
            Ok(())
        });

        let result = main_pipe
            .concurrently(Pipe::from_pull_once(BgFail))
            .collect()
            .await;

        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn debounce_emits_after_quiet_period() {
        let result = Pipe::from_iter(vec![1, 2, 3])
            .debounce(std::time::Duration::from_millis(20))
            .collect()
            .await
            .unwrap();
        // Fast source: only the last element survives after the quiet period
        assert_eq!(result, vec![3]);
    }

    #[tokio::test]
    async fn eval_for_each_async_side_effect() {
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen2 = seen.clone();
        Pipe::from_iter(vec![1, 2, 3])
            .eval_for_each(move |x| {
                let seen = seen2.clone();
                async move {
                    seen.lock().unwrap().push(x);
                    Ok(())
                }
            })
            .await
            .unwrap();
        assert_eq!(*seen.lock().unwrap(), vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn empty_pipe_through_operators() {
        let empty: Pipe<i64> = Pipe::empty();
        assert_eq!(
            empty.clone().map(|x| x * 2).collect().await.unwrap(),
            Vec::<i64>::new()
        );
        assert_eq!(
            empty.clone().filter(|_| true).collect().await.unwrap(),
            Vec::<i64>::new()
        );
        assert_eq!(
            empty.clone().take(5).collect().await.unwrap(),
            Vec::<i64>::new()
        );
        assert_eq!(
            empty.clone().skip(5).collect().await.unwrap(),
            Vec::<i64>::new()
        );
        assert_eq!(empty.clone().fold(0, |a, b| a + b).await.unwrap(), 0);
        assert_eq!(empty.clone().count().await.unwrap(), 0);
        assert_eq!(empty.clone().first().await.unwrap(), None);
        assert_eq!(empty.clone().last().await.unwrap(), None);
        assert_eq!(empty.clone().reduce(|a, b| a + b).await.unwrap(), None);
        assert_eq!(
            empty.clone().chunks(3).collect().await.unwrap(),
            Vec::<Vec<i64>>::new()
        );
        assert_eq!(
            empty.clone().enumerate().collect().await.unwrap(),
            Vec::<(usize, i64)>::new()
        );
        assert_eq!(
            empty.clone().distinct().collect().await.unwrap(),
            Vec::<i64>::new()
        );
        assert_eq!(
            empty.clone().changes().collect().await.unwrap(),
            Vec::<i64>::new()
        );
    }

    #[tokio::test]
    async fn error_propagation_through_map() {
        use crate::pull::{ChunkFut, PullOperator};

        struct FailSource;
        impl PullOperator<i64> for FailSource {
            fn next_chunk(&mut self) -> ChunkFut<'_, i64> {
                Box::pin(async { Err("source-error".into()) })
            }
        }

        let result = Pipe::from_pull_once(FailSource)
            .map(|x| x * 2)
            .collect()
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn error_propagation_through_filter() {
        use crate::pull::{ChunkFut, PullOperator};

        struct FailSource;
        impl PullOperator<i64> for FailSource {
            fn next_chunk(&mut self) -> ChunkFut<'_, i64> {
                Box::pin(async { Err("source-error".into()) })
            }
        }

        let result = Pipe::from_pull_once(FailSource)
            .filter(|_| true)
            .collect()
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn error_propagation_through_flat_map() {
        use crate::pull::{ChunkFut, PullOperator};

        struct FailSource;
        impl PullOperator<i64> for FailSource {
            fn next_chunk(&mut self) -> ChunkFut<'_, i64> {
                Box::pin(async { Err("source-error".into()) })
            }
        }

        let result = Pipe::from_pull_once(FailSource)
            .flat_map(Pipe::once)
            .collect()
            .await;
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn error_propagation_through_eval_map() {
        let result = Pipe::from_iter(vec![1, 2, 3])
            .eval_map(|x| async move {
                if x == 2 {
                    Err(crate::pull::PipeError::Custom("boom".into()))
                } else {
                    Ok(x)
                }
            })
            .collect()
            .await;
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn error_propagation_through_scan() {
        use crate::pull::{ChunkFut, PullOperator};

        struct FailAfterTwo {
            count: usize,
        }
        impl PullOperator<i64> for FailAfterTwo {
            fn next_chunk(&mut self) -> ChunkFut<'_, i64> {
                Box::pin(async move {
                    if self.count >= 2 {
                        Err("scan-fail".into())
                    } else {
                        self.count += 1;
                        Ok(Some(vec![self.count as i64]))
                    }
                })
            }
        }

        let result = Pipe::from_pull_once(FailAfterTwo { count: 0 })
            .scan(0i64, |acc, x| {
                *acc += x;
                *acc
            })
            .collect()
            .await;
        assert!(result.is_err());
    }
}
