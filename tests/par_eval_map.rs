//! Ordered parallel map: workers live no longer than the pipe, and output
//! keeps pace with the consumer, not with upstream readiness.

use lazyflow::prelude::*;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

const SLOW_WORK: Duration = Duration::from_millis(100);

/// Item 0 waits until item 1 has started, then resolves with `first`;
/// item 1 records a side effect after `SLOW_WORK` unless it is cancelled.
fn racing_workers(
    first: impl Fn() -> Result<i32, PipeError> + Send + Sync + 'static,
) -> (Pipe<i32>, Arc<AtomicBool>) {
    let second_started = Arc::new(tokio::sync::Notify::new());
    let completed = Arc::new(AtomicBool::new(false));
    let observed = Arc::clone(&completed);
    let first = Arc::new(first);
    let pipe = Pipe::from_iter([0, 1]).par_eval_map(2, move |x| {
        let started = Arc::clone(&second_started);
        let completed = Arc::clone(&completed);
        let first = Arc::clone(&first);
        async move {
            if x == 0 {
                started.notified().await;
                first()
            } else {
                started.notify_one();
                tokio::time::sleep(SLOW_WORK).await;
                completed.store(true, Ordering::SeqCst);
                Ok(x)
            }
        }
    });
    (pipe, observed)
}

async fn after_slow_work_would_finish() {
    tokio::time::sleep(SLOW_WORK * 2).await;
}

#[tokio::test(start_paused = true)]
async fn workers_still_running_are_cancelled_when_the_consumer_stops() {
    let (pipe, completed) = racing_workers(|| Ok(0));
    assert_eq!(pipe.first().await.unwrap(), Some(0));
    after_slow_work_would_finish().await;
    assert!(!completed.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn workers_still_running_are_cancelled_when_an_earlier_worker_fails() {
    let (pipe, completed) = racing_workers(|| Err(PipeError::Timeout));
    assert!(matches!(pipe.collect().await, Err(PipeError::Timeout)));
    after_slow_work_would_finish().await;
    assert!(!completed.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn workers_still_running_are_cancelled_when_an_earlier_worker_panics() {
    let (pipe, completed) = racing_workers(|| panic!("worker failed"));
    assert!(matches!(pipe.collect().await, Err(PipeError::Panicked(_))));
    after_slow_work_would_finish().await;
    assert!(!completed.load(Ordering::SeqCst));
}

fn never_ready<T: Send + 'static>() -> Pipe<T> {
    Pipe::generate(|_tx| std::future::pending())
}

#[tokio::test(start_paused = true)]
async fn finished_results_are_emitted_while_upstream_is_still_pending() {
    let source = Pipe::from_iter([1, 2]).chain(never_ready());
    let pipe = source.par_eval_map(2, |x| async move { Ok(x) }).take(2);
    let result = tokio::time::timeout(SLOW_WORK, pipe.collect()).await;
    assert_eq!(
        result.expect("output waited on upstream").unwrap(),
        vec![1, 2]
    );
}

/// Work started before the consumer takes one item stays within the
/// in-flight window plus the output buffer, whatever the source size.
#[tokio::test(start_paused = true)]
async fn work_ahead_of_the_consumer_is_bounded_by_concurrency() {
    const CONCURRENCY: usize = 2;
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let first = Pipe::from_iter(0..1024)
        .par_eval_map(CONCURRENCY, move |x| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok(x) }
        })
        .first()
        .await
        .unwrap();
    assert_eq!(first, Some(0));
    assert!(observed.load(Ordering::SeqCst) <= 2 * CONCURRENCY + 1);
}

#[tokio::test(start_paused = true)]
async fn a_slow_first_item_keeps_order_and_never_exceeds_concurrency() {
    const CONCURRENCY: usize = 3;
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let observed_peak = Arc::clone(&peak);
    let out = Pipe::from_iter(0..50u64)
        .par_eval_map(CONCURRENCY, move |x| {
            let active = Arc::clone(&active);
            let peak = Arc::clone(&peak);
            async move {
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                let delay = if x == 0 { 50 } else { x % 4 };
                tokio::time::sleep(Duration::from_millis(delay)).await;
                active.fetch_sub(1, Ordering::SeqCst);
                Ok(x)
            }
        })
        .collect()
        .await
        .unwrap();
    assert_eq!(out, (0..50).collect::<Vec<_>>());
    assert!(observed_peak.load(Ordering::SeqCst) <= CONCURRENCY);
}

#[tokio::test(start_paused = true)]
async fn concurrency_one_maps_every_source_chunk_in_order() {
    let source = Pipe::from_iter(0..3).chain(Pipe::from_iter(3..6));
    let out = source
        .par_eval_map(1, |x| async move { Ok(x * 10) })
        .collect()
        .await
        .unwrap();
    assert_eq!(out, vec![0, 10, 20, 30, 40, 50]);
}

#[tokio::test(start_paused = true)]
async fn an_upstream_failure_follows_the_results_of_earlier_items() {
    let failing = Pipe::<i32>::generate(|_tx| async { Err(PipeError::Timeout) });
    let source = Pipe::from_iter([1, 2]).chain(failing);
    let mut results = source
        .par_eval_map(2, |x| async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(x)
        })
        .attempt()
        .collect()
        .await
        .unwrap()
        .into_iter();
    assert_eq!(results.next().unwrap().unwrap(), 1);
    assert_eq!(results.next().unwrap().unwrap(), 2);
    assert!(matches!(results.next(), Some(Err(PipeError::Timeout))));
    assert!(results.next().is_none());
}
