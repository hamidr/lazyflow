//! Ordered parallel map: workers live no longer than the pipe, and output
//! keeps pace with the consumer, not with upstream readiness.

use lazyflow::prelude::*;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
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
