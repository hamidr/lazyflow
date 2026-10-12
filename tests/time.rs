//! Time operators must compose with upstream pulls that are not cancellation-safe.

use lazyflow::prelude::*;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

const BATCH_TIMEOUT: Duration = Duration::from_millis(20);
const SLOW_PULL: Duration = Duration::from_millis(105);

#[tokio::test(start_paused = true)]
async fn chunks_timeout_keeps_in_flight_data_across_multiple_deadlines() {
    let source = Pipe::once(1).chain(Pipe::once(2).delay_by(SLOW_PULL));
    let batches = source
        .chunks_timeout(10, BATCH_TIMEOUT)
        .collect()
        .await
        .unwrap();
    assert_eq!(batches, vec![vec![1], vec![2]]);
}

#[tokio::test(start_paused = true)]
async fn chunks_timeout_preserves_a_delayed_error_after_flushing() {
    let source = Pipe::once(1).chain(Pipe::once(2).eval_map(|_| async {
        tokio::time::sleep(SLOW_PULL).await;
        Err(PipeError::Timeout)
    }));
    let mut results = source
        .chunks_timeout(10, BATCH_TIMEOUT)
        .attempt()
        .collect()
        .await
        .unwrap()
        .into_iter();
    assert_eq!(results.next().unwrap().unwrap(), vec![1]);
    assert!(matches!(results.next(), Some(Err(PipeError::Timeout))));
    assert!(results.next().is_none());
}

#[tokio::test(start_paused = true)]
async fn chunks_timeout_keeps_a_slow_resource_acquisition_alive() {
    let released = Arc::new(AtomicBool::new(false));
    let observed = released.clone();
    let source = Pipe::bracket(
        || {
            Box::pin(async {
                tokio::time::sleep(SLOW_PULL).await;
                Ok(2)
            })
        },
        |resource| Pipe::once(*resource),
        move |_| {
            released.store(true, Ordering::SeqCst);
        },
    );
    let batches = source
        .chunks_timeout(10, BATCH_TIMEOUT)
        .collect()
        .await
        .unwrap();
    assert_eq!(batches, vec![vec![2]]);
    assert!(observed.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn chunks_timeout_flushes_on_time_and_cancels_pending_work_when_dropped() {
    let finalized = Arc::new(AtomicBool::new(false));
    let observed = finalized.clone();
    let source = Pipe::once(1)
        .chain(Pipe::once(2).eval_map(|_| std::future::pending::<Result<i32, PipeError>>()))
        .on_finalize(move || {
            finalized.store(true, Ordering::SeqCst);
        });
    let batch = tokio::time::timeout(
        BATCH_TIMEOUT * 2,
        source.chunks_timeout(10, BATCH_TIMEOUT).first(),
    )
    .await
    .expect("partial batch must flush before upstream finishes")
    .unwrap();
    assert_eq!(batch, Some(vec![1]));
    // Receiver drop aborts its producer; allow the abort to run its finalizers.
    tokio::task::yield_now().await;
    assert!(observed.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn chunks_timeout_preserves_count_batches_and_final_remainder() {
    let batches = Pipe::from_iter(0..7)
        .chunks_timeout(3, BATCH_TIMEOUT)
        .collect()
        .await
        .unwrap();
    assert_eq!(batches, vec![vec![0, 1, 2], vec![3, 4, 5], vec![6]]);
}

#[tokio::test(start_paused = true)]
async fn chunks_timeout_emits_nothing_for_empty_input() {
    let batches = Pipe::<i32>::empty()
        .chunks_timeout(3, BATCH_TIMEOUT)
        .collect()
        .await
        .unwrap();
    assert!(batches.is_empty());
}
