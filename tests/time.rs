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

const RATE: Duration = Duration::from_millis(40);

/// When each element left `throttle`, measured from the start.
async fn throttled_at(source: Pipe<i32>, rate: Duration) -> Vec<Duration> {
    let start = tokio::time::Instant::now();
    source
        .throttle(rate)
        .map(move |_| start.elapsed())
        .collect()
        .await
        .unwrap()
}

#[tokio::test(start_paused = true)]
async fn throttle_spaces_the_elements_of_one_source_chunk() {
    let times = throttled_at(Pipe::from_iter(0..3), RATE).await;
    assert_eq!(times, [Duration::ZERO, RATE, RATE * 2]);
}

#[tokio::test(start_paused = true)]
async fn throttle_spaces_elements_arriving_in_many_small_chunks() {
    let source = Pipe::once(0).chain(Pipe::once(1)).chain(Pipe::once(2));
    let times = throttled_at(source, RATE).await;
    assert_eq!(times, [Duration::ZERO, RATE, RATE * 2]);
}

#[tokio::test(start_paused = true)]
async fn throttle_does_not_burst_to_catch_up_after_a_slow_consumer() {
    const SLOW: Duration = Duration::from_millis(100);
    let start = tokio::time::Instant::now();
    let times = Pipe::from_iter(0..3)
        .throttle(RATE)
        .map(move |_| start.elapsed())
        .eval_map(|t| async move {
            tokio::time::sleep(SLOW).await;
            Ok(t)
        })
        .collect()
        .await
        .unwrap();
    assert!(times.windows(2).all(|w| w[1] - w[0] >= RATE), "{times:?}");
}

#[tokio::test(start_paused = true)]
async fn throttle_with_zero_duration_passes_elements_straight_through() {
    let times = throttled_at(Pipe::from_iter(0..3), Duration::ZERO).await;
    assert_eq!(times, [Duration::ZERO; 3]);
}

#[tokio::test(start_paused = true)]
async fn throttle_waits_only_for_the_elements_taken() {
    let start = tokio::time::Instant::now();
    let taken = Pipe::iterate(0, |x| x + 1)
        .throttle(RATE)
        .take(2)
        .collect()
        .await
        .unwrap();
    assert_eq!(taken, [0, 1]);
    assert_eq!(start.elapsed(), RATE);
}

#[tokio::test(start_paused = true)]
async fn chunks_timeout_of_size_zero_batches_single_elements() {
    let batches = Pipe::from_iter([1, 2])
        .chunks_timeout(0, BATCH_TIMEOUT)
        .take(3)
        .collect()
        .await
        .unwrap();
    assert_eq!(batches, [vec![1], vec![2]]);
}

#[tokio::test(start_paused = true)]
async fn chunks_timeout_of_size_zero_ends_with_a_finite_source() {
    let batches = Pipe::once(1)
        .chunks_timeout(0, BATCH_TIMEOUT)
        .collect()
        .await
        .unwrap();
    assert_eq!(batches, [vec![1]]);
}

const QUIET: Duration = Duration::from_millis(20);

#[tokio::test(start_paused = true)]
async fn debounce_keeps_an_element_still_in_flight_when_the_quiet_period_ends() {
    let source = Pipe::once(1).chain(Pipe::once(2).delay_by(SLOW_PULL));
    let out = source.debounce(QUIET).collect().await.unwrap();
    assert_eq!(out, [1, 2]);
}

#[tokio::test(start_paused = true)]
async fn debounce_keeps_a_delayed_error_after_emitting() {
    let source = Pipe::once(1).chain(Pipe::once(2).eval_map(|_| async {
        tokio::time::sleep(SLOW_PULL).await;
        Err(PipeError::Timeout)
    }));
    let mut results = source
        .debounce(QUIET)
        .attempt()
        .collect()
        .await
        .unwrap()
        .into_iter();
    assert_eq!(results.next().unwrap().unwrap(), 1);
    assert!(matches!(results.next(), Some(Err(PipeError::Timeout))));
}

#[tokio::test(start_paused = true)]
async fn debounce_emits_only_the_latest_of_a_quick_burst() {
    let out = Pipe::from_iter([1, 2, 3])
        .chain(Pipe::once(4).delay_by(SLOW_PULL))
        .debounce(QUIET)
        .collect()
        .await
        .unwrap();
    assert_eq!(out, [3, 4]);
}

#[tokio::test(start_paused = true)]
async fn debounce_waits_for_quiet_measured_from_the_latest_element() {
    let start = tokio::time::Instant::now();
    let times = Pipe::once(1)
        .chain(Pipe::once(2).delay_by(Duration::from_millis(10)))
        .chain(Pipe::once(3).delay_by(SLOW_PULL))
        .debounce(QUIET)
        .map(move |_| start.elapsed())
        .collect()
        .await
        .unwrap();
    assert_eq!(times[0], Duration::from_millis(10) + QUIET);
}
