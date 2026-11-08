//! A pipe over an always-ready source must give the worker back, so a
//! sibling task runs and a `timeout` fires. Run on a `current_thread`
//! runtime, where a driver that never returns `Pending` starves everything.

use std::future::Future;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use lazyflow::prelude::*;

/// Source pulls a sibling task may wait for. The budget is 128 pulls, so
/// 1000 leaves room for operators that spend several units per chunk.
const SIBLING_BOUND: usize = 1000;

/// Finite source of one-element chunks that is ready on every pull.
struct Ready {
    pulls: Arc<AtomicUsize>,
    left: usize,
}

impl PullOperator<u64> for Ready {
    fn next_chunk(&mut self) -> ChunkFut<'_, u64> {
        Box::pin(async move {
            self.pulls.fetch_add(1, Ordering::Relaxed);
            if self.left == 0 {
                return Ok(None);
            }
            self.left -= 1;
            Ok(Some(vec![1]))
        })
    }
}

fn ready(pulls: &Arc<AtomicUsize>, chunks: usize) -> Pipe<u64> {
    let pulls = Arc::clone(pulls);
    Pipe::from_pull(move || {
        Box::new(Ready {
            pulls: Arc::clone(&pulls),
            left: chunks,
        })
    })
}

/// Run `drive` over a 100k-chunk ready source beside a sibling task, and
/// assert the sibling got to run within [`SIBLING_BOUND`] source pulls.
async fn sibling_runs_early<F, Fut>(drive: F)
where
    F: FnOnce(Pipe<u64>) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let pulls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(AtomicUsize::new(usize::MAX));
    // The driver is spawned first so it runs first; the sibling can only
    // go ahead of the driver's end if the driver yields.
    let driver = tokio::spawn(drive(ready(&pulls, 100_000)));
    let sibling = tokio::spawn({
        let (pulls, seen) = (Arc::clone(&pulls), Arc::clone(&seen));
        async move { seen.store(pulls.load(Ordering::Relaxed), Ordering::Relaxed) }
    });
    driver.await.unwrap();
    sibling.await.unwrap();
    let at = seen.load(Ordering::Relaxed);
    assert!(
        at <= SIBLING_BOUND,
        "sibling waited for {at} source pulls, bound is {SIBLING_BOUND}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn collect_lets_a_sibling_run() {
    sibling_runs_early(|p| async move {
        p.collect().await.unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn fold_lets_a_sibling_run() {
    sibling_runs_early(|p| async move {
        p.fold(0u64, |a, b| a + b).await.unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn try_fold_chunks_lets_a_sibling_run() {
    sibling_runs_early(|p| async move {
        p.try_fold_chunks(0usize, |n, chunk| ControlFlow::Continue(n + chunk.len()))
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn fold_collect_lets_a_sibling_run() {
    sibling_runs_early(|p| async move {
        p.fold_collect(|all| all).collect().await.unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn timeout_fires_around_a_ready_source() {
    // Ready for half a second, so an unyielding drive returns Ok long
    // after the 20 ms timer was due.
    let until = Instant::now() + Duration::from_millis(500);
    let source = Pipe::from_pull(move || {
        struct ForAWhile(Instant);
        impl PullOperator<u64> for ForAWhile {
            fn next_chunk(&mut self) -> ChunkFut<'_, u64> {
                let live = Instant::now() < self.0;
                Box::pin(async move { Ok(live.then(|| vec![1])) })
            }
        }
        Box::new(ForAWhile(until))
    });
    let driver = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_millis(20), source.collect()).await
    });
    assert!(
        driver.await.unwrap().is_err(),
        "timeout did not fire around the pipe"
    );
}
