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

#[tokio::test(flavor = "current_thread")]
async fn prefetch_lets_a_sibling_run() {
    sibling_runs_early(|p| async move {
        p.prefetch(2).collect().await.unwrap();
    })
    .await;
}

/// Like `tokio::io::sink`, but without its own budget accounting.
struct PlainSink;

impl tokio::io::AsyncWrite for PlainSink {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::task::Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn into_writer_lets_a_sibling_run() {
    sibling_runs_early(|p| async move {
        p.map(|_| vec![1u8]).into_writer(PlainSink).await.unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn concurrently_background_lets_a_sibling_run() {
    // The main pipe waits on a timer, so the background drain runs alone.
    let pulls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(AtomicUsize::new(usize::MAX));
    let main = Pipe::from_iter([1u64]).eval_map(|x| async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(x)
    });
    let driver = tokio::spawn(main.concurrently(ready(&pulls, 100_000)).collect());
    let sibling = tokio::spawn({
        let (pulls, seen) = (Arc::clone(&pulls), Arc::clone(&seen));
        async move {
            // Let the driver start the background drain first.
            tokio::task::yield_now().await;
            seen.store(pulls.load(Ordering::Relaxed), Ordering::Relaxed)
        }
    });
    driver.await.unwrap().unwrap();
    sibling.await.unwrap();
    let at = seen.load(Ordering::Relaxed);
    assert!(
        at <= SIBLING_BOUND,
        "sibling waited for {at} background pulls, bound is {SIBLING_BOUND}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn filter_dropping_everything_lets_a_sibling_run_under_collect() {
    sibling_runs_early(|p| async move {
        p.filter(|_| false).collect().await.unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn filter_dropping_everything_lets_a_sibling_run_under_first() {
    sibling_runs_early(|p| async move {
        p.filter(|_| false).first().await.unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn fixpoint_over_repeated_values_lets_a_sibling_run() {
    sibling_runs_early(|p| async move {
        p.fixpoint(|_| Pipe::empty()).collect().await.unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn skip_lets_a_sibling_run() {
    sibling_runs_early(|p| async move {
        p.skip(usize::MAX).collect().await.unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn skip_while_lets_a_sibling_run() {
    sibling_runs_early(|p| async move {
        p.skip_while(|_| true).collect().await.unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn flat_map_into_nothing_lets_a_sibling_run() {
    sibling_runs_early(|p| async move {
        p.flat_map(|_| Pipe::<u64>::empty())
            .collect()
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn flatten_of_empty_pipes_lets_a_sibling_run() {
    sibling_runs_early(|p| async move {
        p.map(|_| Pipe::<u64>::empty())
            .flatten()
            .collect()
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn lines_without_a_newline_lets_a_sibling_run() {
    sibling_runs_early(|p| async move {
        p.map(|_| b"a".to_vec()).lines().collect().await.unwrap();
    })
    .await;
}

/// Ready source of empty chunks, which `un_none_terminate` has to skip.
struct ReadyEmpty {
    left: usize,
}

impl PullOperator<Option<u64>> for ReadyEmpty {
    fn next_chunk(&mut self) -> ChunkFut<'_, Option<u64>> {
        Box::pin(async move {
            if self.left == 0 {
                return Ok(None);
            }
            self.left -= 1;
            Ok(Some(Vec::new()))
        })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn un_none_terminate_over_empty_chunks_lets_a_sibling_run() {
    let seen = Arc::new(AtomicUsize::new(0));
    let driver = tokio::spawn(async {
        Pipe::from_pull(|| Box::new(ReadyEmpty { left: 100_000 }))
            .un_none_terminate()
            .collect()
            .await
            .unwrap();
    });
    let sibling = tokio::spawn({
        let seen = Arc::clone(&seen);
        async move { seen.store(1, Ordering::Relaxed) }
    });
    // The sibling ran before the driver ended iff the driver yielded.
    sibling.await.unwrap();
    assert_eq!(seen.load(Ordering::Relaxed), 1);
    assert!(
        !driver.is_finished(),
        "driver ran to its end without yielding"
    );
    driver.await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn top_n_lets_a_sibling_run() {
    sibling_runs_early(|p| async move {
        p.top_n(1, |x| *x).collect().await.unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn chunks_larger_than_the_source_lets_a_sibling_run() {
    sibling_runs_early(|p| async move {
        p.chunks(10_000_000).collect().await.unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn sliding_window_larger_than_the_source_lets_a_sibling_run() {
    sibling_runs_early(|p| async move {
        p.sliding_window(10_000_000).collect().await.unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn group_adjacent_by_one_key_lets_a_sibling_run() {
    sibling_runs_early(|p| async move {
        p.group_adjacent_by(|_| 0u8).collect().await.unwrap();
    })
    .await;
}

/// Runs `drive` over a 100k-chunk source of empty chunks; the sibling
/// must run before the driver ends.
async fn empty_chunks_let_a_sibling_run<F, Fut>(drive: F)
where
    F: FnOnce(Pipe<Option<u64>>) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let driver = tokio::spawn(drive(Pipe::from_pull(|| {
        Box::new(ReadyEmpty { left: 100_000 })
    })));
    let sibling = tokio::spawn(async {});
    sibling.await.unwrap();
    assert!(
        !driver.is_finished(),
        "driver ran to its end without yielding"
    );
    driver.await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn throttle_over_empty_chunks_lets_a_sibling_run() {
    empty_chunks_let_a_sibling_run(|p| async move {
        p.throttle(Duration::from_millis(1))
            .collect()
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn zip_with_a_side_of_empty_chunks_lets_a_sibling_run() {
    empty_chunks_let_a_sibling_run(|p| async move {
        p.zip(Pipe::from_iter([1u8])).collect().await.unwrap();
    })
    .await;
}
