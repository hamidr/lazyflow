//! A fan-out branch that stops reading must not stop the others.

use lazyflow::prelude::*;
use std::time::Duration;

const N: i64 = 64;

/// One chunk per item, many more than the branch buffers hold, so the
/// feeding task sends to the dropped branch after it is gone.
fn source() -> Pipe<i64> {
    Pipe::generate(|tx| async move {
        for i in 0..N {
            tx.emit(i).await?;
        }
        Ok(())
    })
}

async fn within_a_second<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(1), f)
        .await
        .expect("the surviving branch stalled")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broadcast_branch_dropped_midway_leaves_the_others_complete() {
    let mut branches = source().broadcast(2, 1);
    let keep = branches.pop().unwrap();
    let quitter = branches.pop().unwrap();
    let kept = tokio::spawn(keep.collect());
    let first = quitter.take(1).collect().await.unwrap();
    assert_eq!(first, vec![0]);
    let kept = within_a_second(kept).await.unwrap().unwrap();
    assert_eq!(kept, (0..N).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_partition_branch_dropped_midway_leaves_the_others_complete() {
    let mut branches = source().partition(2, 1, |x| *x as u64);
    let odd = branches.pop().unwrap();
    let even = branches.pop().unwrap();
    let kept = tokio::spawn(odd.collect());
    let first = even.take(1).collect().await.unwrap();
    assert_eq!(first, vec![0]);
    let kept = within_a_second(kept).await.unwrap().unwrap();
    assert_eq!(kept, (0..N).filter(|x| x % 2 == 1).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broadcast_branch_dropped_before_any_branch_runs_leaves_the_others_complete() {
    let mut branches = source().broadcast(2, 1);
    let keep = branches.pop().unwrap();
    drop(branches);
    let kept = within_a_second(keep.collect()).await.unwrap();
    assert_eq!(kept, (0..N).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_partition_branch_dropped_before_any_branch_runs_leaves_the_others_complete() {
    let mut branches = source().partition(2, 1, |x| *x as u64);
    let odd = branches.pop().unwrap();
    drop(branches);
    let kept = within_a_second(odd.collect()).await.unwrap();
    assert_eq!(kept, (0..N).filter(|x| x % 2 == 1).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broadcast_branch_dropped_after_the_source_started_leaves_the_others_complete() {
    let mut branches = source().broadcast(3, 1);
    let keep = branches.pop().unwrap();
    let started = branches.pop().unwrap();
    assert_eq!(started.take(1).collect().await.unwrap(), vec![0]);
    drop(branches);
    let kept = within_a_second(keep.collect()).await.unwrap();
    assert_eq!(kept, (0..N).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_source_failure_reaches_the_branches_left_after_one_was_dropped() {
    let failing = source().chain(Pipe::generate(|_tx| async { Err(PipeError::Timeout) }));
    let mut branches = failing.broadcast(2, 1);
    let keep = branches.pop().unwrap();
    drop(branches);
    let result = within_a_second(keep.collect()).await;
    assert!(matches!(result, Err(PipeError::Timeout)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_source_stops_once_every_branch_is_dropped() {
    let emitted = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = std::sync::Arc::clone(&emitted);
    let endless = Pipe::generate(move |tx| {
        let counter = std::sync::Arc::clone(&counter);
        async move {
            loop {
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tx.emit(0i64).await?;
            }
        }
    });
    let mut branches = endless.broadcast(2, 1);
    let first = branches.pop().unwrap();
    assert_eq!(first.first().await.unwrap(), Some(0));
    drop(branches);
    tokio::time::sleep(Duration::from_millis(20)).await;
    let settled = emitted.load(std::sync::atomic::Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(emitted.load(std::sync::atomic::Ordering::SeqCst), settled);
}
