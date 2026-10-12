//! un_none_terminate ends the pipe at the first None for good: nothing
//! after it is emitted, and the source is not pulled again.

use lazyflow::prelude::*;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

async fn terminated(source: Pipe<Option<i32>>) -> Vec<i32> {
    source.un_none_terminate().collect().await.unwrap()
}

#[tokio::test]
async fn items_after_the_sentinel_in_a_later_chunk_are_not_emitted() {
    let source = Pipe::from_iter([Some(1), None]).chain(Pipe::once(Some(2)));
    assert_eq!(terminated(source).await, [1]);
}

#[tokio::test]
async fn a_sentinel_opening_a_later_chunk_ends_the_pipe() {
    let source = Pipe::from_iter([Some(1)])
        .chain(Pipe::from_iter([None, Some(2)]))
        .chain(Pipe::once(Some(3)));
    assert_eq!(terminated(source).await, [1]);
}

#[tokio::test]
async fn a_sentinel_first_ends_the_pipe_empty() {
    let source = Pipe::once(None).chain(Pipe::once(Some(1)));
    assert!(terminated(source).await.is_empty());
}

#[tokio::test]
async fn a_sentinel_last_keeps_everything_before_it() {
    assert_eq!(
        terminated(Pipe::from_iter([Some(1), Some(2), None])).await,
        [1, 2]
    );
}

#[tokio::test]
async fn without_a_sentinel_the_pipe_ends_with_its_source() {
    let source = Pipe::from_iter([Some(1)]).chain(Pipe::from_iter([Some(2)]));
    assert_eq!(terminated(source).await, [1, 2]);
}

#[tokio::test]
async fn the_source_is_not_pulled_after_the_sentinel() {
    let pulled_after = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&pulled_after);
    let after = Pipe::once(Some(2)).eval_tap(move |_| {
        flag.store(true, Ordering::SeqCst);
        async { Ok(()) }
    });
    let source = Pipe::from_iter([Some(1), None]).chain(after);
    assert_eq!(terminated(source).await, [1]);
    assert!(!pulled_after.load(Ordering::SeqCst));
}

#[tokio::test]
async fn an_empty_source_chunk_does_not_end_the_pipe() {
    let source = Pipe::from_iter([Some(1)])
        .chain(Pipe::from_iter(Vec::<Option<i32>>::new()))
        .chain(Pipe::once(Some(2)));
    assert_eq!(terminated(source).await, [1, 2]);
}
