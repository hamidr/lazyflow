//! A panic inside a spawned producer task must reach the consumer as an
//! error. When the task dies, its sender drops and the channel closes,
//! which a receiver cannot tell apart from a normal end of stream: the
//! pipe would silently end short instead of failing.

use lazyflow::prelude::*;
use std::time::Duration;

fn panics_at_two() -> Pipe<i64> {
    Pipe::from_iter(vec![1i64, 2, 3]).map(|x| {
        if x == 2 {
            panic!("boom at two");
        }
        x
    })
}

async fn assert_panic_surfaces<B: Send + std::fmt::Debug + 'static>(pipe: Pipe<B>) {
    match pipe.collect().await {
        Err(e) => assert!(
            e.to_string().contains("boom at two"),
            "error names the panic: {e}"
        ),
        Ok(rows) => panic!("panic became a short stream of {} items", rows.len()),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panic_in_a_generator_fails_the_pipe() {
    let pipe: Pipe<i64> = Pipe::generate(|tx| async move {
        tx.emit(1).await?;
        panic!("boom at two");
    });
    assert_panic_surfaces(pipe).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panic_in_a_one_shot_generator_fails_the_pipe() {
    let pipe: Pipe<i64> = Pipe::generate_once(|tx| async move {
        tx.emit(1).await?;
        panic!("boom at two");
    });
    assert_panic_surfaces(pipe).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panic_upstream_of_prefetch_fails_the_pipe() {
    assert_panic_surfaces(panics_at_two().prefetch(1)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panic_in_any_merged_source_fails_the_pipe() {
    let ok = Pipe::from_iter(vec![10i64, 11]);
    assert_panic_surfaces(Pipe::merge(vec![ok, panics_at_two()])).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panic_in_a_switched_inner_pipe_fails_the_pipe() {
    assert_panic_surfaces(Pipe::once(0i64).switch_map(|_| panics_at_two())).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panic_upstream_of_debounce_fails_the_pipe() {
    assert_panic_surfaces(panics_at_two().debounce(Duration::from_millis(1))).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panic_upstream_of_chunks_timeout_fails_the_pipe() {
    assert_panic_surfaces(panics_at_two().chunks_timeout(8, Duration::from_millis(1))).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_upstream_error_through_chunks_timeout_fails_the_pipe() {
    let pipe = Pipe::from_iter(vec![1i64, 2]).eval_map(|x| async move {
        if x == 2 {
            Err(PipeError::Custom("boom at two".into()))
        } else {
            Ok(x)
        }
    });
    assert_panic_surfaces(pipe.chunks_timeout(8, Duration::from_millis(1))).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panic_in_an_ordered_worker_fails_the_pipe() {
    let pipe = Pipe::from_iter(vec![1i64, 2, 3]).par_eval_map(2, |x| async move {
        if x == 2 {
            panic!("boom at two");
        }
        Ok(x)
    });
    assert_panic_surfaces(pipe).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panic_in_an_unordered_worker_fails_the_pipe() {
    let pipe = Pipe::from_iter(vec![1i64, 2, 3]).par_eval_map_unordered(2, |x| async move {
        if x == 2 {
            panic!("boom at two");
        }
        Ok(x)
    });
    assert_panic_surfaces(pipe).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panic_upstream_of_a_stream_fails_the_stream() {
    use futures_core::Stream;
    let mut stream = std::pin::pin!(panics_at_two().into_stream());
    let mut saw_error = false;
    while let Some(item) = std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
        if let Err(e) = item {
            assert!(e.to_string().contains("boom at two"), "{e}");
            saw_error = true;
        }
    }
    assert!(saw_error, "panic became a short stream");
}

fn fails_at_two() -> Pipe<i64> {
    Pipe::from_iter(vec![1i64, 2, 3]).eval_map(|x| async move {
        if x == 2 {
            Err(PipeError::Custom("boom at two".into()))
        } else {
            Ok(x)
        }
    })
}

/// Every branch of a fan-out reads the same source, so every branch must
/// fail when the source does, not just end early.
async fn assert_every_branch_fails(branches: Vec<Pipe<i64>>) {
    let handles: Vec<_> = branches
        .into_iter()
        .map(|b| tokio::spawn(b.collect()))
        .collect();
    for (i, h) in handles.into_iter().enumerate() {
        match h.await.unwrap() {
            Err(e) => assert!(e.to_string().contains("boom at two"), "branch {i}: {e}"),
            Ok(rows) => panic!("branch {i} ended short instead of failing: {rows:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panic_upstream_of_broadcast_fails_every_branch() {
    assert_every_branch_fails(panics_at_two().broadcast(2, 4)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_upstream_error_through_broadcast_fails_every_branch() {
    assert_every_branch_fails(fails_at_two().broadcast(2, 4)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panic_upstream_of_partition_fails_every_branch() {
    assert_every_branch_fails(panics_at_two().partition(2, 4, |x| *x as u64)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_upstream_error_through_partition_fails_every_branch() {
    assert_every_branch_fails(fails_at_two().partition(2, 4, |x| *x as u64)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_branch_error_keeps_the_upstream_error_as_its_source() {
    use std::error::Error;
    let mut branches = fails_at_two().broadcast(2, 4);
    let other = tokio::spawn(branches.pop().unwrap().collect());
    let err = branches.pop().unwrap().collect().await.unwrap_err();
    let _ = other.await;
    let source = err.source().expect("branch error has a source");
    assert!(source.to_string().contains("boom at two"), "{source}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panic_in_a_background_pipe_fails_the_main_pipe() {
    let main = Pipe::iterate(0i64, |x| x + 1).eval_map(|x| async move {
        tokio::time::sleep(Duration::from_millis(1)).await;
        Ok(x)
    });
    assert_panic_surfaces(main.concurrently(panics_at_two()).take(10_000)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panic_in_a_joined_inner_pipe_fails_the_pipe() {
    let outer = Pipe::from_iter(vec![panics_at_two(), Pipe::once(9i64)]);
    assert_panic_surfaces(outer.par_join(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panic_in_an_unbounded_joined_inner_pipe_fails_the_pipe() {
    let outer = Pipe::from_iter(vec![panics_at_two(), Pipe::once(9i64)]);
    assert_panic_surfaces(outer.par_join_unbounded()).await;
}
