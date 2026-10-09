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
        Ok(rows) => panic!("panic became a short stream: {rows:?}"),
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
