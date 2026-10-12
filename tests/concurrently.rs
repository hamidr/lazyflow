//! A background failure ends the composition even while the main pipe
//! has nothing to deliver; a background success leaves the main pipe be.

use lazyflow::prelude::*;
use std::time::Duration;

const PROMPTLY: Duration = Duration::from_millis(100);

async fn promptly<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(PROMPTLY, f)
        .await
        .expect("the composition stalled")
}

fn never_ready<T: Send + 'static>() -> Pipe<T> {
    Pipe::generate(|_tx| std::future::pending())
}

fn fails_after(delay: Duration) -> Pipe<i32> {
    Pipe::generate(move |_tx| async move {
        tokio::time::sleep(delay).await;
        Err(PipeError::Timeout)
    })
}

#[tokio::test(start_paused = true)]
async fn a_background_failure_interrupts_a_main_pull_that_is_pending() {
    let pipe = never_ready::<i32>().concurrently(fails_after(Duration::from_millis(10)));
    let result = promptly(pipe.collect()).await;
    assert!(matches!(result, Err(PipeError::Timeout)), "{result:?}");
}

#[tokio::test(start_paused = true)]
async fn a_background_failure_keeps_items_the_main_pipe_already_delivered() {
    let main = Pipe::once(1).chain(never_ready());
    let pipe = main.concurrently(fails_after(Duration::from_millis(10)));
    let results = promptly(pipe.attempt().collect()).await.unwrap();
    assert_eq!(results[0].as_ref().unwrap(), &1);
    assert!(matches!(results[1], Err(PipeError::Timeout)));
    assert_eq!(results.len(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_background_success_lets_a_slower_main_pipe_finish() {
    let main = Pipe::from_iter([1, 2, 3]).eval_map(|x| async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        Ok(x)
    });
    let pipe = main.concurrently(Pipe::once(0));
    assert_eq!(promptly(pipe.collect()).await.unwrap(), vec![1, 2, 3]);
}
