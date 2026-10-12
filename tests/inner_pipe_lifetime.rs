//! Inner pipes started by par_join, par_join_unbounded and switch_map
//! must not outlive the pipe that started them.

use lazyflow::prelude::*;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

const SLOW: Duration = Duration::from_millis(100);

/// One fast inner pipe and one that records a side effect after `SLOW`.
fn inner_pipes(completed: Arc<AtomicBool>) -> Pipe<Pipe<i32>> {
    let slow = Pipe::once(1).eval_map(move |x| {
        let completed = Arc::clone(&completed);
        async move {
            tokio::time::sleep(SLOW).await;
            completed.store(true, Ordering::SeqCst);
            Ok(x)
        }
    });
    Pipe::from_iter(vec![Pipe::once(0), slow])
}

async fn assert_cancelled(
    first: impl std::future::Future<Output = Result<Option<i32>, PipeError>>,
    completed: Arc<AtomicBool>,
) {
    assert_eq!(first.await.unwrap(), Some(0));
    tokio::time::sleep(SLOW * 2).await;
    assert!(
        !completed.load(Ordering::SeqCst),
        "an inner pipe ran on after the consumer stopped"
    );
}

#[tokio::test(start_paused = true)]
async fn par_join_stops_inner_pipes_when_the_consumer_stops() {
    let completed = Arc::new(AtomicBool::new(false));
    let pipe = inner_pipes(Arc::clone(&completed)).par_join(2);
    assert_cancelled(pipe.first(), completed).await;
}

#[tokio::test(start_paused = true)]
async fn par_join_unbounded_stops_inner_pipes_when_the_consumer_stops() {
    let completed = Arc::new(AtomicBool::new(false));
    let pipe = inner_pipes(Arc::clone(&completed)).par_join_unbounded();
    assert_cancelled(pipe.first(), completed).await;
}

#[tokio::test(start_paused = true)]
async fn switch_map_stops_the_inner_pipe_when_the_consumer_stops() {
    let completed = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&completed);
    let pipe = Pipe::once(0).switch_map(move |x| {
        let flag = Arc::clone(&flag);
        let slow = Pipe::once(1).eval_map(move |x| {
            let flag = Arc::clone(&flag);
            async move {
                tokio::time::sleep(SLOW).await;
                flag.store(true, Ordering::SeqCst);
                Ok(x)
            }
        });
        Pipe::once(x).chain(slow)
    });
    assert_cancelled(pipe.first(), completed).await;
}
