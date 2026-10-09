//! Failures a caller handles differently get their own `PipeError`
//! variant, so a caller matches on the variant rather than the message.

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_producer_panic_is_a_panicked_error_carrying_the_message() {
    let err = panics_at_two().prefetch(1).collect().await.unwrap_err();
    assert!(
        matches!(&err, PipeError::Panicked(m) if m.contains("boom at two")),
        "{err:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_worker_panic_is_a_panicked_error() {
    let err = Pipe::from_iter(vec![1i64, 2])
        .par_eval_map(2, |x| async move {
            if x == 2 {
                panic!("boom at two");
            }
            Ok(x)
        })
        .collect()
        .await
        .unwrap_err();
    assert!(matches!(err, PipeError::Panicked(_)), "{err:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_fan_out_branch_sees_the_same_variant() {
    for branch in panics_at_two().broadcast(2, 4) {
        let err = tokio::spawn(branch.collect()).await.unwrap().unwrap_err();
        assert!(matches!(err, PipeError::Panicked(_)), "{err:?}");
    }
}

#[tokio::test]
async fn a_pull_past_its_deadline_is_a_timeout_error() {
    let slow: Pipe<i64> = Pipe::generate(|_tx| async move {
        tokio::time::sleep(Duration::from_secs(5)).await;
        Ok(())
    });
    let err = slow
        .timeout(Duration::from_millis(5))
        .collect()
        .await
        .unwrap_err();
    assert!(matches!(err, PipeError::Timeout), "{err:?}");
}

fn endless() -> Pipe<i64> {
    Pipe::iterate(0i64, |x| x + 1).eval_map(|x| async move {
        tokio::time::sleep(Duration::from_millis(1)).await;
        Ok(x)
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_background_failure_reaches_the_main_pipe_as_the_same_variant() {
    let background: Pipe<i64> = Pipe::generate(|_tx| async move { Err(PipeError::Timeout) });
    let err = endless()
        .concurrently(background)
        .collect()
        .await
        .unwrap_err();
    assert!(matches!(err, PipeError::Timeout), "{err:?}");
}

#[derive(Debug)]
struct Disk;
impl std::fmt::Display for Disk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("disk full")
    }
}
impl std::error::Error for Disk {}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_background_custom_error_stays_reachable_through_source() {
    use std::error::Error;
    let background: Pipe<i64> =
        Pipe::generate(|_tx| async move { Err(PipeError::Custom(Box::new(Disk))) });
    let err = endless()
        .concurrently(background)
        .collect()
        .await
        .unwrap_err();
    let mut cause: Option<&(dyn Error + 'static)> = Some(&err);
    let mut found = false;
    while let Some(c) = cause {
        found |= c.is::<Disk>();
        cause = c.source();
    }
    assert!(found, "the original error is in the source chain: {err:?}");
}
