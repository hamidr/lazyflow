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
