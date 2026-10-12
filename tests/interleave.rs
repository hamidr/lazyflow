//! interleave alternates element by element, whatever the chunking of
//! either source; a source that is merely slow keeps its turn.

mod common;

use common::{CHUNKINGS, in_chunks_of};
use lazyflow::prelude::*;
use std::time::Duration;

/// Element-level round robin, draining the longer side at the end.
fn round_robin(left: &[i32], right: &[i32]) -> Vec<i32> {
    let mut out = Vec::new();
    for i in 0..left.len().max(right.len()) {
        out.extend(left.get(i));
        out.extend(right.get(i));
    }
    out
}

#[tokio::test]
async fn alternation_does_not_depend_on_source_chunking() {
    let cases: [(Vec<i32>, Vec<i32>); 4] = [
        ((0..4).collect(), (10..14).collect()),
        ((0..300).collect(), (1000..1010).collect()),
        ((0..3).collect(), Vec::new()),
        (Vec::new(), (10..13).collect()),
    ];
    for (left, right) in cases {
        for left_chunks in CHUNKINGS {
            for right_chunks in CHUNKINGS {
                let out = in_chunks_of(left.clone(), left_chunks)
                    .interleave(in_chunks_of(right.clone(), right_chunks))
                    .collect()
                    .await
                    .unwrap();
                let context = format!(
                    "{left_chunks:?} / {right_chunks:?}, {} + {}",
                    left.len(),
                    right.len()
                );
                assert_eq!(out, round_robin(&left, &right), "{context}");
            }
        }
    }
}

#[tokio::test]
async fn the_reported_split_alternates_strictly() {
    let right = Pipe::from_iter(10..12).chain(Pipe::from_iter(12..14));
    let out = Pipe::from_iter(0..4)
        .interleave(right)
        .collect()
        .await
        .unwrap();
    assert_eq!(out, [0, 10, 1, 11, 2, 12, 3, 13]);
}

#[tokio::test(start_paused = true)]
async fn a_pending_source_holds_its_turn_instead_of_being_skipped() {
    let left = Pipe::once(0).chain(Pipe::generate(|_tx| std::future::pending()));
    let right = Pipe::from_iter(10..13);
    let pipe = left.interleave(right).take(3).collect();
    let partial = tokio::time::timeout(Duration::from_millis(50), pipe).await;
    assert!(
        partial.is_err(),
        "right ran ahead of a pending left: {partial:?}"
    );
    let first_two = Pipe::once(0)
        .chain(Pipe::generate(|_tx| std::future::pending()))
        .interleave(Pipe::from_iter(10..13))
        .take(2)
        .collect()
        .await
        .unwrap();
    assert_eq!(first_two, [0, 10]);
}

#[tokio::test]
async fn a_source_failure_follows_the_elements_before_it() {
    let failing = Pipe::once(1).chain(Pipe::generate(|_tx| async { Err(PipeError::Timeout) }));
    let results = Pipe::from_iter([0, 2, 4])
        .interleave(failing)
        .attempt()
        .collect()
        .await
        .unwrap();
    let values: Vec<_> = results
        .iter()
        .filter_map(|r| r.as_ref().ok().copied())
        .collect();
    assert_eq!(values, [0, 1, 2]);
    assert!(matches!(results.last(), Some(Err(PipeError::Timeout))));
}
