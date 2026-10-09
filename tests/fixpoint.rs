//! `Pipe::fixpoint`: semi-naive iteration to a fixpoint.

use lazyflow::prelude::*;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

/// a -> b -> c -> a is a cycle, c -> d leaves it.
const EDGES: &[(u8, u8)] = &[(b'a', b'b'), (b'b', b'c'), (b'c', b'a'), (b'c', b'd')];

type Deltas = Vec<(u8, u8)>;

fn edges() -> Pipe<(u8, u8)> {
    Pipe::from_iter(EDGES.to_vec())
}

/// Linear closure: extend each newly found path by one edge.
fn extend_by_an_edge(round: Round<(u8, u8)>) -> Pipe<(u8, u8)> {
    let next: Vec<(u8, u8)> = round
        .delta()
        .iter()
        .flat_map(|&(x, y)| {
            EDGES
                .iter()
                .filter(move |e| e.0 == y)
                .map(move |e| (x, e.1))
        })
        .collect();
    Pipe::from_iter(next)
}

fn closure_of_edges() -> BTreeSet<(u8, u8)> {
    let mut all: BTreeSet<(u8, u8)> = EDGES.iter().copied().collect();
    loop {
        let more: Vec<(u8, u8)> = all
            .iter()
            .flat_map(|&(x, y)| {
                EDGES
                    .iter()
                    .filter(move |e| e.0 == y)
                    .map(move |e| (x, e.1))
            })
            .collect();
        let before = all.len();
        all.extend(more);
        if all.len() == before {
            return all;
        }
    }
}

#[tokio::test]
async fn a_cyclic_closure_reaches_its_fixpoint_with_each_item_once() {
    let out = edges().fixpoint(extend_by_an_edge).collect().await.unwrap();
    let set: BTreeSet<_> = out.iter().copied().collect();
    assert_eq!(set.len(), out.len(), "no item is emitted twice: {out:?}");
    assert_eq!(set, closure_of_edges());
}

#[tokio::test]
async fn each_round_sees_only_the_items_new_in_the_round_before() {
    let deltas: Arc<Mutex<Vec<Deltas>>> = Arc::default();
    let seen = Arc::clone(&deltas);
    let out = edges()
        .fixpoint(move |round: Round<(u8, u8)>| {
            seen.lock().unwrap().push(round.delta().to_vec());
            extend_by_an_edge(round)
        })
        .collect()
        .await
        .unwrap();
    let deltas = deltas.lock().unwrap();
    assert_eq!(deltas[0], EDGES.to_vec(), "round 1 reads the seed");
    let total: usize = deltas.iter().map(Vec::len).sum();
    assert_eq!(total, out.len(), "every item is a delta exactly once");
}

#[tokio::test]
async fn a_round_reads_every_item_found_so_far_through_full() {
    // Non-linear closure: join new paths with all paths, both ways.
    let out = edges()
        .fixpoint(|round: Round<(u8, u8)>| {
            let full: Vec<(u8, u8)> = round.full().copied().collect();
            let mut next = Vec::new();
            for &(x, y) in round.delta() {
                for &(p, q) in &full {
                    if p == y {
                        next.push((x, q));
                    }
                    if q == x {
                        next.push((p, y));
                    }
                }
            }
            Pipe::from_iter(next)
        })
        .collect()
        .await
        .unwrap();
    let set: BTreeSet<_> = out.iter().copied().collect();
    assert_eq!(set.len(), out.len());
    assert_eq!(set, closure_of_edges());
}

#[tokio::test]
async fn an_empty_seed_never_runs_a_round() {
    let rounds = Arc::new(Mutex::new(0));
    let r = Arc::clone(&rounds);
    let out = Pipe::<(u8, u8)>::empty()
        .fixpoint(move |round| {
            *r.lock().unwrap() += 1;
            extend_by_an_edge(round)
        })
        .collect()
        .await
        .unwrap();
    assert!(out.is_empty());
    assert_eq!(*rounds.lock().unwrap(), 0);
}

#[tokio::test]
async fn a_bounded_fixpoint_runs_at_most_that_many_rounds() {
    let out = Pipe::once(0u64)
        .fixpoint_bounded(3, |round: Round<u64>| {
            Pipe::from_iter(round.delta().iter().map(|n| n + 1).collect::<Vec<_>>())
        })
        .collect()
        .await
        .unwrap();
    assert_eq!(out, vec![0, 1, 2, 3]);
}

#[tokio::test]
async fn round_numbers_count_from_one() {
    let numbers: Arc<Mutex<Vec<usize>>> = Arc::default();
    let n = Arc::clone(&numbers);
    Pipe::once(0u64)
        .fixpoint_bounded(2, move |round: Round<u64>| {
            n.lock().unwrap().push(round.number());
            Pipe::once(round.number() as u64)
        })
        .collect()
        .await
        .unwrap();
    assert_eq!(*numbers.lock().unwrap(), vec![1, 2]);
}

#[tokio::test]
async fn an_error_in_a_round_fails_the_pipe() {
    let result = edges()
        .fixpoint(|round: Round<(u8, u8)>| {
            if round.number() == 2 {
                Pipe::from_iter(vec![Err::<(u8, u8), _>(())]).eval_map(
                    |r: Result<(u8, u8), ()>| async move {
                        r.map_err(|_| PipeError::Custom("boom in round two".into()))
                    },
                )
            } else {
                extend_by_an_edge(round)
            }
        })
        .collect()
        .await;
    let err = result.expect_err("a failing round fails the pipe");
    assert!(err.to_string().contains("boom in round two"), "{err}");
}

#[tokio::test]
async fn the_pipe_is_a_blueprint_and_runs_again_from_scratch() {
    let pipe = edges().fixpoint(extend_by_an_edge);
    let a: BTreeSet<_> = pipe.clone().collect().await.unwrap().into_iter().collect();
    let b: BTreeSet<_> = pipe.collect().await.unwrap().into_iter().collect();
    assert_eq!(a, b);
}

static HASHERS_BUILT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Counts the hashers it builds, to show the fixpoint dedups through it.
#[derive(Default)]
struct CountingState(std::collections::hash_map::RandomState);

impl std::hash::BuildHasher for CountingState {
    type Hasher = std::collections::hash_map::DefaultHasher;
    fn build_hasher(&self) -> Self::Hasher {
        HASHERS_BUILT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.0.build_hasher()
    }
}

#[tokio::test]
async fn a_caller_chosen_hasher_dedups_with_the_same_result() {
    let before = HASHERS_BUILT.load(std::sync::atomic::Ordering::Relaxed);
    let out = edges()
        .fixpoint_with_hasher::<CountingState>(None, extend_by_an_edge)
        .collect()
        .await
        .unwrap();
    let set: BTreeSet<_> = out.iter().copied().collect();
    assert_eq!(set.len(), out.len());
    assert_eq!(set, closure_of_edges());
    assert!(HASHERS_BUILT.load(std::sync::atomic::Ordering::Relaxed) > before);
}

#[tokio::test]
async fn a_caller_chosen_hasher_honours_a_round_bound() {
    let out = Pipe::once(0u64)
        .fixpoint_with_hasher::<CountingState>(Some(2), |round: Round<u64>| {
            Pipe::from_iter(round.delta().iter().map(|n| n + 1).collect::<Vec<_>>())
        })
        .collect()
        .await
        .unwrap();
    assert_eq!(out, vec![0, 1, 2]);
}
