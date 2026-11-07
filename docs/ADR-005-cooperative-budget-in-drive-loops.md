---
id: ADR-005
title: Cooperative Budget in Pull Drive Loops
status: proposed
date: 2026-11-07
status_date: 2026-11-07
summary: One consume_budget helper in drive loops replaces YIELD_AFTER_EMPTY; stops starvation on ready sources
depends_on: []
supersedes: []
superseded_by: []
---

# ADR-005: Cooperative Budget in Pull Drive Loops

**Related**: [Audit findings](audit-findings-and-remediation.md) (cancellation is cooperative; no promise to interrupt non-yielding code)

## Context

A pipe is driven by loops of the form
`while let Some(chunk) = root.next_chunk().await?` (`src/pipeline/terminals.rs`,
`collect_all` and `fold_all` in `src/pull.rs`). An `.await` only gives the
worker back if the future returns `Pending`. A chain of pull operators over an
always-ready source (in-memory rows, a cache) never does, and it touches no
tokio resource, so tokio's cooperative budget never runs out. Bounded mpsc
channels do draw on that budget, so the gap is the plain pull chain.

The only yield today is `YIELD_AFTER_EMPTY`: `PullFilter`, `PullAndThen` and
`PullEvalFilter` call `yield_now` after 32 consecutive empty chunks. A chunk
that is not empty never yields. Other loops have no yield at all: `PullFixpoint`
and `PullFlatMap`, and the operators that drain or discard, such as `PullTopN`,
`PullDrop` and `PullSkipWhile`.

This came up while designing overload protection for knot-server, which runs
queries on tokio workers. A throwaway spike measured the effect at `d0bbd7f`,
tokio 1.54.1, one worker, with the pipe in a spawned task, an always-ready
source of 3M chunks of 256 `u64`, and 3 runs each. "Budgeted" wrapped the source
in `consume_budget`:

| | Unchanged | Budgeted |
|---|---|---|
| 20 ms `tokio::time::timeout` | never fired, returned at about 520 ms | fired at about 20 ms |
| Longest gap for a sibling task | about 520 ms | 2 ms |
| Full drain, no timeout | about 515 ms | about 905 ms |

The drain row overstates the cost, because the spike added a boxed future per
pull. A prototype of this decision put it at about 10 ns per pull on
`from_iter -> collect` (about +6% on 390 chunks). The spike source is not kept;
G1 below replaces it.

## Decision

Add one crate-private helper to `src/pull.rs`, in the shape of:

```rust
pub(crate) async fn pull_next<B: Send + 'static>(
    op: &mut dyn PullOperator<B>,
) -> Result<Option<Vec<B>>, PipeError> {
    tokio::task::coop::consume_budget().await;
    op.next_chunk().await
}
```

Every loop that can pull again without returning to its caller uses it:

- The terminal loops in `terminals.rs`, plus `collect_all` and `fold_all`
  (`PullFoldCollect` drains through `collect_all`).
- The drain loops: `drain_to_writer` in `io.rs`, `concurrently` in `concurrency.rs`, and
  `start_partition` in `pull_ops.rs`, which skips its send on an empty chunk.
- The operators that discard: `PullFilter`, `PullAndThen`, `PullEvalFilter`,
  `PullFlatMap`, `PullFlatten`, `PullFixpoint`, `PullDrop`, `PullSkipWhile`,
  `PullUnNoneTerminate`, and the line splitter in `io.rs`.
- The operators that accumulate: `PullTopN`, `PullChunks`, `PullSlidingWindow`,
  `PullGroupAdjacentBy`, `PullThrottle`, `PullZip`.

Loops that send on a bounded channel on every pass (`drain_to_channel`, the
`stream.rs` drain, `start_broadcast`) stay as they are, because the send already
draws budget. A `prefetch` case passes at `d0bbd7f`, which confirms it for
`drain_to_channel`.

Delete `YIELD_AFTER_EMPTY` and every `empty_runs` counter, so one mechanism
replaces two. Raise the root tokio requirement from `"1"` to `"1.44"`, the
release that added `task::coop`. The older `tokio::task::consume_budget` path is
deprecated.

**Rejected alternatives**

- Per-operator yields in the consumer (`knot-exec`): a consumer cannot reach
  lazyflow's terminal loops, and every other consumer keeps the defect.
- `yield_now` every N chunks: a second budget beside tokio's, and it yields even
  when the task's budget has room.
- Event-driven `CancelToken` (wake a blocked `PullCancel` at once): knot-server
  already drops the query from `tokio::time::timeout`, once the budget lets it
  run.
- `fold_collect_blocking` over `spawn_blocking`: covers only whole-buffer
  transforms. knot's join probe and adjacency build are not that shape, so it
  needs its own offload anyway.
- A dedicated CPU pool in lazyflow: scheduling policy belongs to the consumer.
  It adds a dependency and cannot run the async pull chain.
- Streaming or byte-bounded channels: no measured need. Internal `generate`
  channels already hold at most 2 chunks.
- Requiring a knot workload before accepting: the defect is shown on lazyflow's pull
  protocol and hits every ready source, so a consumer's workload is not the test.
- A percentage limit on G2 (5% or 10%): it punishes benchmarks whose chunks do almost
  no work, and on real chunks a 10% regression would be large and still pass.

## Consequences

- This stands on lazyflow's own pull protocol. The spike source was synthetic, and
  whether a real knot plan reaches an always-ready path is left to knot's overload
  design, whose stall bench measures the gain. TiKV and redb reads do suspend, so
  knot may gain little and still pay the per-pull cost.
- A long pipe on a ready source no longer starves its worker, and `timeout` and
  `select!` around it fire. This needs no consumer change.
- `pull_next` is crate-private, so a consumer's own `PullOperator` with an inner
  loop, or a hand-written loop over `into_pull`, is not covered. knot has one
  custom operator (`LazyScanSource`) and no such loop.
- Each drive-loop pull pays one `consume_budget` call, plus a real yield once the
  budget runs out. Trivial chunks pay the most (see the table).
- Filters that discard everything now yield when the budget runs out, not every
  32 empties, so the interval between yields changes.
- The budget counts pulls, not work. The stall is bounded by about one budget's
  worth of chunks (128 in tokio's `Budget::initial`), so slow chunks stall for
  longer. A chain of looping operators spends several units per chunk, so it
  yields sooner.
- A single long synchronous step is still uninterruptible: one closure call, one
  sort, one huge chunk. That stays the consumer's job, as the audit document
  already says.
- A pipe run under `tokio::task::unconstrained` opts out, which is what that API
  is for.

## Measurement plan

- **G1, no starvation.** New `tests/cooperative_budget.rs`, on a `current_thread`
  runtime with a finite always-ready source. A nearly endless source hangs at
  `d0bbd7f` instead of failing, because the guard timer shares the thread. A
  sibling task spawned beside each driver runs within 1000 source pulls. Drivers:
  `collect`, `fold`, `try_fold_chunks`, `filter(|_| false)` under `collect` and
  `first`, `fixpoint` over repeated values, `top_n`, `fold_collect`, `skip`,
  `skip_while`, `chunks`, `prefetch`. A 20 ms `timeout` around the source returns
  `Err`. `cargo test --test cooperative_budget`. Pass: all pass after. At
  `d0bbd7f` only the two `filter` cases and `prefetch` pass.
- **G2, cost.** At `d0bbd7f`: `cargo bench --bench operators -- --save-baseline pre`.
  After the change: `cargo bench --bench operators -- --baseline pre`. Back at
  `d0bbd7f`, repeat the second command as a noise check. Run on an idle machine; a
  repeat that moves more than 3% voids the run. Pass: each benchmark's added time,
  divided by its `pull_next` calls per iteration, stays under 20 ns. Count the calls
  once with a temporary counter in the helper.
- **G3, where flat_map and fixpoint charge.** Run G2 twice: `PullFlatMap`, `PullFlatten`
  and `PullFixpoint` charging on every pull, then only when a pull produces nothing.
  Earlier runs are not decisive. With a 3% noise check, every pull cost about +13% on
  `flat_map` and +7 to +8% on `fixpoint chain`, about 6 ns per estimated pull. On a
  loaded machine the second form suggested about +5.5% and +3 to +7%, but that run moved
  8 to 11% on its noise check. Decide: keep every pull if it passes G2, since one form
  is simpler; otherwise use the second form.
- **G4, chunk weight.** Measure the longest sibling-task gap over an always-ready source
  with chunks of 256, 4096 and 65536 rows, one worker, one `consume_budget` per pull.
  Decide: keep one unit per pull unless a chunk size lazyflow's own operators emit
  stalls a sibling for more than 10 ms; then charge once per N rows.
- **G5, one mechanism.** `rg -n 'YIELD_AFTER_EMPTY|empty_runs' src`. Pass: no hits.
- **G6, tokio floor.** `cargo update -p tokio --precise 1.44.0 && cargo check -p lazyflow`,
  then restore `Cargo.lock`. Pass: it builds. If another dependency refuses 1.44.0,
  raise the floor to the lowest version that resolves and builds.
- **G7, project gates.** `scripts/gate.sh`. Pass: prints `gate: ok`.
- **G8, consumer.** In knot, with an uncommitted
  `[patch."https://github.com/hamidr/lazyflow.git"]` pointing at this tree:
  `cargo test -p knot-exec -p knot-client`, then restore `Cargo.toml` and
  `Cargo.lock`. Pass: same results as without the patch.
