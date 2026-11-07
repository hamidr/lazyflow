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
that is not empty never yields, and `PullFixpoint` and `PullFlatMap` have loops
with no yield at all.

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

The drain cost is about 130 ns per chunk, including a boxed future the spike
added. It is a worst case, because these chunks cost only about 170 ns each. The
spike source is not kept; G1 below replaces it.

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

- The terminal loops in `terminals.rs`, plus `collect_all` and `fold_all`.
- The drain loops in `io.rs` and `concurrency.rs` (`concurrently`).
- The operators that discard and pull again: `PullFilter`, `PullAndThen`,
  `PullEvalFilter`, `PullFlatMap`, `PullFixpoint`.

Delete `YIELD_AFTER_EMPTY` and every `empty_runs` counter, so one mechanism
replaces two. Raise the root tokio requirement from `"1"` to `"1.44"`, the
release that added `task::coop`.

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

## Consequences

- A long pipe on a ready source no longer starves its worker, and `timeout` and
  `select!` around it fire. This needs no consumer change.
- Each drive-loop pull pays one `consume_budget` call, plus a real yield once the
  budget runs out. Trivial chunks pay the most (see the table).
- Filters that discard everything now yield when the budget runs out, not every
  32 empties, so the interval between yields changes.
- The budget counts pulls, not work. The stall is bounded by about one budget's
  worth of chunks, so slow chunks stall for longer.
- A single long synchronous step is still uninterruptible: one closure call, one
  sort, one huge chunk. That stays the consumer's job, as the audit document
  already says.
- A pipe run under `tokio::task::unconstrained` opts out, which is what that API
  is for.

## Measurement plan

- **G1, no starvation.** New `tests/cooperative_budget.rs`, using an always-ready
  source and no wall-clock thresholds on the sibling check. A sibling task spawned
  beside each driver (`collect`, `fold`, `try_fold_chunks`, an all-discarding
  `filter`, `fixpoint`) runs within 1000 source pulls. A 20 ms `timeout` around a
  source far longer than that returns `Err`.
  `cargo test --test cooperative_budget`. Pass: fails at `d0bbd7f`, passes after.
- **G2, cost.** At `d0bbd7f`: `cargo bench --bench operators -- --save-baseline pre`.
  After the change: `cargo bench --bench operators -- --baseline pre`.
  Pass: no benchmark's estimate regresses by more than 5%.
- **G3, one mechanism.** `rg -n 'YIELD_AFTER_EMPTY|empty_runs' src`. Pass: no hits.
- **G4, tokio floor.** `cargo update -p tokio --precise 1.44.0 && cargo check -p lazyflow`,
  then restore `Cargo.lock`. Pass: it builds. If another dependency refuses 1.44.0,
  raise the floor to the lowest version that resolves and builds.
- **G5, project gates.** `scripts/gate.sh`. Pass: prints `gate: ok`.
- **G6, consumer.** In knot, with an uncommitted
  `[patch."https://github.com/hamidr/lazyflow.git"]` pointing at this tree:
  `cargo test -p knot-exec`. Pass: same results as without the patch.

## Open questions

1. Does a real knot plan reach an always-ready path, and how much does this gain
   there? The spike source was synthetic. TiKV and redb reads do suspend, so the
   candidates are fixpoint rounds and materialized rows.
2. Does `drain_to_channel` (and the `stream.rs` drain) need the helper? Its
   `tx.send` should already draw budget, which is why it is left out above. Add a
   `prefetch` case to G1 and include it only if that case fails.
3. Is 5% the right limit for G2 on benchmarks whose chunks do almost no work?
4. Should the budget charge by chunk size, so a very large chunk costs more than
   one unit? `consume_budget` takes no weight, so this means several calls per
   chunk. Left out until a measurement asks for it.
