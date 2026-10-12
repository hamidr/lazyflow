# Streaming models

Models supplement executable Rust regressions; they do not prove the implementation correct.

## Timed batch pull

`timed_batch_pull.qnt` constrains `Pipe::chunks_timeout` in `src/pipeline/time.rs`.
An upstream pull can consume data before completing. Flushing a timed batch must
preserve that pending pull, even across multiple deadlines.

Checked with Quint 0.32.0:

```bash
quint typecheck models/timed_batch_pull.qnt
quint run models/timed_batch_pull.qnt --invariants=conserved --max-steps=30 --max-samples=10000 --backend=typescript
```

Result: no conservation violation in 10,000 sampled traces of up to 30 steps,
with three source items and at most one in-flight item. This is randomized
simulation, not exhaustive verification. The design retains the pending future
across timer ticks; only completion starts another pull.

Negative control models the old behavior, dropping the pending future on a timer:

```bash
quint run models/timed_batch_pull.qnt --step=buggyStep --invariants=conserved --max-steps=30 --seed=0x134ac84b5a6037 --backend=typescript
```

Expected result: invariant violation (nonzero exit). The recorded trace consumes
two items, buffers one, then flushes it while discarding the second pending item:
`consumed=2, pending=0, buffered=0, emitted=1`.

The model abstracts away payload ordering, output-channel capacity, whole-pipeline
cancellation, and errors. `tests/time.rs` covers concrete timer composition with
delayed data/errors, resource acquisition, and cancellation/finalization.
