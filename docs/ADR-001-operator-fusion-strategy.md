---
id: ADR-001
title: Operator Fusion Strategy
status: proposed
date: 2026-04-11
status_date: 2026-04-14
summary: Deferred in favor of connectors; revisit after real users
depends_on: [ADR-002]
supersedes: []
superseded_by: []
---

# ADR-001: Operator Fusion Strategy

## Context

Lazyflow's operator composition model uses `Box<dyn PullOperator<B>>` for each
operator in the chain. Every `.map()`, `.filter()`, `.take()` etc. wraps the
parent factory in a new closure that constructs a boxed operator at
materialization time.

For a pipeline like `from_iter(data).map(f).filter(p).take(n).collect()`
processing 100K elements in 256-element chunks (391 chunks), the measured
overhead per chunk is:

- 4 vtable dispatches (one per operator in the stack)
- 2-3 Vec allocations (source chunk + map output; filter is in-place)
- 4 Box::pin future constructions (one per next_chunk call)

This overhead is negligible for I/O-bound or compute-heavy transforms. But
for trivial transform chains (field extraction, arithmetic, predicate
composition), the operator wrapping cost dominates actual work. Profiling
shows that `map(f).map(g)` allocates twice the Vecs of a single
`map(|x| g(f(x)))`, and `filter(p).filter(q)` doubles the empty-chunk
retry loops (up to 32 iterations each before yielding).

The library already has one manual fusion: `and_then()` combines map+filter
into a single operator with a persistent buffer. This validates that fusion
produces real allocation savings.

Three strategies were evaluated:

### Strategy A: Construction-time algebraic rewriting

Detect fusible patterns when operators are chained and replace them with
combined operators. For example, `.map(f).map(g)` detects at construction
time that the parent is a map and produces a single `map(|x| g(f(x)))`.

- **Fusible pairs**: map+map, filter+filter, map+filter (-> and_then),
  take+take (-> min), skip+skip (-> sum)
- **Requires**: Storing operator metadata alongside the factory or using
  an enum-based intermediate representation
- **Complexity**: Moderate. Operator constructors gain pattern-matching
  logic. Requires either type-erasure tricks (Any downcast) or an explicit
  operator tag enum.
- **Gains**: Eliminates 1 vtable dispatch + 1 Vec allocation per fused
  pair, per chunk. For a 10-operator pipeline reduced to 6, saves ~40%
  of per-chunk overhead on trivial transforms.

### Strategy B: Batch operator trait

Add a `BatchOperator<A, B>` trait that receives `Vec<A>` and returns
`Vec<B>`, enabling LLVM autovectorization of the inner loop. Users opt in
via `.batch_map(|chunk| ...)`.

- **Requires**: New trait, new operator method, no changes to existing API
- **Complexity**: Low. Additive -- does not touch existing code paths.
- **Gains**: Autovectorization of numeric/data transforms. Measured 2-8x
  speedup for f64 arithmetic on modern CPUs via SIMD.
- **Limitation**: Only helps CPU-bound synchronous transforms. No benefit
  for async operators.

### Strategy C: Staged compilation with logical plan

Build an explicit operator tree as data (enum nodes), optimize it
(predicate pushdown, fusion, dead branch elimination), then materialize
into a physical operator chain. This is the DataFusion/Spark Catalyst
approach.

- **Requires**: Major architectural change. Logical plan enum, optimizer
  passes, type-erased intermediate representation.
- **Complexity**: High. Loses compile-time type safety in the logical plan
  layer. Requires `Box<dyn Any>` or similar for heterogeneous operator
  chains.
- **Gains**: Full query-engine-class optimizations. Predicate pushdown,
  projection elimination, cross-operator rewriting.
- **Limitation**: Only justified if lazyflow evolves toward a streaming query
  engine competing with DataFusion.

## Decision

Adopt a phased approach: **Strategy A now, Strategy B next, Strategy C
only if the project moves toward streaming analytics.**

### Phase 1: Construction-time algebraic fusion

Introduce an internal `OperatorTag` enum that records the last operator
applied. When a new operator is chained, check if the parent's tag allows
fusion:

```
map(f) + map(g)       -> map(|x| g(f(x)))       [compose closures]
filter(p) + filter(q) -> filter(|x| p(x) && q(x)) [combine predicates]
map(f) + filter(p)    -> and_then(|x| { let y = f(x); p(&y).then(|| y) })
take(m) + take(n)     -> take(min(m, n))
skip(m) + skip(n)     -> skip(m + n)
```

Implementation approach:
- Store an optional `FusionHint` alongside the factory in `Pipe<B>`
- Each operator constructor checks the hint and either fuses or wraps
- Fusion is transparent -- users get it automatically, no API change
- Fallback to normal wrapping when fusion is not applicable

### Phase 2: Batch operator trait

Add `batch_map` and `batch_filter` methods that accept chunk-level
closures. These bypass per-element dispatch and enable LLVM
autovectorization:

```rust
pipe.batch_map(|chunk: Vec<f64>| {
    chunk.into_iter().map(|x| x * 2.0 + 1.0).collect()
})
```

This is additive -- no changes to existing operators or architecture.

### Phase 3: Deferred

Strategy C (staged compilation) is deferred until there is a concrete
use case requiring cross-operator optimization beyond local fusion pairs.
The trigger would be: lazyflow is used as a streaming query engine with
user-defined query plans, not just programmatic pipeline construction.

## Consequences

### Positive

- Phase 1 reduces per-chunk overhead by ~30-50% for trivial transform
  chains (the common case in ETL and data processing)
- Phase 2 unlocks SIMD-friendly batch processing for numeric workloads
- No breaking API changes in any phase
- Fusion is automatic and transparent to users
- Existing tests remain valid -- fused operators must produce identical
  output

### Negative

- Phase 1 adds complexity to operator constructors (pattern matching
  on parent hint). Each fusible operator gains ~10 lines of detection
  logic.
- FusionHint adds one enum field (8 bytes) to the Pipe struct, increasing
  its size from 8 bytes (Arc pointer) to 16 bytes.
- Fused closures capture two closures instead of one, slightly increasing
  per-materialization allocation. But this is paid once vs per-chunk
  savings.
- Phase 2 introduces a parallel API surface (batch_map vs map). Users
  must choose. Documentation must explain when to use which.
- Debug/error messages become less precise when fused operators fail --
  the error traces to the combined operator, not the original user-written
  step.

### Risks

- Fusion correctness: combined operators must be semantically identical
  to the unfused chain. Property-based tests (already in the codebase)
  should verify this for each fusion pair.
- Fusion heuristics may not fire in all cases (e.g., when operators are
  added through Transform composition or dynamic pipeline construction).
  The system must always fall back to correct unfused behavior.
- Phase 1 fusion relies on the factory being the "last" operation.
  Operators added via `.through()` or `.apply()` may break the hint chain.
  Fusion must be best-effort, never required for correctness.

### Alternatives rejected

- **Const-generic chunk sizes** (`Pipe<B, N>`): Infects the entire type
  system with a const parameter. Marginal gains for massive API
  complexity. Not worth it.
- **Replacing Box<dyn> with enums**: Breaks extensibility. Users can't
  implement custom PullOperator if the dispatch is closed.
  The openness of the trait is a feature.
- **Full monomorphization** (generics all the way down): Would require a
  completely different architecture. Loses dynamic composition, the
  clone-and-materialize pattern, and runtime pipeline construction.
  Fundamentally incompatible with lazyflow's design.
- **JIT compilation**: No stable Rust story. Massive complexity for
  marginal gains over batch processing + autovectorization.
