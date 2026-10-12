# Lazyflow audit findings and remediation plan

## Executive summary

Audit baseline: commit `10ef892`, version `0.12.0`.

The factory-based `Pipe` design, extensible `PullOperator` protocol, and separate
connector crates are sound foundations. The main problems are execution semantics:
internal chunk boundaries change public results, timer selection cancels unfinished
work, and task ownership does not consistently follow pipeline lifetime.

The audit identified **16 behavior/API findings and one resource-hardening gap**.
Each finding below has a runtime or compiler artifact. H1 demonstrates a missing
resource bound, not an actual out-of-memory crash. F9 is a demonstrated live-stream
latency hazard whose intended buffering contract should be clarified.

**Recommendation:** repair execution and lifecycle guarantees before pursuing
operator fusion or additional API surface. Use one focused TDD change per finding,
with regression coverage retained in the repository.

### Current implementation status

All 16 findings and H1 are fixed on `fix/audit-streaming`, one commit per
finding (F4 has a preceding behavior-preserving refactor), each with its
regression tests. The audit proof suites from the proof worktree pass against
the branch: 14 of 14 core proofs, 5 of 5 connector proofs, and the F16
compiler example builds and runs. The workspace gate (`scripts/gate.sh`:
fmt, clippy with warnings denied, all-features tests, rustdoc with warnings
denied) passes with 406 tests, 36 ignored.

Contract choices made while fixing: F9 treats the stream chunk size as a
maximum; F13 raises size 0 to 1; F14 counts line content without `\n` or
`\r\n`; H1 adds `SseConfig::max_event_size`, default 16 MiB, and does not
reconnect after a size violation. F2 now reports an upstream failure after
the results of earlier items, and F6 ties `WsSender` to the incoming pipe's
lifetime. `CHANGELOG.md` records the externally visible changes.

Follow-ups beyond the reported findings: `debounce` had the F1 fault (its
quiet timer dropped the pending upstream pull) and is fixed the same way.
`par_join`, `par_join_unbounded` and `switch_map` hold their inner tasks by
bare handles but were verified to stop when the consumer goes away, since
each inner task drains into a channel and quits once it closes;
`tests/inner_pipe_lifetime.rs` pins that.

Source line references below refer to the audited commit; the fixes shift
them.

## Validation and evidence

| Check | Audited baseline | Local F1 implementation |
| --- | --- | --- |
| Workspace tests, all features | 331 passed, 36 ignored | 337 passed, 36 ignored |
| Clippy, all targets and features, warnings denied | Passed | Passed |
| Formatting | Passed | Passed |
| Workspace rustdoc, all features, warnings denied | Passed | Passed |

The initial default-feature suite passed 330 tests. Tests ran with Rust 1.98.1.
An installed `protoc` binary was added to the build environment for gRPC tests;
its initial absence was an environment issue, not a library defect.

The isolated audit added:

- 14 failing core behavioral regression tests.
- Four failing connector behavioral regression tests.
- One resource-limit probe demonstrating H1.
- One compiler-failure example demonstrating F16.

These assertions describe the desired behavior and intentionally fail against
`10ef892`. They are separate from the passing baseline suite.

Local proof worktree, not part of this repository's tracked files:

```text
/Users/hamid/.cache/lazyflow-audit-10ef892/
  AUDIT.md
  tests/audit_regressions.rs
  lazyflow-http/tests/audit_regressions.rs
  examples/audit_macro_async.rs
  core-proof.log
  http-proof.log
  macro-proof.log
```

From that worktree, reproduce with:

```bash
cargo test -p lazyflow --test audit_regressions -- --nocapture
cargo test -p lazyflow-http --test audit_regressions -- --nocapture
cargo check -p lazyflow --example audit_macro_async
```

All three commands are expected to fail at the audit baseline. The compiler-proof
example is deliberately invalid after macro expansion; do not copy it unchanged
into normal all-targets CI. Convert it into a positive compilation regression as
part of F16.

## Design principles for the fixes

1. **Keep concerns separate.** Source pulling, batching, output ordering, task
   lifetime, and wire parsing should have explicit responsibilities. Avoid a
   monolithic replacement streaming engine.
2. **Preserve composition.** A buffering or timing operator must not assume its
   upstream is cancellation-safe unless the protocol explicitly requires it.
   Existing built-in upstream operators are not universally cancellation-safe.
3. **Keep state at the correct lifetime.** Batch remainders, termination flags,
   pending pulls, and resume checkpoints must survive the events that should not
   reset them.
4. **Own background work.** Pipeline shutdown must reach every task it owns, not
   just a coordinator. Cancellation is cooperative; do not promise interruption
   of arbitrary blocking or non-yielding user code.
5. **Bound all retained work.** Bounded channels alone do not bound completed job
   queues, reorder buffers, source chunks, or unfinished network messages.
6. **Reuse demonstrated abstractions.** Prefer the existing pull protocol and
   lifecycle mechanisms. Extract small shared helpers where ownership semantics
   genuinely repeat; avoid speculative frameworks or new public APIs.
7. **Test observable contracts.** Check values, ordering, timing at the consumer,
   remote connection shutdown, and side effects, not merely total runtime or
   element counts.
8. **Separate fixes from contract changes.** Clarify F9's buffering semantics,
   F13's zero-size policy, F14's length definition, and H1's configurable limits
   before changing externally visible behavior.

## Findings and applicable solutions

### F1: Timer batching cancels unfinished upstream work

**Priority:** high. **Status:** fixed.

**Location:** `src/pipeline/time.rs:140-174`.

**Observed:** a source yielding item 1 immediately and item 2 after 100 ms,
composed with a 20 ms batch timer, returns `[[1]]` instead of `[[1], [2]]`.
Additional TDD regressions showed delayed errors being swallowed and slow resource
acquisition being cancelled.

**Cause:** when the timer wins `select!`, the temporary upstream pull future is
dropped. That future can already own consumed data or acquisition state.

**Applied solution:** create one upstream pull future and retain it across timer
wins. Timer ticks flush the accumulated batch and restart only the timer. A new
pull begins only after the current pull completes. Whole-pipeline drop still
cancels pending work through the existing producer ownership.

This solution adds no producer task, channel, dependency, or public API.

**Regression coverage:** six tests in `tests/time.rs` cover repeated deadlines,
delayed typed errors, resource acquisition/release, timely partial flushing,
downstream drop/finalization, count batches, and empty input. Three regressions
failed before the implementation; all six now pass.

**Model:** `models/timed_batch_pull.qnt` checks item conservation. Quint simulation
found no violation across 10,000 sampled traces of 30 steps, with three source
items and one pending item. The negative control reproduces lost data. This is
bounded randomized simulation, not proof of the implementation or liveness.
Commands and model limitations are recorded in `models/README.md`.

### F2: Ordered parallel-map scheduling stalls output and overproduces

**Priority:** high.

**Location:** `src/pipeline/async_ops.rs:96-138`.

**Observed:**

- After a source chunk `[1, 2]`, a never-ready next source pull prevents
  `par_eval_map(2).take(2)` from returning both completed results.
- Calling `.first()` over 1,024 input elements executed 284 callbacks in recorded
  runs despite concurrency 2. The exact count is scheduler-dependent.

**Cause:** the coordinator schedules a whole source chunk, emits one result,
then fetches another source chunk before draining its completed-result queue.
The semaphore bounds active work, not retained results or work ahead of demand.

**Proposed solution:** introduce a bounded ordered in-flight window. Refill it
only when capacity is available, and let completed output make progress
independently of upstream readiness. Preserve the current source pull if racing
it with worker completion; otherwise the F1 cancellation error can recur here.
Bound active jobs and retained completed results together. Account separately for
one already-pulled source chunk and the bounded output channel.

**Tests:**

- Two completed outputs followed by pending upstream must both arrive.
- A slow first job with fast later jobs must preserve order without unbounded
  accumulation.
- Active callbacks must never exceed configured concurrency.
- Work performed before `.first()` returns must stay within a documented bound
  derived from the in-flight window and output capacity, not total source size.
- Test concurrency 1, multiple source chunks, upstream failure, and worker failure.

**Proofs:** `ordered_parallel_map_emits_buffered_results_before_next_source_chunk`,
`ordered_parallel_map_bounds_work_ahead_of_consumer`.

### F3: Ordered parallel-map workers outlive the pipeline

**Priority:** high.

**Location:** `src/pipeline/async_ops.rs:96-125`; coordinator-only abort in
`src/channel.rs:125-129`.

**Observed:** a second worker performs its delayed side effect after `.first()`
returns and the pipeline has been dropped.

**Cause:** dropping the coordinator drops its worker `JoinHandle`s. In Tokio,
dropping a `JoinHandle` detaches its task; it does not abort it.

**Proposed solution:** make worker cancellation part of coordinator ownership.
Use an owning task collection or a small abort-on-drop owner covering every
spawned worker, including one currently being awaited. Ensure ownership survives
queue removal and handles normal completion, downstream drop, errors, and panics.
An error should not leave unrelated workers detached.

Keep output ordering separate from lifetime management so F2 and F3 remain
independently understandable and testable. Complete F3 before a larger F2
scheduler refactor where practical.

**Tests:** synchronize worker startup, cancel downstream, and verify pending
worker futures are dropped and cannot perform later side effects. Include
error/panic exits and removal of a job from the pending queue.

**Proof:** `ordered_parallel_map_cancels_workers_on_drop`.

### F4: Dropped unmaterialized fan-out branches stall surviving branches

**Priority:** high.

**Location:** `src/pipeline/pull_ops.rs:442-486,523-583` and branch factories in
`src/pipeline/concurrency.rs`.

**Observed:** create two broadcast or partition branches with buffer 1, drop one
without consuming it, then consume the survivor. The survivor times out.

**Cause:** lazy shared state retains the unclaimed receiver. Dropping its branch
blueprint does not close that receiver, so the producer blocks on a queue with no
possible consumer.

**Proposed solution:** give each logical branch an ownership registration whose
lifetime begins when branches are created, not when they materialize. Dropping
the last blueprint/consumer owner of a branch must remove or close its receiver.
Initialization must skip already-dropped branches. Avoid ownership cycles between
branch registrations and the shared dispatcher.

Preserve the distinction between an existing but idle branch, which should exert
backpressure, and a dropped branch, which should not. Define cloned-branch
behavior consistently with the existing single-use fan-out receivers.

**Tests:** drop before initialization, after initialization but before that
branch is consumed, midway through consumption, and after source failure. Cover
both broadcast and partition; ensure the source stops when all branches drop.

**Proofs:** `dropped_unmaterialized_broadcast_branch_does_not_stall`,
`dropped_unmaterialized_partition_branch_does_not_stall`.

### F5: Background failure cannot interrupt a blocked main pull

**Priority:** high.

**Location:** `src/pipeline/pull_ops.rs:1027-1035`.

**Observed:** a never-ready main source remains blocked after the background
source returns `PipeError::Timeout`.

**Cause:** `concurrently()` checks the background watch value only before
awaiting the main pull. No background-failure notification wakes that wait.

**Proposed solution:** observe background state changes while awaiting the main
pull. Preserve the same main future across nonterminal notifications. On actual
background failure, terminate the composition and propagate the original typed
error. On successful background completion, continue the main stream without
repeatedly polling a closed notification channel.

**Tests:** background error while main is pending; background success while main
continues; main completion cancels background; simultaneous readiness; downstream
drop; preservation of typed errors.

**Proof:** `background_error_interrupts_pending_main`.

### F6: WebSocket writer survives incoming-stream shutdown

**Priority:** high.

**Location:** `lazyflow-http/src/ws.rs:161-180,220`.

**Observed:** after `incoming.first()` returns, `sender.send_text("after-drop")`
still succeeds and the local server receives that message.

**Cause:** explicit writer abortion occurs only after normal read-loop exit.
Cancelling the generator skips that code and detaches the writer task.

**Proposed solution:** place the writer under an abort-on-drop owner immediately
after spawning. All generator exit paths must release that owner and socket
resources. Normal protocol shutdown can still attempt a graceful close; cleanup
on task cancellation must not depend on reaching an async epilogue.

If independent sender lifetime is deliberately desired instead, expose and
document an explicit connection owner/close operation. Do not leave lifetime
semantics accidental. Prefer correcting existing incoming-pipe ownership over
introducing a new public abstraction for this fix.

**Tests:** remote peer observes shutdown after `.first()`, `.take()`, explicit
stream drop, and read failure. Verify sender closure eventually follows cleanup;
channel enqueue success alone is not proof of successful network delivery.

**Proof:** `websocket_writer_stops_when_incoming_is_dropped`.

### F7: SSE parsing depends on transport chunk boundaries

**Priority:** high.

**Location:** `lazyflow-http/src/sse.rs:187-207,221-223`.

**Observed:** valid `\u{e9}` (U+00E9) split between HTTP body chunks fails UTF-8 validation;
`data: hello\r\r` produces no event and ends with `PipeError::Closed`.

**Cause:** each transport chunk is decoded independently, and only LF is
recognized as a line delimiter. HTTP chunk boundaries need not align with UTF-8
characters or SSE lines.

**Proposed solution:** extract a production incremental SSE parser that accepts
arbitrary byte fragments, retains incomplete decoding/line state, and emits
parsed events. Handle LF, CR, CRLF, and an optional leading BOM across fragment
boundaries. Follow SSE decoding rules for malformed input rather than treating
valid incomplete UTF-8 as corruption. Keep HTTP connection/retry policy outside
the parser.

Use this same parser in unit and integration tests; remove duplicated test-only
parser logic once equivalent production coverage exists. Introduce parser state
without silently implementing F8/H1 behavior in the same fix; add those changes
and their tests separately.

**Tests:** split representative wire data at every byte position, including
multibyte characters, BOM, CRLF, empty lines, comments, multiline data, and EOF.
Retain a real HTTP fragmentation regression as well as parser unit tests.

**Proofs:** `sse_accepts_utf8_split_across_transport_chunks`,
`sse_accepts_cr_only_line_endings`.

### F8: SSE ID-only frames lose resume state

**Priority:** high.

**Location:** `lazyflow-http/src/sse.rs:226-246,272-275`.

**Observed:** after `id: checkpoint-7\n\n` and disconnect, the reconnect request
omits `Last-Event-ID: checkpoint-7`.

**Cause:** checkpoint updates are conditional on dispatching a nonempty-data
event, after which the temporary ID buffer is cleared.

**Proposed solution:** separate persistent resume state from per-event data and
type state. Process ID updates at the SSE-defined boundary even if no data event
is emitted. Keep the checkpoint across events and reconnections; an empty ID
resets it and suppresses the reconnect header. Ignore invalid ID fields according
to the protocol.

**Tests:** ID-only frames, events without subsequent IDs, empty-ID reset,
forbidden null characters, and captured reconnect headers. Verify that
incomplete frames at EOF follow the protocol's dispatch rules.

**Proof:** `sse_resume_tracks_id_only_frames`. Replay duplication is a potential
application consequence, not something the local test measured.

### F9: Default Stream adapter waits for a full internal batch

**Priority:** medium; clarify intended contract.

**Location:** `src/stream.rs:20-29,45-47`.

**Observed:** a stream yielding one ready item and then `Pending` cannot satisfy
`Pipe::from_stream(...).first()`. Buffer size 1 returns that item immediately.

**Cause:** the adapter awaits each of 256 items or EOF before exposing any data.
Its internal capacity acts as a minimum batch size for a live stream.

**Proposed solution:** await the first available item, then collect only
immediately ready items up to the configured capacity. Return the accumulated
chunk when the source becomes pending. Distinguish pending-with-no-items from
pending-after-items; only the former should delay downstream output.

Document the size as a maximum batch size. If full-batch blocking is intentional,
make that behavior explicit and opt-in rather than an unexpected default.

**Tests:** one item followed by pending; an open channel with a few items;
immediately ready bursts larger than the cap; EOF with a short final batch;
buffer size 1; no busy loop when an empty stream is pending.

**Proof:** `stream_adapter_emits_available_item_without_waiting_for_256`.

### F10: chunks exposes internal source boundaries

**Priority:** medium.

**Location:** `src/pipeline/pull_ops.rs:841-864`.

**Observed:** `from_iter(0..512).chunks(100)` produces batch lengths
`[100,100,56,100,100,56]`, not `[100,100,100,100,100,12]`.

**Cause:** each pull uses a fresh buffer and emits its remainder immediately,
rather than carrying that remainder into the next batch.

**Proposed solution:** retain a remainder in `PullChunks` across pulls. Emit only
complete batches until EOF, then at most one short final batch. Avoid repeated
whole-buffer copying; use an appropriate owned queue or offset-based buffer.
Preserve existing error policy unless an explicit contract change is approved.

**Tests:** same input under many source fragmentations; sizes 1, 7, 100, 256,
and larger than a source chunk; inputs 255/256/257/512 elements; empty input;
exact multiples. Assert batch boundaries as well as flattened output.

**Proof:** `chunks_only_emits_partial_batch_at_end`.

### F11: Option-based termination does not persist

**Priority:** medium.

**Location:** `src/pipeline/pull_ops.rs:1090-1121`.

**Observed:** `[Some(1), None]`, followed by another source chunk `[Some(2)]`,
produces `[1,2]` instead of stopping at `[1]`.

**Cause:** seeing `None` returns currently accumulated items but records no
terminal state for subsequent pulls.

**Proposed solution:** add persistent completion state. Set it at the first
sentinel, discard remaining items in that chunk, and never pull upstream again.
Return accumulated pre-sentinel items once, then EOF.

**Tests:** sentinel first, middle, last, and exactly at a source boundary;
no sentinel; trailing chunks with observable side effects; repeated pulls after
termination. Keep ordinary EOF distinct from an element-level sentinel.

**Proof:** `un_none_terminate_stays_terminated_across_chunks`.

### F12: throttle emits delayed bursts

**Priority:** medium.

**Location:** `src/pipeline/pull_ops.rs:211-229`.

**Observed:** three elements throttled at 40 ms arrive downstream together after
roughly 83 ms rather than being spaced by the configured interval.

**Cause:** sleeps occur while constructing one output vector. No element reaches
the next operator until the whole vector is ready.

**Proposed solution:** retain remaining elements from the upstream chunk and
return at most one element per permitted emission time. Keep the first element
immediate if that remains the contract. Base spacing on actual emission so a
slow consumer does not create catch-up bursts. Do not introduce background tasks
for this inline operator.

**Tests:** record downstream timestamps with paused virtual time; cover one large
source chunk and many small chunks, slow consumers, zero duration, and early
termination. Total collection duration alone is insufficient.

**Proof:** `throttle_spaces_downstream_elements`.

### F13: Zero-sized timed batches make no progress

**Priority:** medium.

**Location:** `src/pipeline/time.rs:116-134`.

**Observed:** `once(1).chunks_timeout(0, ...).take(3)` returns three empty batches
without consuming the source.

**Cause:** `batch.len() >= 0` is always true, and removing zero items never
changes the condition.

**Proposed solution:** normalize zero to one, matching existing `chunks()` and
`sliding_window()` behavior. Explicit rejection is another valid API design, but
would require a documented error policy. Prefer normalization for consistency
without changing the constructor signature.

**Tests:** zero, one, empty input, finite input, and downstream cancellation.
Ensure every emitted batch is nonempty and finite sources terminate.

**Proof:** `zero_sized_timeout_batch_does_not_invent_items`.

### F14: Delimited oversized lines bypass length limits

**Priority:** medium; input-boundary hardening.

**Location:** `src/io.rs:126-153`.

**Observed:** `lines_with_max_len(4)` accepts `12345\n`; default `lines()` accepts
a 1,048,577-byte line when its newline arrives in the same input buffer.

**Cause:** the size guard runs only when the accumulated buffer contains no LF.
Extracted complete lines are not individually checked.

**Proposed solution:** enforce the limit on every completed line and on the
unfinished remainder. Process input incrementally instead of first concatenating
an arbitrarily large collection of buffers. Define whether limits count payload
bytes excluding CR/LF; apply that definition consistently across delimiters and
fragment boundaries. Use checked length arithmetic when combining lengths.

Share the low-level bounded-line logic with other consumers only if delimiter
and encoding contracts actually match. SSE additionally permits bare CR and has
its own decoding rules; do not force incompatible parsers into one abstraction.

**Tests:** exact limit and limit+1, same-buffer and split-buffer newline, CRLF,
no final delimiter, multiple individually valid lines whose total exceeds the
limit, and the documented default 1 MiB bound.

**Proof:** `lines_limit_applies_to_newline_terminated_lines`.

### F15: Interleave ordering depends on source chunk sizes

**Priority:** medium.

**Location:** `src/pipeline/pull_ops.rs:114-152`.

**Observed:** left `[0,1,2,3]`, right chunks `[10,11]` and `[12,13]` produce
`[0,10,1,11,2,3,12,13]` rather than `[0,10,1,11,2,12,3,13]`.

**Cause:** when one local buffer empties, the loop drains the other without
refilling the source whose turn is next.

**Proposed solution:** honor element-level turns. Refill the selected source
when its buffer is empty, skipping it only after actual EOF. Retain leftovers
and return accumulated output at safe chunk boundaries. Keep strict round-robin
semantics separate from arrival-order `merge`; a pending selected source is not
an exhausted source.

**Tests:** arbitrary independent source fragmentation, unequal lengths, one empty
side, pending selected source, and source failure. Compare output against a
simple element-level round-robin reference.

**Proof:** `interleave_is_independent_of_source_chunk_boundaries`.

### F16: Bare-return pipe_fn cannot contain await

**Priority:** medium.

**Location:** `lazyflow-macros/src/lib.rs:155-165`.

**Observed:** a bare-return `#[pipe_fn] async fn` containing an await fails with:

```text
error[E0728]: `await` is only allowed inside `async` functions and blocks
```

**Cause:** expansion evaluates the user body inside a synchronous closure before
wrapping its result in `Ok`.

**Proposed solution:** evaluate the bare-return body in an async context and await
it before wrapping the resulting value. Preserve the user's explicit `return`
semantics inside that body. Keep fallible-return handling separate; avoid
rewriting unrelated macro functionality in this fix.

**Tests:** real awaits in bare and fallible functions, early return, moved
captures, and composition through `eval_map`. Use positive compile tests within
the existing test framework; the audit's compiler-failure example can become
one such test after the fix.

**Proof:** `examples/audit_macro_async.rs` and `macro-proof.log` in the proof worktree.

### H1: SSE has no aggregate event-size bound

**Priority:** high when consuming untrusted endpoints; resource-hardening gap.

**Location:** `lazyflow-http/src/sse.rs:211-219,268-270`.

**Observed:** an event assembled from short legal lines accumulates and emits
17,843,199 data bytes. The 16 MiB line-buffer guard does not limit aggregate event
state.

**Cause:** repeated `data:` fields append to an event accumulator until a blank
line. Individually bounded lines do not bound the whole event.

**Proposed solution:** add explicit configurable limits for line size and total
retained event state. Check limits before growing buffers, include inserted
newlines and retained metadata as appropriate, and define consistent behavior
for overflow. A parser-size violation should not silently become an endless
reconnect loop against the same oversized event.

Select a documented default rather than treating the existing 16 MiB line limit
as an implicit event-size contract. Adding fields to public `SseConfig` can affect
external struct literals; address source compatibility and release notes.

**Tests:** exact-limit and over-limit events, many short data lines, metadata
fields, fragmented input, an unfinished event, and reconnect-disabled/enabled
failure behavior. Keep tests finite; intentionally exhausting machine memory is
unnecessary.

**Evidence boundary:** accepting a 17 MiB event is not an SSE protocol violation.
The test demonstrates missing protection; it did not produce an OOM crash or
measure an exploit threshold.

**Probe:** `sse_bounds_multiline_event_size`.

## Suggested implementation sequence

Each row is a separate focused change with its own red/green test cycle and,
when committing resumes, its own commit.

| Order | Finding | Reason |
| --- | --- | --- |
| 1 | F1 | Preserve in-flight work; local implementation already tested. |
| 2 | F3 | Establish worker ownership before scheduler changes. |
| 3 | F2 | Restore ordered-map progress and bounded work ahead. |
| 4 | F4 | Correct branch lifetime and backpressure interaction. |
| 5 | F5 | Ensure background failures wake blocked consumers. |
| 6 | F6 | Close the WebSocket task/socket lifetime gap. |
| 7 | F7 | Establish one production incremental SSE parser. |
| 8 | F8 | Add correct persistent checkpoint behavior to that parser. |
| 9 | H1 | Enforce resource bounds at parser/connection boundaries. |
| 10 | F9 | Restore prompt live-stream delivery. |
| 11 | F10 | Make batching independent of source fragmentation. |
| 12 | F11 | Persist sentinel termination. |
| 13 | F12 | Enforce consumer-visible rate limiting. |
| 14 | F13 | Normalize zero batch size independently of F1. |
| 15 | F14 | Enforce per-line bounds across all delimiter layouts. |
| 16 | F15 | Preserve element-level interleaving. |
| 17 | F16 | Correct macro async-body expansion. |

This order is a risk-based recommendation, not an architectural dependency graph.
Small independent fixes can move earlier without combining unrelated changes.

### Per-fix completion criteria

1. Add a focused test reproducing the finding at the current baseline; capture
   its failure before implementation.
2. State the invariant being repaired and model concurrent state changes where
   useful. Keep model scope and limitations explicit.
3. Implement the smallest coherent fix; preserve unrelated API behavior.
4. Add boundary and composition regressions, using paused clocks and explicit
   task synchronization rather than wall-clock sleeps where practical.
5. Run focused tests, then workspace all-features tests, Clippy, formatting, and
   rustdoc checks. Keep changes attributable to one finding.
6. Review the diff and any remaining lifecycle/resource assumptions. Record
   unavailable checks rather than claiming they passed.
7. Commit only the fix, tests, and directly relevant documentation/model when
   the requested commit workflow is available. Do not include local tool config
   or unrelated proof-worktree artifacts.

## Test-suite improvements

The green baseline missed these issues because many tests do not challenge the
boundaries where the contracts fail:

- Property inputs often contain fewer than 200 elements, below the default
  256-element source chunk.
- Batch roundtrips verify preserved elements, not correct batch boundaries.
- Throttle tests check total elapsed time, not downstream spacing.
- Fan-out tests materialize branches before dropping them.
- WebSocket tests assert received counts without checking remote shutdown.
- SSE parser unit tests exercise duplicated test-only logic rather than the
  production byte-stream parser.
- 36 ignored documentation examples leave public examples outside compile checks.

Add reusable test sources that emit the same logical data under varied chunking,
explicitly pending pulls, and controlled failure points. Include sizes around
255/256/257 and larger streams. Reuse test helpers when they represent a clear
source behavior, rather than building a large generic test framework.

Extend CI to cover all features and the declared Rust 1.85 minimum. Add dependency
advisory scanning and targeted parser fuzzing separately from these correctness
fixes. The manual all-features checks passed; that does not establish MSRV or
advisory compliance.

## Scope limits and non-findings

- No dependency CVE scan was performed; advisory tools were unavailable.
- Declared Rust 1.85 was not executed. No MSRV failure is claimed.
- No sustained-load benchmark, exhaustive concurrency verification, full parser
  fuzz campaign, or complete Nix package/release validation was performed.
- CancelToken's between-pull behavior matches its explicit documented contract;
  it was not classified as a defect.
- Topic lag dropping and memory growth in distinct/group-adjacent operations are
  documented; they were not classified as unexpected behavior.
- No unproven gRPC TLS/interceptor issue is asserted.
- Similar cancellation concerns in other time operators warrant investigation,
  but only reproduced behaviors are reported as findings here.

## References

- Original local audit: `/Users/hamid/.cache/lazyflow-audit-10ef892/AUDIT.md`.
- Tokio 1.52.3 installed source, `src/runtime/task/join.rs`: dropping a
  `JoinHandle` detaches its task. F3 and F6 also have independent runtime proofs.
- [WHATWG Server-sent events](https://html.spec.whatwg.org/multipage/server-sent-events.html):
  stream-level UTF-8 decoding, CR/LF/CRLF delimiters, and persistent last-event-ID
  processing.
