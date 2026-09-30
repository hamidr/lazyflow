# Changelog

All notable changes to the lazyflow workspace are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
- **lazyflow-grpc**: `serve::Server` builder (ADR-004 Phase 1) --
  `.tls()`, `.client_ca()`, `.client_auth_optional()` (behind a new `tls`
  Cargo feature), and `.interceptor()`, eliminating the TLS/mTLS/auth
  bootstrap boilerplate a tonic service otherwise repeats.
  `.build()` hands back the real `tonic::transport::Server`, so
  `.serve()`, `.serve_with_shutdown()`, `.add_service()`, and every other
  tonic transport method keep working unchanged.

## [0.10.0] - 2026-06-12

### Added
- **lazyflow**: `Pipe::try_fold_chunks`, a chunk-granular fold with
  cooperative break. Drives the pipeline chunk by chunk, handing each whole
  chunk to the fold closure so a consumer can amortize per-item work
  (accounting, metering, budget checks) to once per chunk and stop
  cooperatively at a chunk boundary without a separate cancel token. The
  primitive behind a caller-defined budget-aware blocking operator (e.g. a
  bounded top-N that charges an external memory accountant per row) that
  `top_n`/`fold_collect` alone cannot express, since those buffer their
  whole input before the caller sees anything.

## [0.9.0] - 2026-05-20

### Added
- **lazyflow**: `Pipe::fold_collect` and `Pipe::try_fold_collect`, a
  blocking-operator primitive that drains the input, transforms the whole
  buffer, and re-emits it as a stream, replacing the hand-written
  collect-then-`from_iter` idiom.
- **lazyflow**: `Pipe::top_n`, a bounded top-N operator that keeps the `k`
  highest-ranked elements in O(k) memory and O(N log k) time, emitting them
  in descending key order.

## [0.8.0] - 2026-05-20

### Changed
- **lazyflow-grpc**: upgraded tonic from 0.12 to 0.14. The prost codec is now
  a separate crate, so consumers writing server-side RPCs must add `tonic-prost`
  and switch `build.rs` codegen from `tonic-build` to `tonic-prost-build`.
- Bumped the workspace to 0.8.0 (`lazyflow`, `lazyflow-macros`, `lazyflow-io`,
  `lazyflow-http`, `lazyflow-grpc`)

## [0.7.0] - 2026-05-20

### Changed
- Renamed the crate from `pipe` to `lazyflow` for crates.io availability; the
  workspace now publishes as `lazyflow`, `lazyflow-macros`, `lazyflow-io`,
  `lazyflow-http`, and `lazyflow-grpc`
- Updated dependencies to the latest semver-compatible versions
- Dropped the explicit version from the `lazyflow-io` dev-dependency so the
  workspace publishes cleanly

## [0.6.2] - 2026-04-14

### Added
- GitHub Actions CI workflow (fmt, clippy, test on Linux + macOS via Nix)
- Criterion benchmark suite for core operators (map, filter, flat_map, chunks, fold, merge)
- CHANGELOG.md covering full version history
- lazyflow-grpc in README: install instructions, streaming example, crate ecosystem table

### Changed
- Synced ADR statuses and TRACKER.md with shipped work
- Prepared crate metadata for crates.io publishing (version fields, keywords)

## [0.6.0] - 2026-04-11

### Added
- **lazyflow-grpc** crate (v0.1.0): tonic `Streaming<T>` source, server response adapter, TLS/mTLS server builder
- Re-exported tonic types from lazyflow-grpc so users don't need a direct tonic dependency

### Changed
- Rewrote README for clarity and narrative flow

## [0.5.1] - 2026-04-11

### Added
- WebSocket source + sink in lazyflow-http (lazy connect, cloneable sender, automatic ping/pong)

### Fixed
- WebSocket implementation: lazy connect, channel-based sender, safety improvements
- Code quality: docs, examples, duplicate Arc::clone

## [0.5.0] - 2026-04-11

### Added
- **lazyflow-http** crate: SSE source with auto-reconnect, exponential backoff, Last-Event-ID resume
- ADR-001: operator fusion strategy (deferred)
- ADR-002: connector strategy (SSE -> WebSocket -> brokers)

### Fixed
- SSE implementation: security, correctness, and test coverage

## [0.4.0] - 2026-04-11

### Fixed
- Integer overflow in PullChunks group calculation
- Integer overflow in intersperse capacity calculation
- OOM prevention: max line length limit in PullLines

### Added
- Safety documentation for tcp_server timeout, file path traversal, lossy UTF-8, distinct memory cost, group_adjacent_by unbounded groups

## [0.3.1] - 2026-04-11

### Changed
- Reworked `#[pipe_fn]` to generate functions instead of structs
- Vec::retain in PullFilter for in-place filtering
- Reuse Vec allocation in PullAndThen across chunks
- Replace oneshot channels with JoinHandles in par_eval_map
- Use Arc::try_unwrap in broadcast receiver to avoid cloning

### Added
- `#[pipe_fn]` macro to derive Operator from async functions
- `PipeResult<T>` type alias

### Fixed
- Error handling, safety, and testing improvements across codebase

## [0.3.0] - 2026-04-11

### Added
- Topic pub/sub primitive for multi-subscriber streaming
- Signal reactive state primitive and Pipe::hold
- switch_map operator (latest-wins stream switching)
- distinct / distinct_by operators for deduplication
- changes operator (emit only on value change)
- group_adjacent_by operator (consecutive grouping)
- on_finalize combinator (lightweight cleanup)
- delay_by operator (per-element delay)
- reduce terminal (fold without initial value)
- par_join / par_join_unbounded for concurrent stream merging
- generate_once / pipe_gen_once! for single-use sources
- eval_for_each async terminal

### Fixed
- par_eval_map: acquire permit before spawning
- bracket: release receives resource via Arc
- Error swallowing in prefetch, merge, debounce, generate, generate_once
- switch_map error propagation and task cleanup
- partition abort handle sharing
- Asserted chunks size > 0

## [0.2.0] - 2026-04-11

### Added
- pipe!, pipe_gen!, #[operator], #[pull_operator] macros (lazyflow-macros crate)
- CancelToken for cooperative graceful shutdown
- meter_with combinator for observability
- concurrently combinator
- attempt, zip_with, none_terminate, broadcast_through operators
- sliding_window, unzip, interleave, intersperse, flatten, throttle, debounce
- repeat, repeat_with, interval constructors
- par_eval_map / par_eval_map_unordered
- Pipe::generate for ergonomic async sources
- Sink trait for composable output destinations
- chunks_timeout and timeout combinators
- Transform trait for composable stream transforms
- bracket combinator for resource safety
- Integration tests for complex pipeline flows

### Changed
- Pipe<B> is now cloneable via factory pattern
- PipeError replaced type alias with typed error enum
- Reduced allocations on hot paths
- Broadcast and partition are fully lazy

## [0.1.0] - 2026-04-11

### Added
- Initial release extracted from knot-pipe
- Core Pipe<B> type: lazy, pull-based, async streaming
- Operators: map, filter, flat_map, scan, take, skip, chunks, zip, chain, enumerate, tap
- Terminals: collect, fold, for_each, first, last, count, into_writer, into_stream
- Concurrency: merge, broadcast, partition, prefetch
- Error handling: handle_error_with, retry
- lazyflow-io crate: file, TCP, UDP constructors
- Generic I/O adapters: AsyncRead source, AsyncWrite sink, lines
