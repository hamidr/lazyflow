# lazyflow

A lazy, effectful streaming library for Rust, inspired by [FS2](https://fs2.io/).

Pull-based, async, back-pressured. No data flows until a terminal is called.

```rust
use lazyflow::prelude::*;

let result = Pipe::from_iter(1..=10)
    .filter(|x| x % 2 == 0)
    .map(|x| x * 10)
    .take(3)
    .collect().await?;

assert_eq!(result, vec![20, 40, 60]);
```

## Why lazyflow?

- **Lazy**: pipelines are blueprints, not live streams. Nothing executes until
  you call a terminal (`.collect()`, `.for_each()`, etc.)
- **Cloneable**: `Pipe` implements `Clone`. Each clone materializes an
  independent operator chain -- free fan-out, retry, and parallel execution
- **Back-pressured**: pull-based with bounded channels. A slow consumer
  naturally slows the producer. No unbounded buffering.
- **Type-changing**: operators like `.map()` and `.flat_map()` change the
  element type through the chain, fully checked at compile time

## Getting started

```toml
[dependencies]
lazyflow = { git = "https://github.com/hamidr/lazyflow" }

# Optional crates:
lazyflow-io = { git = "https://github.com/hamidr/lazyflow" }    # File, TCP, UDP
lazyflow-http = { git = "https://github.com/hamidr/lazyflow" }   # SSE, WebSocket
lazyflow-grpc = { git = "https://github.com/hamidr/lazyflow" }   # gRPC streaming
```

Requires **Tokio** as the async runtime.

## Core types

| Type | Role |
|------|------|
| `Pipe<B>` | Lazy stream blueprint. Cloneable -- each clone runs independently. |
| `Operator<A, B>` | Per-element async transform with captured state. |
| `Transform<A, B>` | Reusable, composable stream transform. |
| `Sink<B, R>` | Reusable output destination. |
| `Topic<B>` | Pub/sub broadcast to multiple subscribers. |
| `Signal<B>` | Reactive state -- holds current value, emits changes. |
| `CancelToken` | Cooperative cancellation for graceful shutdown. |

## Examples

### ETL pipeline

```rust
Pipe::from_iter(raw_records)
    .map(|r| parse_csv(r))
    .and_then(|r| r.ok())              // drop parse failures
    .filter(|r| r.amount > 0.0)
    .chunks_timeout(1000, Duration::from_secs(5))  // micro-batch
    .eval_map(|batch| insert_db(batch))
    .for_each(|count| println!("inserted {count} rows"))
    .await?;
```

### Fan-out / fan-in

```rust
let branches = Pipe::from_iter(events)
    .partition(4, 8, |e| e.user_id as u64);  // hash-partition by user

let results = Pipe::merge(branches
    .into_iter()
    .map(|branch| branch.map(process))
    .collect())
    .collect().await?;
```

### TCP echo server

```rust
use lazyflow_io::net;

net::tcp_server("0.0.0.0:8080".parse()?)
    .map(|conn| {
        let (lines, writer) = conn.into_lines();
        lines.eval_map(move |line| {
            let writer = writer.clone();
            async move {
                writer.write_all(format!("echo: {line}\n").as_bytes()).await?;
                Ok(line)
            }
        })
    })
    .par_join_unbounded()
    .for_each(|line| println!("{line}"))
    .await?;
```

### SSE consumer

```rust
use lazyflow_http::sse;

sse::connect("https://example.com/events")
    .filter(|e| e.event.as_deref() == Some("update"))
    .map(|e| e.data)
    .for_each(|data| println!("{data}"))
    .await?;
```

Parses incrementally across HTTP body chunks. Auto-reconnects with
exponential backoff and `Last-Event-ID` resume, kept across id-only frames
and reconnects. `SseConfig::max_event_size` (default 16 MiB) bounds one
event; exceeding it ends the pipe with an error and no reconnect.

### WebSocket

```rust
use lazyflow_http::ws;

let (incoming, sender) = ws::connect("wss://example.com/ws");

// Send from any task
let s = sender.clone();
tokio::spawn(async move { s.send_text("hello").await });

// Process incoming
incoming
    .and_then(|m| m.text())
    .for_each(|text| println!("{text}"))
    .await?;
```

Lazy connection, cloneable sender, automatic ping/pong, 16 MiB message limit.
The socket lives as long as the incoming pipe: once that pipe ends or is
dropped, the socket closes and `WsSender` sends fail with `PipeError::Closed`.

### gRPC streaming

```rust
use lazyflow_grpc::streaming;

let response = client.server_stream(Request::new(req)).await?;
let events: Pipe<MyResponse> = streaming::from_tonic(response.into_inner());

events
    .filter(|e| e.is_active())
    .map(|e| transform(e))
    .for_each(|e| println!("{e:?}"))
    .await?;
```

Single-use source (no auto-reconnect -- use `retry()` for application-level reconnection).

### Graceful shutdown

```rust
let token = CancelToken::new();
let pipe = Pipe::iterate(0, |x| x + 1)
    .with_cancel(token.clone())
    .prefetch(4)
    .map(|x| x * 2);

// Later: source stops, pipeline drains buffered elements
token.cancel();
```

### Reusable transforms

```rust
let normalize = Transform::new(|p: Pipe<f64>| p.map(|x| x / 100.0));
let clip = Transform::new(|p: Pipe<f64>| p.map(|x| x.max(0.0).min(1.0)));
let pipeline = normalize.and_then(clip);

let result = Pipe::from_iter(values).apply(&pipeline).collect().await?;
```

### Clone and reuse

```rust
let pipeline = Pipe::from_iter(1..=10)
    .filter(|x| x % 2 == 0)
    .map(|x| x * 10);

// Each clone materializes independently
let sum = pipeline.clone().fold(0, |a, b| a + b).await?;
let first = pipeline.first().await?;
```

## Macros

```rust
// Literal pipe
let p = pipe![1, 2, 3];

// Async generator (cloneable)
let p = pipe_gen!(tx => {
    for i in 0..5 { tx.emit(i).await?; }
});

// Async generator (single-use, owns captures)
let p = pipe_gen_once!(tx => {
    loop { tx.emit(listener.accept().await?).await?; }
});

// Derive Operator from an impl block
#[operator]
impl MyTransform {
    async fn execute(&self, input: A) -> Result<B, PipeError> { ... }
}

// Derive PullOperator from an impl block
#[pull_operator]
impl MyCursor {
    async fn next_chunk(&mut self) -> Result<Option<Vec<B>>, PipeError> { ... }
}

// Normalize async fn for use with eval_map
#[pipe_fn]
async fn double(x: i64) -> i64 { x * 2 }
```

## API reference

### Constructors

| Method | Description |
|--------|-------------|
| `from_iter(items)` | From `IntoIterator` (`B: Clone`) |
| `once(item)` | Single element |
| `empty()` | Empty stream |
| `unfold(seed, f)` | Lazy generation from seed |
| `iterate(init, f)` | Infinite: init, f(init), f(f(init)), ... |
| `repeat(value)` / `repeat_with(f)` | Infinite constant / factory |
| `interval(duration)` | Periodic `Instant` ticks |
| `generate(f)` / `generate_once(f)` | Async generator via `Emitter` |
| `from_reader(r)` / `from_reader_sized(r, n)` | `AsyncRead` source |
| `from_stream(s)` / `from_stream_buffered(s, n)` | `futures::Stream` source |
| `from_pull(factory)` / `from_pull_once(op)` | Custom `PullOperator` |
| `bracket(acquire, use_fn, release)` | Resource-safe pipeline |
| `retry(factory, max)` | Retry from scratch on error |

### Operators

| Method | Description |
|--------|-------------|
| `map(f)` | Transform elements (may change type) |
| `filter(p)` | Keep matching elements |
| `and_then(f)` | Fused map + filter via `Option` |
| `flat_map(f)` / `switch_map(f)` | One-to-many / latest-wins expansion |
| `scan(init, f)` | Stateful transform |
| `tap(f)` / `inspect(f)` | Side-effect, pass through |
| `take(n)` / `skip(n)` | First / drop N elements |
| `take_while(p)` / `skip_while(p)` | Predicate-based slicing |
| `enumerate()` | `(index, element)` pairs |
| `changes()` | Emit only when value changes (O(1) memory) |
| `distinct()` / `distinct_by(key)` | Deduplicate via `HashSet` (unbounded memory) |
| `fixpoint(step)` / `fixpoint_bounded(n, step)` / `fixpoint_with_hasher::<S>(max, step)` | Semi-naive recursion: each round reads the last round's new items (`Round::delta`) and everything so far (`Round::full`); stops when a round finds nothing new |
| `group_adjacent_by(key)` | Group consecutive same-key runs |
| `intersperse(sep)` | Insert separator between elements |
| `chunks(n)` / `sliding_window(n)` | Fixed / overlapping batches; only the last `chunks` batch may be short |
| `chain(other)` / `interleave(other)` | Sequential / element-by-element alternation |
| `zip(other)` / `zip_with(other, f)` | Positional pairing |
| `flatten()` / `unchunks()` | Unnest `Pipe<Pipe<B>>` / `Pipe<Vec<B>>` |
| `attempt()` | Errors become `Result` elements |
| `none_terminate()` / `un_none_terminate()` | Wrap in `Some` and end with `None` / stop at the first `None` |
| `lines()` / `lines_with_max_len(n)` | Split a `Pipe<Vec<u8>>` into lines; content over 1 MiB (or `n`) fails |
| `top_n(k, key)` | Keep the `k` highest-ranked elements in O(k) memory, descending |
| `fold_collect(f)` / `try_fold_collect(f)` | Drain, transform the whole buffer, re-emit |
| `on_finalize(f)` | Cleanup on completion/error/drop |
| `with_cancel(token)` | Stop on `CancelToken` signal |
| `meter_with(name, cb)` | Observability hook |

### Async operators

| Method | Description |
|--------|-------------|
| `eval_map(f)` / `eval_filter(f)` / `eval_tap(f)` | Async per-element transform / filter / side-effect |
| `par_eval_map(n, f)` | Bounded parallel, ordered output; at most `n` results running or awaiting emission |
| `par_eval_map_unordered(n, f)` | Bounded parallel, unordered output |
| `pipe(operator)` | Custom `Operator<A, B>` |

### Time-based

| Method | Description |
|--------|-------------|
| `timeout(d)` | `PipeError::Timeout` if a pull exceeds the deadline |
| `throttle(d)` | Rate-limit output as the consumer receives it (delays, not drops) |
| `delay_by(d)` | Delay each element |
| `debounce(d)` | Emit after quiet period |
| `chunks_timeout(n, d)` | Batch by count or time; size 0 acts as 1 |

### Concurrency

| Method | Description |
|--------|-------------|
| `prefetch(n)` | Buffer N chunks ahead |
| `merge(pipes)` / `merge_with(other)` | Concurrent fan-in |
| `broadcast(n, buf)` | Fan-out to N consumers |
| `broadcast_through(buf, transforms)` | Fan-out + transform + merge |
| `partition(n, buf, key)` | Hash-partition across N branches |
| `unzip(buf)` | `Pipe<(A, B)>` into `(Pipe<A>, Pipe<B>)` |
| `concurrently(bg)` | Run background alongside self |
| `par_join(n)` / `par_join_unbounded()` | Flatten `Pipe<Pipe<B>>` concurrently |

### Error handling

| Method | Description |
|--------|-------------|
| `handle_error_with(f)` | Switch to fallback pipe on error |
| `attempt()` | Errors become `Result` elements |
| `bracket(acquire, use_fn, release)` | Guaranteed cleanup via `Arc<R>` |
| `retry(factory, max)` | Retry from scratch on error |

`PipeError` variants: `Io`, `Closed`, `RetryExhausted`, `Timeout`,
`Panicked(msg)` (a spawned task panicked, so the pipe fails instead of ending
short), and `Custom`. Fan-out branches and `concurrently` keep the source's
variant.

### Terminals

| Method | Description |
|--------|-------------|
| `collect().await?` | Collect into `Vec<B>` |
| `fold(init, f).await?` | Reduce to single value |
| `reduce(f).await?` | Reduce without initial value |
| `count().await?` | Count elements |
| `for_each(f).await?` / `eval_for_each(f).await?` | Sync / async side-effect |
| `first().await?` / `last().await?` | First / last element |
| `try_fold_chunks(init, f).await?` | Fold whole chunks; `ControlFlow::Break` stops early |
| `into_writer(w).await?` | Drain to `AsyncWrite` |
| `into_stream()` / `into_stream_buffered(n)` | Convert to `futures::Stream` |
| `drain_to(&sink).await?` | Consume via `Sink` |

### Reactive primitives

| Method | Description |
|--------|-------------|
| `Topic::new(buf)` | Pub/sub broadcast channel |
| `topic.subscribe()` | New subscriber as `Pipe<B>` |
| `topic.publish(value)` | Send to all subscribers |
| `Signal::new(init)` | Reactive state cell |
| `signal.get()` / `set(v)` / `modify(f)` | Read / write / mutate |
| `signal.subscribe()` | Changes as `Pipe<B>` |
| `pipe.hold(init)` | Convert pipe to signal |

### Composition

| Method | Description |
|--------|-------------|
| `through(f)` | Stream-level transform |
| `apply(&transform)` | Apply a `Transform<A, B>` |
| `Transform::new(f).and_then(other)` | Compose transforms |
| `Sink::new(f)` / `collect()` / `count()` / `first()` / `last()` / `drain()` | Output destinations |

## Crate ecosystem

| Crate | Description |
|-------|-------------|
| `lazyflow` | Core streaming engine |
| `lazyflow-macros` | Proc macros: `#[operator]`, `#[pull_operator]`, `#[pipe_fn]` |
| `lazyflow-io` | File, TCP, UDP constructors |
| `lazyflow-http` | SSE source, WebSocket source/sink |
| `lazyflow-grpc` | gRPC streaming source, server response adapter |

## Development

The Nix flake pins the toolchain (Rust 1.99.0) and `protoc`. Run the CI
checks locally (fmt, clippy with warnings denied, all-features tests, rustdoc
with warnings denied):

```bash
scripts/gate.sh
```

`models/` holds Quint models that supplement the regression tests; see
`models/README.md`.

## License

MIT
