---
id: ADR-002
title: Connector Strategy
status: accepted
date: 2026-04-11
status_date: 2026-04-14
summary: SSE and WebSocket shipped (Phase 2); Phase 3 Kafka/NATS brokers deferred
depends_on: []
supersedes: []
superseded_by: []
---

# ADR-002: Connector Strategy -- SSE First, Then WebSocket, Then Brokers

**Related**: [ADR-001](ADR-001-operator-fusion-strategy.md) (deferred in favor of this)

## Context

Pipe has a capable streaming engine (pull-based backpressure, chunked
execution, clone-and-materialize, cooperative cancellation, error
recovery) but only connects to raw TCP, UDP, and local files via
`lazyflow-io`. Nobody can build a production streaming service with lazyflow
today because it doesn't talk to the systems production services talk
to: HTTP APIs, WebSocket feeds, message brokers.

Without real-world connectors, every other investment (fusion, windowing,
observability) optimizes a library nobody deploys. Connectors are the
critical path to users.

### Connector candidates evaluated

| Connector | Infra needed | Protocol complexity | Validates |
|-----------|-------------|---------------------|-----------|
| SSE (Server-Sent Events) | None (any HTTP endpoint) | Low (text/event-stream, reconnect) | Backpressure, retry, cancellation |
| HTTP chunked response sink | None | Low (Transfer-Encoding: chunked) | Sink API, backpressure from slow clients |
| WebSocket | None (any WS endpoint) | Medium (framing, ping/pong, close) | Bidirectional, source + sink |
| Kafka | Broker cluster | High (consumer groups, offsets, rebalancing) | Partitioned consumption, exactly-once |
| NATS JetStream | NATS server | Medium (ack, replay, consumer groups) | Durable subscriptions, at-least-once |
| PostgreSQL LISTEN/NOTIFY | PostgreSQL | Low (but niche) | Database-driven events |
| Redis Streams | Redis | Medium (XREAD, consumer groups, claiming) | Lightweight brokered streaming |

### Why SSE first

SSE is the smallest connector that exercises the most architecture:

1. **Backpressure**: SSE is a server-push protocol. The client must
   handle a fast server without unbounded buffering. Pipe's bounded
   channels and pull-based execution are designed for exactly this --
   the SSE source fills a bounded channel, the consumer pulls at its
   own pace, and TCP flow control propagates backpressure to the server.

2. **Error recovery**: SSE has a built-in reconnection protocol
   (`Last-Event-ID` header, `retry:` field). This validates lazyflow's
   `retry()` and `handle_error_with()` operators against a real
   reconnection scenario.

3. **Cancellation**: Dropping the pipe must close the HTTP connection
   cleanly. This validates `CancelToken` and abort-handle-based cleanup
   in a real I/O context.

4. **Zero infrastructure**: Tests can run against any SSE endpoint
   (or a local test server spawned in-process). No Docker, no broker
   setup, no CI complexity.

5. **Immediately useful**: SSE consumption is a real use case (GitHub
   event streams, financial data feeds, real-time dashboards, webhook
   relay).

### Why not Kafka first

Kafka validates important properties (partitioned consumption,
exactly-once, offset management) but:

- Requires a running broker for every test run (Docker in CI)
- Consumer group protocol is ~2000 lines of state machine logic
- 80% of implementation time goes to Kafka protocol details, not
  validating lazyflow's architecture
- The rdkafka C library dependency adds cross-compilation pain
- If lazyflow's architecture has a flaw (e.g., backpressure doesn't
  compose through connectors), you want to discover it with a
  1-day SSE connector, not a 3-week Kafka connector

## Decision

Build connectors in three phases. Each phase validates a progressively
harder architectural property before committing to the next.

### Phase 1: lazyflow-http crate (SSE source + HTTP sink)

New crate: `lazyflow-http`, depending on `lazyflow`, `reqwest` (HTTP client),
and `hyper` (for the sink/server side, optional feature).

**SSE source API**:

```rust
use lazyflow_http::sse;

// Basic: connect and stream events
let events: Pipe<SseEvent> = sse::connect("https://api.example.com/events")
    .await?;

// With reconnection: resumes from last event ID on disconnect
let events = sse::connect_with(SseConfig {
    url: "https://api.example.com/events".into(),
    headers: vec![("Authorization", "Bearer ...")],
    reconnect: true,       // auto-reconnect on disconnect
    max_reconnect_delay: Duration::from_secs(30),
    last_event_id: None,   // or Some("...") to resume
    buffer_size: 256,      // internal channel capacity
})?;

// SseEvent type
pub struct SseEvent {
    pub event: Option<String>,  // event type (e.g., "message")
    pub data: String,           // event payload
    pub id: Option<String>,     // Last-Event-ID for reconnection
    pub retry: Option<Duration>, // server-suggested retry interval
}

// Usage in pipeline
events
    .filter(|e| e.event.as_deref() == Some("update"))
    .map(|e| serde_json::from_str::<Update>(&e.data))
    .and_then(|r| r.ok())
    .for_each(|update| println!("{update:?}"))
    .await?;
```

**HTTP chunked sink API** (feature-gated behind `sink` feature):

```rust
use lazyflow_http::sink;

// Pipe<String> -> HTTP chunked response body
// (for use inside an HTTP handler)
pipe.into_http_body()  // -> impl hyper::body::Body
```

**What this validates**:
- Backpressure from lazyflow consumer to HTTP/TCP layer
- Reconnection with state (Last-Event-ID)
- CancelToken/drop-based connection cleanup
- Error propagation from network failures to lazyflow errors
- Real-world SSE parsing (multi-line data, comments, retry fields)

**Deliverables**:
- `lazyflow-http` crate with `sse` module
- SSE protocol parser (text/event-stream format)
- Auto-reconnection with exponential backoff
- Integration test against in-process test server
- Example: GitHub public events consumer

### Phase 2: WebSocket source + sink

Add `lazyflow-http::ws` module (or separate `lazyflow-ws` crate) using
`tokio-tungstenite`:

```rust
use lazyflow_http::ws;

// Bidirectional: source + sink from one connection
let (incoming, outgoing_sink) = ws::connect("wss://stream.example.com")
    .await?;

// incoming: Pipe<WsMessage>
// outgoing_sink: Sink<WsMessage, ()>

incoming
    .filter(|msg| msg.is_text())
    .map(|msg| process(msg))
    .drain_to(&outgoing_sink)
    .await?;
```

**What this validates beyond SSE**:
- Bidirectional communication (source + sink on same connection)
- Binary framing (not just text)
- Connection lifecycle (ping/pong, close handshake)
- Sink backpressure (slow server)

### Phase 3: Message broker connectors (Kafka, NATS)

Separate crates: `lazyflow-kafka`, `lazyflow-nats`. Only after Phase 1 and 2
confirm that lazyflow's architecture composes correctly with real I/O.

**What this validates beyond WebSocket**:
- Partitioned consumption (consumer groups, rebalancing)
- Offset/ack management (at-least-once, exactly-once)
- Durable subscriptions across process restarts
- Multi-partition fan-out via lazyflow's `partition()` operator

**Deferred** until Phase 1 and 2 are shipped and have real users
providing feedback on the connector API patterns.

## Consequences

### Positive

- SSE connector is immediately useful and shippable in days, not weeks
- Each phase validates architecture before committing to more complex
  connectors
- Phase 1 establishes the connector API patterns (config structs,
  error handling, reconnection) that Phase 2 and 3 follow
- Zero new infrastructure required for Phase 1 development and CI
- Real-world testing possible against public SSE endpoints (GitHub,
  Mastodon, etc.)
- New `lazyflow-http` crate keeps the core `lazyflow` crate dependency-free

### Negative

- reqwest dependency is heavy (~30 transitive deps). Mitigated by
  keeping it in a separate crate -- core lazyflow is unaffected.
- SSE is a niche protocol. Not everyone needs it. But it's the
  cheapest way to validate the connector model.
- Two-crate pattern (lazyflow-io for raw I/O, lazyflow-http for HTTP) may
  confuse users about where to find things. Clear documentation and
  a "connectors" section in README should address this.
- Phase 3 is deferred with no timeline. Users wanting Kafka now have
  to write their own using `Pipe::generate()` and the rdkafka crate.
  This is explicitly acceptable -- the generate/from_pull escape
  hatches exist for this purpose.

### Risks

- reqwest's async runtime assumptions may conflict with lazyflow's
  pull-based model. Mitigation: the SSE source runs reqwest in a
  spawned task, feeds a bounded channel, and the pipe pulls from
  the channel. This is the same pattern as `Pipe::generate()`.
- SSE parsing edge cases (multi-line data fields, BOM handling,
  non-standard servers) may consume more implementation time than
  expected. Mitigation: use an existing SSE parsing crate if one
  is adequate, or keep the parser minimal and spec-compliant
  (W3C EventSource spec is ~2 pages).
- Connector API patterns established in Phase 1 may not generalize
  to brokers in Phase 3. Mitigation: Phase 2 (WebSocket) is
  specifically chosen to stress-test bidirectionality and connection
  lifecycle before committing to broker complexity.
