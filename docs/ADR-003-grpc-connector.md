---
id: ADR-003
title: gRPC Connector
status: accepted
date: 2026-04-11
status_date: 2026-04-14
summary: Server streaming source and server response shipped (Phase 1); Phase 2 bidi deferred
depends_on: [ADR-002]
supersedes: []
superseded_by: []
---

# ADR-003: gRPC Connector -- Server Streaming Source First, Then Bidi

**Related**: [ADR-002](ADR-002-connector-strategy.md) (extends connector strategy with gRPC)

## Context

ADR-002 established a phased connector strategy: SSE (Phase 1, shipped),
WebSocket (Phase 2, shipped), message brokers (Phase 3, deferred). Both
shipped phases validated that lazyflow's pull-based backpressure, bounded
channels, and clone-and-materialize semantics compose correctly with
real network protocols.

gRPC is the dominant RPC framework for service-to-service communication
in microservice architectures. It supports four interaction patterns:

| Pattern | Direction | Pipe analogy |
|---------|-----------|--------------|
| Unary | Request -> Response | `eval_map` operand |
| Server streaming | Request -> Stream<Response> | `Pipe<T>` source (like SSE) |
| Client streaming | Stream<Request> -> Response | Sink / sender handle |
| Bidirectional streaming | Stream<Request> <-> Stream<Response> | `(Pipe<T>, Sender)` tuple (like WebSocket) |

Three of the four patterns produce or consume streams, making gRPC a
natural fit for lazyflow. The Rust ecosystem has a mature gRPC stack in
`tonic`, which is Tokio-native and uses `prost` for protobuf codegen --
no runtime mismatch.

### Why gRPC before brokers

gRPC sits between WebSocket (Phase 2) and brokers (Phase 3) in
complexity:

1. **No infrastructure**: Like SSE and WebSocket, gRPC tests can run
   against an in-process `tonic` server. No Docker, no broker setup.

2. **Validates new properties**: Unlike WebSocket, gRPC has typed
   messages (protobuf), metadata/trailers, status codes, deadlines,
   and HTTP/2 flow control. These exercise lazyflow's type system and
   error handling in ways WebSocket did not.

3. **High user demand**: gRPC is ubiquitous in production microservices.
   A lazyflow-grpc crate makes lazyflow immediately useful for service mesh
   integration, streaming ETL from gRPC APIs, and real-time data
   pipelines between services.

4. **Validates codegen integration**: Users bring protobuf-generated
   types. lazyflow-grpc must be generic over these types, validating that
   lazyflow's trait bounds compose with external codegen -- a property
   brokers will also need (Avro schemas, Kafka message types).

### Why not brokers instead

The same arguments from ADR-002 still hold:

- Kafka/NATS require running infrastructure for every test
- Consumer group protocols are large state machines
- 80% of implementation time goes to broker protocol, not lazyflow
  architecture validation
- If lazyflow-grpc reveals a composability flaw, better to find it in a
  1-week connector than a 3-week Kafka connector

### Codegen coupling

gRPC's reliance on protobuf codegen is the main design tension.
lazyflow-grpc cannot own the generated types -- users run `tonic-build`
in their own build.rs. Two approaches:

| Approach | Pros | Cons |
|----------|------|------|
| Trait bounds on `prost::Message` | Type-safe, works with any generated type | Couples to prost; excludes alternative codegen |
| Generic over `T: Send + 'static` | Maximum flexibility | No compile-time proto validation |

Decision: bound on `T: Send + 'static` at the lazyflow-grpc API level.
tonic already handles serialization internally via its `Codec` trait.
lazyflow-grpc wraps tonic's `Streaming<T>` for receiving and accepts
`impl tonic::IntoStreamingRequest` for sending -- the protobuf
encoding/decoding stays in tonic's layer, not ours.

## Decision

Create a new `lazyflow-grpc` crate in two phases. Each phase validates
progressively harder properties.

### Phase 1: Client-side gRPC streaming (source)

New crate: `lazyflow-grpc`, depending on `lazyflow` and `tonic`.

Wrap tonic's `Streaming<T>` response into a `Pipe<T>` source. This is
the server-streaming-RPC-as-lazyflow-source pattern -- the simplest gRPC
streaming scenario and directly analogous to SSE.

**API sketch**:

```rust
use lazyflow_grpc::streaming;

// Wrap a tonic server-streaming response into a Pipe
let channel = tonic::transport::Channel::from_static("http://[::1]:50051")
    .connect()
    .await?;
let mut client = MyServiceClient::new(channel);
let response = client.server_stream(Request::new(req)).await?;

// Convert tonic::Streaming<T> -> Pipe<T>
let events: Pipe<MyResponse> = streaming::from_tonic(response.into_inner());

// Now use lazyflow operators: backpressure, map, filter, retry, fan-out
events
    .filter(|e| e.status == Status::Active)
    .map(|e| transform(e))
    .for_each(|e| println!("{e:?}"))
    .await?;
```

**What this validates**:
- Pipe's pull-based model over HTTP/2 flow control (tonic/hyper manage
  the HTTP/2 window; lazyflow pulls from tonic's stream via bounded channel)
- Type composition: user-generated protobuf types flow through lazyflow's
  generic operators without friction
- Error mapping: tonic `Status` errors map to `PipeError`
- Cancellation: dropping the pipe cancels the gRPC stream (tonic drops
  the HTTP/2 stream on Streaming drop)

**Deliverables**:
- `lazyflow-grpc` crate with `streaming` module
- `from_tonic(Streaming<T>) -> Pipe<T>` converter
- tonic `Status` -> `PipeError` mapping
- Integration test against in-process tonic server
- Example: server-streaming RPC consumer

### Phase 2: Bidirectional streaming and sink

Add client-streaming and bidi-streaming support. This mirrors WebSocket's
`(Pipe<T>, Sender)` pattern.

```rust
use lazyflow_grpc::bidi;

// Bidirectional: outgoing requests + incoming responses
let (responses, sender) = bidi::connect(|outgoing_stream| {
    client.bidi_stream(outgoing_stream)
}).await?;

// responses: Pipe<MyResponse>
// sender: GrpcSender<MyRequest>

// Send requests via sender, receive responses via pipe
sender.send(MyRequest { ... }).await?;
responses
    .map(|r| process(r))
    .for_each(|r| println!("{r:?}"))
    .await?;
```

**What this validates beyond Phase 1**:
- Bidirectional streaming over a single gRPC connection
- Sink backpressure: slow server -> sender blocks
- Client-streaming: aggregate a `Pipe<T>` into a single gRPC response
- Connection lifecycle: gRPC stream completion vs cancellation

### Not in scope

- **Server-side gRPC handlers**: Accepting incoming gRPC calls and
  routing them into lazyflow pipelines. This requires deep integration with
  tonic's service trait and Tower middleware. Deferred until client-side
  patterns are validated.
- **Reflection / dynamic dispatch**: lazyflow-grpc assumes compiled protobuf
  types. Dynamic message handling is out of scope.
- **Load balancing / service discovery**: Handled by tonic's Channel
  layer, not by lazyflow-grpc.
- **Interceptors / middleware**: Users compose tonic interceptors before
  handing streams to lazyflow-grpc. No duplication of tonic's middleware.

## Consequences

### Positive

- gRPC streaming maps naturally to lazyflow's existing patterns (source,
  sink, bidirectional tuple) -- no new abstractions needed
- Zero infrastructure for development and CI (in-process tonic server)
- Validates codegen integration pattern that brokers will also need
- tonic is Tokio-native; no runtime bridging or compatibility layers
- HTTP/2 flow control provides transport-level backpressure that
  composes with lazyflow's channel-based backpressure
- Immediately useful for the large population of Rust services using
  tonic for inter-service communication

### Negative

- tonic + prost add significant transitive dependencies (~50 crates).
  Mitigated by separate crate -- core lazyflow stays dependency-free.
- Users must set up protobuf codegen (build.rs, .proto files) before
  they can use lazyflow-grpc. This is inherent to gRPC, not a lazyflow-grpc
  design choice.
- API ergonomics depend on tonic's types (Channel, Streaming, Request).
  lazyflow-grpc is a thin adapter, not a full abstraction -- tonic version
  bumps may require lazyflow-grpc major version bumps.
- Phase 2 (bidi) API design is speculative. The exact shape of the
  `connect` function will depend on how tonic exposes client-streaming
  and bidi-streaming request builders.

### Risks

- **tonic version churn**: tonic is pre-1.0 (currently 0.14). Breaking
  changes in tonic could force lazyflow-grpc updates. Mitigation: pin tonic
  minor version; keep the adapter layer thin so updates are mechanical.
- **HTTP/2 backpressure opacity**: lazyflow cannot directly observe or
  control HTTP/2 flow control windows. If tonic buffers aggressively
  internally, lazyflow's backpressure may not propagate to the server.
  Mitigation: measure in integration tests; if needed, add a bounded
  channel between tonic's stream and lazyflow's pull operator (same pattern
  as SSE).
- **Codegen ergonomics**: If the API requires too much boilerplate to
  convert between tonic types and lazyflow types, adoption will suffer.
  Mitigation: provide helper macros or extension traits if Phase 1
  feedback indicates friction.
