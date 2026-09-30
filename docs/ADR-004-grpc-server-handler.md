# ADR-004: gRPC Server Handler -- Pipe-Native Service Definitions

**Status**: Accepted (Phase 1 shipped 2026-09-30; Phase 2 still deferred)
**Date**: 2026-04-11
**Supersedes**: --
**Related**: [ADR-003](ADR-003-grpc-connector.md) (extends gRPC connector with server-side support)

## Context

ADR-003 defined a phased gRPC connector strategy. The client-side
streaming source (`streaming::from_tonic`) and the server-side response
adapter (`serve::to_stream`) are both implemented and in use. The serve
module was added opportunistically -- it was small and immediately useful.

In practice (validated by knot-server, a graph database using lazyflow-grpc),
the server-side pattern works but requires significant tonic boilerplate:

1. **Service trait implementation**: Users must implement tonic's generated
   `#[tonic::async_trait]` trait and call `serve::to_stream(pipe)` at
   each streaming RPC return site.

2. **Server bootstrap**: TLS configuration, interceptor wiring,
   graceful shutdown, and `Server::builder()` setup all live in user
   code. This is ~60 lines of mechanical tonic boilerplate.

3. **Error bridging**: Users manually wrap domain errors into
   `PipeError::Custom(Box::new(tonic::Status::...))` and rely on
   `pipe_error_to_status` to unwrap them. The round-trip works but
   is indirect.

4. **Unary RPCs get no benefit**: Only streaming RPCs use lazyflow today.
   The 5 unary RPCs in knot-server are plain `async fn` -- lazyflow adds
   nothing there.

The current architecture is correct but split: lazyflow-grpc is an adapter
between two worlds rather than a cohesive abstraction. Users must
understand both tonic's API and lazyflow's API in full.

### What a server handler layer would provide

A thin declarative layer over tonic that lets users define gRPC services
in terms of lazyflow primitives:

| Today (tonic + lazyflow-grpc) | Proposed (lazyflow-grpc server) |
|---------------------------|----------------------------|
| Implement generated `#[tonic::async_trait]` trait | Declare handlers as closures/functions |
| `type QueryStream = serve::PipeResponse<T>` | Implicit -- streaming handlers return `Pipe<T>` |
| Manual `Server::builder().tls_config(...)` | `lazyflow_grpc::Server::builder().tls(cert, key)` |
| Manual interceptor wiring | `.interceptor(fn)` on builder |
| `serve::to_stream(pipe)` at each call site | Automatic for streaming handlers |
| `pipe_error_to_status` for error mapping | Built into the handler contract |

### Why now

- ADR-003's streaming source and serve adapter are implemented and
  validated in production (knot-server).
- The serve module already exists -- this ADR extends it, not replaces it.
- knot-server has 6 RPCs (1 streaming, 5 unary) providing a concrete
  test case for the API design.
- Server-side support was explicitly listed as "not in scope" in ADR-003
  pending client-side validation. That validation is complete.

### Design tension: thin wrapper vs framework

Two extremes:

| Approach | Pros | Cons |
|----------|------|------|
| Thin builder over tonic | Small API surface, easy to maintain, tonic upgrades are mechanical | Users still see tonic types (Request, Response, Status) |
| Full framework hiding tonic | Clean lazyflow-only API, no tonic in user code | Large API surface, version coupling, reimplements tonic features |

Decision: thin wrapper. tonic is good at what it does (transport, codegen,
HTTP/2). lazyflow-grpc should eliminate boilerplate, not replace tonic's
internals. Users who need advanced tonic features (custom codecs, Tower
layers, health checks) can drop down to tonic directly.

## Decision

Extend lazyflow-grpc's `serve` module with a server builder API. Tonic
remains the transport layer. The builder wraps `tonic::transport::Server`
and accepts standard tonic services for serving.

### Phase 1: Server builder

**Server builder**:

```rust
use lazyflow_grpc::serve;

let server = serve::Server::builder()
    .tls(cert_pem, key_pem)
    .client_ca(ca_pem)            // optional mTLS
    .interceptor(auth_interceptor) // tonic interceptor
    .build();

server
    .serve("0.0.0.0:50051".parse()?, svc)
    .with_shutdown(shutdown_signal())
    .await?;
```

The builder wraps `tonic::transport::Server` and exposes the subset
of configuration that most services need. Advanced users bypass the
builder and use tonic directly with `serve::to_stream`.

**What this does NOT include**:

- Proto codegen -- users still run `tonic-build` and get the generated
  service trait. lazyflow-grpc's builder wraps the tonic service, not
  replaces it.
- Handler traits or routing -- the generated tonic trait already defines
  the routing. Users implement it as before. A macro that generates the
  trait impl from closures is Phase 2.

**Intended usage**:

```rust
// Users still implement the tonic trait, but the body is simpler:
#[tonic::async_trait]
impl MyService for MyServer {
    type QueryStream = serve::PipeResponse<QueryResponse>;

    async fn query(
        &self,
        req: Request<QueryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        let pipe = self.build_query_pipe(req.into_inner())?;
        Ok(Response::new(serve::to_stream(pipe)))
        // ^ this part already works today
    }
}

// The new part: server bootstrap is simpler
serve::Server::builder()
    .tls(cert, key)
    .interceptor(auth)
    .build()
    .serve(addr, MyServiceServer::new(server))
    .with_shutdown(ctrl_c())
    .await?;
```

**Deliverables**:

- `serve::Server` builder with TLS, mTLS, interceptor, and graceful
  shutdown support
- `into_tonic_builder()` escape hatch for advanced configuration
- Integration test: build and serve a tonic service through the builder
- Migrate knot-server to use the builder as validation

### Phase 2: Handler traits and service macro (deferred)

Handler traits that formalize the two RPC patterns:

```rust
/// Streaming RPC: request in, lazyflow pipe of responses out.
pub trait StreamingHandler<Req, Resp>: Send + Sync + 'static {
    fn handle(&self, req: Request<Req>) -> Result<Pipe<Resp>, Status>;
}

/// Unary RPC: request in, single response out.
pub trait UnaryHandler<Req, Resp>: Send + Sync + 'static {
    fn handle(
        &self,
        req: Request<Req>,
    ) -> Pin<Box<dyn Future<Output = Result<Response<Resp>, Status>> + Send>>;
}
```

And a proc macro that generates the tonic trait impl from handler
closures, eliminating the async_trait boilerplate entirely:

```rust
lazyflow_grpc::service! {
    proto: my_proto::my_service_server::MyService,

    // Streaming RPC: return Pipe<T>
    rpc query(req: QueryRequest) -> stream QueryResponse {
        build_query_pipe(req)?
    }

    // Unary RPC: return T directly
    rpc mutate(req: MutateRequest) -> MutateResponse {
        handle_mutate(req).await?
    }
}
```

This is deferred because:
- Proc macros over generated code are fragile
- Phase 1 already eliminates the server bootstrap boilerplate
- The trait impl boilerplate is mechanical but not large (5-10 lines
  per RPC)
- Real usage feedback from Phase 1 will inform whether the macro is
  worth the complexity

### Not in scope

- **Replacing tonic**: lazyflow-grpc is a convenience layer over tonic,
  not a replacement. Dropping tonic would require reimplementing HTTP/2
  transport, gRPC framing, protobuf codegen integration, service
  routing, metadata/trailers, and status codes -- roughly the entirety
  of tonic. tonic remains a hard dependency at every layer (codegen,
  transport, routing, wire format). This is a deliberate choice: tonic
  is mature and well-maintained; duplicating it would be high cost for
  no architectural benefit.
- **Custom codecs**: lazyflow-grpc uses tonic's default prost codec.
  Alternative codecs (flatbuffers, bincode) are tonic-level concerns.
- **Load balancing / discovery**: tonic's Channel handles this.
- **Health checks / reflection**: tonic provides these as optional
  Tower layers. lazyflow-grpc's builder should forward them, not reimplement.
- **Client-streaming / bidi handlers**: Server-side receipt of client
  streams as `Pipe<T>`. Natural extension but deferred until the
  server builder API is validated with unary + server-streaming.

## Consequences

### Positive

- Eliminates ~60 lines of server bootstrap boilerplate per service
- Establishes lazyflow-grpc as a cohesive server framework, not just an
  adapter library
- tonic remains the transport -- no reimplementation, no version fork
- Streaming and unary handlers share a consistent builder API
- Users who outgrow the builder can drop to raw tonic without rewriting
  their handlers (the trait impls are standard tonic)
- Validates the pattern before investing in the Phase 2 macro

### Negative

- Another abstraction layer over tonic -- users debugging transport
  issues must look through lazyflow-grpc to tonic. Mitigated by keeping
  the layer thin and transparent (builder just delegates).
- Server builder duplicates tonic's `Server::builder` surface. If
  tonic adds new configuration options, lazyflow-grpc must forward them.
  Mitigated by exposing an escape hatch (`into_tonic_builder()`).
- Phase 2 macro is speculative. If Phase 1 proves sufficient, the
  macro may never be needed. This is acceptable -- deferral is the
  point.

### Risks

- **tonic version coupling**: The builder wraps tonic's `Server` type.
  tonic 0.12 -> 0.13 breaking changes would require lazyflow-grpc updates.
  Mitigation: same as ADR-003 -- pin minor version, keep wrapper thin.
- **Feature creep**: Pressure to add every tonic feature to the builder
  (connection limits, keepalive, HTTP/2 tuning). Mitigation: expose
  `into_tonic_builder()` for advanced use and keep the lazyflow-grpc
  builder minimal (TLS, interceptor, graceful shutdown).
- **Macro complexity** (Phase 2): Proc macros over tonic-generated
  code may break on tonic version bumps if generated trait signatures
  change. Mitigation: defer until Phase 1 proves the handler pattern,
  then decide if the ergonomic gain justifies the maintenance cost.

## Implementation notes (Phase 1, 2026-09-30)

`lazyflow_grpc::serve::Server` (`lazyflow-grpc/src/serve.rs`), builder
methods `.tls()`, `.client_ca()`, `.client_auth_optional()` (all gated
behind a new `tls` Cargo feature, `tonic/tls-ring`; not every service needs
TLS and rustls is not free to compile in), `.interceptor()`, `.build()`.

Two deliberate deviations from the pseudocode above, both narrowing scope
rather than widening it:

- `.build()` returns `tonic::transport::Server<L>` directly (the real
  tonic type) instead of a separate wrapper with its own `.serve()`
  variants. Tonic's `Server<L>` already has `.serve(addr, svc)`,
  `.serve_with_shutdown(addr, svc, signal)`, `.serve_with_incoming(...)`,
  and `.add_service(...)` built in; re-wrapping them would mean copying
  tonic's own (nontrivial) generic trait bounds for no behavioral gain,
  and would be exactly the "server builder duplicates tonic's `Server`
  surface" risk this ADR already names. `.build()` *is* the escape hatch:
  there is no separate `into_tonic_builder()`.
- `.interceptor()` is called at most once per builder chain (chaining a
  second call is still expressible (it wraps the whole
  `Server<Stack<..., L>>` again), but is not the ergonomic path. The
  ADR's own Decision text scoped Phase 1 to "handler traits and routing"
  as Phase 2; multi-interceptor composition is the same shape of
  question and is left to the caller (compose checks inside one
  `Interceptor` impl) rather than adding stacking sugar nobody asked for
  yet.

TDD: `lazyflow-grpc/tests/serve_integration.rs` gained
`builder_server_round_trips_through_tonic_serve_with_incoming` (the core
claim: `.build()`'s output behaves exactly like a hand-built
`tonic::transport::Server`), `builder_server_stops_on_shutdown_signal`
(graceful shutdown unaffected by the wrapper),
`builder_server_interceptor_rejects_before_the_service_runs` (the auth
use case named in Context), and `builder_server_tls_handshake_round_trips`
(gated `#[cfg(feature = "tls")]`, a real TLS handshake against a
`rcgen`-generated self-signed certificate). `cargo test --workspace`
(default features) and `cargo test -p lazyflow-grpc --features tls` both
green; `cargo clippy --workspace --all-targets -- -D warnings` clean for
both feature combinations; `cargo fmt --all` clean.

Phase 1's own "migrate knot-server to use the builder as validation"
deliverable is not done as part of this change: knot-server pins
`lazyflow-grpc` from crates.io, not this checkout, so migrating it needs a
version bump and publish first, tracked separately.
