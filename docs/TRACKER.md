# ADR & PRD Tracker

GENERATED FILE -- do not edit. Rebuild with `docmeta-tracker <docs-root>`.
Every fact here comes from the frontmatter of the document it names; change the
document, then regenerate.

**Next PRD number:** 001

**Next ADR number:** 005

## ADRs

| # | Title | Status | Phases | Depends On | Summary | Flags |
| --- | --- | --- | --- | --- | --- | --- |
| [ADR-001](ADR-001-operator-fusion-strategy.md) | Operator Fusion Strategy | proposed | -- | ADR-002 | Deferred in favor of connectors; revisit after real users |  |
| [ADR-002](ADR-002-connector-strategy.md) | Connector Strategy | accepted | -- | -- | SSE and WebSocket shipped (Phase 2); Phase 3 Kafka/NATS brokers deferred |  |
| [ADR-003](ADR-003-grpc-connector.md) | gRPC Connector | accepted | -- | ADR-002 | Server streaming source and server response shipped (Phase 1); Phase 2 bidi deferred |  |
| [ADR-004](ADR-004-grpc-server-handler.md) | gRPC Server Handler | accepted | -- | ADR-003 | Server builder shipped 2026-09-30 (Phase 1); Phase 2 service macro deferred |  |
