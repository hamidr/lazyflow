# ADR & PRD Tracker

GENERATED FILE -- do not edit. Rebuild with `docmeta-tracker <docs-root>`.
Every fact here comes from the frontmatter of the document it names; change the
document, then regenerate.

**Generated:** 2026-11-08

**Next PRD number:** 001

**Next ADR number:** 006

## ADRs

| # | Title | Status | Phases | Depends On | Summary | Flags |
| --- | --- | --- | --- | --- | --- | --- |
| [ADR-001](ADR-001-operator-fusion-strategy.md) | Operator Fusion Strategy | proposed | -- | ADR-002 | Deferred in favor of connectors; revisit after real users |  |
| [ADR-002](ADR-002-connector-strategy.md) | Connector Strategy | accepted | -- | -- | SSE and WebSocket shipped (Phase 2); Phase 3 Kafka/NATS brokers deferred |  |
| [ADR-003](ADR-003-grpc-connector.md) | gRPC Connector | accepted | -- | ADR-002 | Server streaming source and server response shipped (Phase 1); Phase 2 bidi deferred |  |
| [ADR-004](ADR-004-grpc-server-handler.md) | gRPC Server Handler | accepted | -- | ADR-003 | Server builder shipped 2026-09-30 (Phase 1); Phase 2 service macro deferred |  |
| [ADR-005](ADR-005-cooperative-budget-in-drive-loops.md) | Cooperative Budget in Pull Drive Loops | implementing | 1/13 | -- | One consume_budget helper in drive loops replaces YIELD_AFTER_EMPTY; stops starvation on ready sources |  |
