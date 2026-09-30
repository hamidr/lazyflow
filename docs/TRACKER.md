# Document Tracker

## ADRs

| # | Title | Status | Phase | Depends On | Notes |
|---|-------|--------|-------|------------|-------|
| [ADR-001](ADR-001-operator-fusion-strategy.md) | Operator Fusion Strategy | Deferred | -- | ADR-002 | Deferred in favor of connectors; revisit after real users |
| [ADR-002](ADR-002-connector-strategy.md) | Connector Strategy | Accepted | Phase 2 shipped | -- | SSE shipped, WebSocket shipped; Phase 3 (Kafka/NATS) deferred |
| [ADR-003](ADR-003-grpc-connector.md) | gRPC Connector | Accepted | Phase 1 shipped | ADR-002 | Streaming source + server response shipped; Phase 2 (bidi) deferred |
| [ADR-004](ADR-004-grpc-server-handler.md) | gRPC Server Handler | Accepted | Phase 1 shipped | ADR-003 | Server builder shipped; Phase 2 (service macro) deferred |

Next ADR number: 005

## PRDs

| # | Title | Status | Phase | Depends On | Notes |
|---|-------|--------|-------|------------|-------|

Next PRD number: 001
