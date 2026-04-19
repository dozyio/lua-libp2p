# Milestone 2 Plan: TCP Transport

## Goal
Support raw connection establishment and byte exchange over TCP.

## Tasks
1. Implement listener abstraction for inbound connections.
2. Implement dialer abstraction for outbound connections.
3. Add connection lifecycle controls (timeouts, close semantics, basic backpressure).
4. Implement minimal multiaddr parsing for `/ip4/.../tcp/...`.
5. Add Lua-to-Lua transport integration tests.

## Deliverables
- `transport/tcp` module.
- Address parsing helpers.
- Integration tests for dial/listen and bidirectional byte flow.

## Exit Criteria
- Two Lua peers can connect and exchange bytes reliably.
- Timeouts and close paths are deterministic.
