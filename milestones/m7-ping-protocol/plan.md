# Milestone 7 Plan: Ping Protocol

Status: Complete

## Goal
Provide basic liveness and latency checks using `/ipfs/ping/1.0.0`.

## Tasks
1. Implement ping responder that echoes payload bytes.
2. Implement ping requester with RTT timing.
3. Add retry/timeout handling for unstable links.
4. Expose simple public API for one-shot and periodic ping.
5. Add interop tests with go-libp2p ping service.

## Deliverables
- `protocol/ping` module.
- API method(s) for ping.
- Interop and timeout behavior tests.

## Exit Criteria
- Lua can ping go-libp2p and receive stable RTT samples.
- Timeout and failure modes are surfaced cleanly.

Result: Exit criteria satisfied for ping interop. Ping responder/requester with RTT timing are implemented, stream-lifetime handling (`ping.handle`) supports repeated pings, and go-libp2p (vole) interop is validated.
