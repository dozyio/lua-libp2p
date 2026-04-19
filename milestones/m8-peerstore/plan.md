# Milestone 8 Plan: Peerstore (In-Memory)

## Goal
Maintain runtime peer metadata needed for dialing and protocol use.

## Tasks
1. Define peer record schema (keys, addrs, protocols, last seen, RTT, tags, TTL).
2. Implement upsert/read APIs with merge rules.
3. Implement address scoring/selection for outbound dialing.
4. Implement expiry and garbage collection for stale entries.
5. Add tests for update ordering and TTL behavior.

## Deliverables
- `peerstore` module with in-memory backend.
- Tests for merge, selection, and expiry behavior.

## Exit Criteria
- Identify and ping updates persist and are queryable.
- Dialer uses peerstore-selected addresses deterministically.
