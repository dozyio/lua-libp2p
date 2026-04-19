# Milestone 11 Plan: Connection Manager

Status: Planned

## Goal
Keep connection count healthy and predictable under load while preserving high-value peers.

## Tasks
1. Add connection manager state for active connections and peer metadata inputs.
2. Implement configurable global high/low watermarks and per-peer limits.
3. Add scoring/prioritization inputs (tags, RTT, direction, recency, protected peers).
4. Implement pruning strategy that closes lowest-value connections first.
5. Expose APIs for `tag_peer`, `untag_peer`, `protect_peer`, and `unprotect_peer`.
6. Add tests for limit enforcement, pruning order, and protection behavior.

## Deliverables
- `network/connmgr` module (or equivalent integration in host/network layer).
- Configurable connection limit and pruning policy.
- Public APIs for tagging/protection.
- Unit/integration tests for pruning and limits.

## Exit Criteria
- Node enforces configured connection bounds over time.
- Under pressure, pruning is deterministic and respects protected peers.
- Interop workloads remain stable while connection churn is controlled.
