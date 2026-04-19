# Milestone 9 Plan: Interop + Hardening

## Goal
Validate reliability and robustness under realistic and adversarial conditions.

## Tasks
1. Build integration matrix for Lua<->Lua and Lua<->go-libp2p (identify + ping).
2. Add malformed input tests for multistream, Noise payloads, and yamux frames.
3. Add stress tests for repeated connect/handshake/stream open/close loops.
4. Tune limits (timeouts, max frame size, stream caps) and error surfacing.
5. Document known limitations and defensive defaults.

## Deliverables
- Integration and robustness test suites.
- Default limit configuration and docs.

## Exit Criteria
- Repeated stress runs pass without leaks/crashes.
- Protocol parsing rejects malformed data safely.
