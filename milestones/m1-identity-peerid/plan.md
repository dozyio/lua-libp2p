# Milestone 1 Plan: Identity + PeerId

## Goal
Implement libp2p-compatible identity primitives using ed25519 and PeerId derivation.

## Tasks
1. Implement ed25519 key generation, serialization, and loading.
2. Implement signing and verification helpers.
3. Implement PeerId derivation from public key with correct multicodec/multihash encoding.
4. Add test vectors and round-trip tests (key -> PeerId, sign -> verify).

## Deliverables
- `crypto/ed25519` module.
- `peerid` module.
- Unit tests for vectors and round trips.

## Exit Criteria
- Known vectors pass.
- Generated identities are stable across process restarts.
