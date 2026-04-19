# Milestone 4 Plan: Noise Security

## Goal
Establish authenticated encrypted channels using libp2p `/noise`.

## Tasks
1. Implement Noise XX handshake state machine.
2. Implement libp2p handshake payload encoding/decoding.
3. Bind Noise static keys to libp2p identity keys via signatures.
4. Expose secure read/write channel interface post-handshake.
5. Add interop tests against a go-libp2p test peer.

## Deliverables
- `security/noise` module.
- Handshake payload codec.
- Interop tests and fixtures.

## Exit Criteria
- Lua node completes `/noise` handshake with go-libp2p.
- Identity binding verification passes for both inbound and outbound.
