# Minimal libp2p for Lua: Milestones

## Milestone 0: Project Skeleton
Status: Complete

- Define module layout (`transport`, `security`, `muxer`, `protocol`, `crypto`, `peerstore`).
- Pick runtime assumptions (Lua version, socket library, protobuf library, crypto bindings).
- Add basic logging, error model, and integration test harness.
- Done when: repo boots, tests run, and one dummy protocol loopback test passes.

## Milestone 1: Identity + PeerId
Status: Complete

- Implement ed25519 key generation, load, and save.
- Implement PeerId derivation from public key (libp2p-compatible encoding).
- Add signed message helpers (for Noise identity payload later).
- Done when: generated PeerId matches expected vectors and signatures verify.

## Milestone 2: TCP Transport
Status: Complete

- Implement listener and dialer abstraction.
- Add connection lifecycle (open, close, timeouts, backpressure basics).
- Normalize multiaddr <-> socket address parsing for `/ip4/.../tcp/...`.
- Done when: two Lua peers can connect and exchange raw bytes.

## Milestone 3: multistream-select
Status: Complete

- Implement protocol negotiation framing for `/multistream/1.0.0`.
- Support both inbound handler registration and outbound protocol selection.
- Handle `na` and fallback correctly (minimal path only).
- Done when: peers negotiate a toy protocol over one TCP connection.

## Milestone 4: Noise Security
Status: Complete

- Implement `/noise` transport handshake (XX pattern, libp2p payload).
- Bind static key to libp2p identity key via signature payload.
- Expose secure channel read and write post-handshake.
- Done when: Lua peer completes secure handshake with a go-libp2p test peer.

Note: Bidirectional Noise interop against go-libp2p test peers now passes (`make interop-noise-go` and `make interop-noise-go-reverse`).

## Milestone 5: Yamux Multiplexing
Status: Complete

Note: Yamux implementation and interop are complete and validated over TCP and Noise-secured connections.

- Implement `/yamux/1.0.0` frame encode/decode and stream IDs.
- Support stream open, data, close, reset, and window updates (minimal flow control).
- Handle concurrent streams over one secure connection.
- Done when: multiple logical streams run in parallel over one connection.

## Milestone 6: Identify Protocol
Status: Complete

- Implement `/ipfs/id/1.0.0` message encode/decode.
- Exchange supported protocols, listen addresses, observed address, and public key/PeerId fields.
- Update peerstore from received identify info.
- Done when: Lua node identifies with go-libp2p and stores peer metadata.

Note: `/ipfs/id/1.0.0` serving and parsing interop with go-libp2p (vole) is now validated, with populated protocol version, agent version, listen addresses, and protocol list.

## Milestone 7: Ping Protocol
Status: Complete

- Implement `/ipfs/ping/1.0.0` handler (echo payload).
- Implement client ping API and RTT measurement.
- Add simple health/keepalive hook.
- Done when: Lua <-> go-libp2p ping works reliably.

Note: Ping interop with go-libp2p (vole) is validated; responder/requester are implemented and the example host handles repeated ping payloads on a stream (`ping.handle`).

## Milestone 8: Peerstore (In-Memory)
Status: Planned

- Store peer keys, addresses, protocols, last seen, RTT, tags, and TTL.
- Add a basic address selection strategy for dialing.
- Add garbage collection/expiry rules for stale records.
- Done when: identify and ping data persists per peer during runtime.

## Milestone 9: Interop + Hardening Pass
Status: In Progress

- Build integration matrix: Lua <-> Lua and Lua <-> go-libp2p for identify/ping.
- Add robustness checks for malformed frames and protocol errors.
- Tune timeouts, max frame sizes, and defensive parsing.
- Done when: stable repeated connect/identify/ping under stress.

Note: Hardening progress includes select-driven polling, nonfatal error handling (`timeout`, `closed`, `decode`, `protocol`, `unsupported`), yamux flow-control fixes for large transfers, and JS perf interop (`/perf/1.0.0`) under Noise+yamux.

## Milestone 10: Minimal Public API + Docs
Status: In Progress

- Expose tiny host API (`new_host`, `start`, `dial`, `ping`, `handle`).
- Document supported protocols and explicit non-goals.
- Provide one runnable example (connect + identify + ping).
- Done when: user can copy example and interop in under 5 minutes.

Note: API ergonomics improved with host service registration (`services = { "identify", "ping", "perf" }`), peer/multiaddr convenience methods (`peer_id`, `get_multiaddrs`, `get_multiaddrs_raw`), and a stable example identity/port flow.

## Milestone 11: Connection Manager
Status: Planned

- Track active connections with per-peer and global limits.
- Add connection scoring inputs (tags, latency, direction, recency).
- Prune low-value connections under pressure while protecting critical peers.
- Expose hooks/APIs for tagging and temporary pinning.
- Done when: node stays within configured connection limits and can recover from connection pressure deterministically.

## Out-of-Order Progress Notes
- Implemented `/plaintext/2.0.0` handshake for debugging/interoperability testing.
- Implemented identify codec/flow with `signedPeerRecord` field support and merge helpers.
- Added signed envelope and peer routing record support (RFC0002/RFC0003 compatibility).
