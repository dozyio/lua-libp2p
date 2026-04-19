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
- Implement listener and dialer abstraction.
- Add connection lifecycle (open, close, timeouts, backpressure basics).
- Normalize multiaddr <-> socket address parsing for `/ip4/.../tcp/...`.
- Done when: two Lua peers can connect and exchange raw bytes.

## Milestone 3: multistream-select
- Implement protocol negotiation framing for `/multistream/1.0.0`.
- Support both inbound handler registration and outbound protocol selection.
- Handle `na` and fallback correctly (minimal path only).
- Done when: peers negotiate a toy protocol over one TCP connection.

## Milestone 4: Noise Security
- Implement `/noise` transport handshake (XX pattern, libp2p payload).
- Bind static key to libp2p identity key via signature payload.
- Expose secure channel read and write post-handshake.
- Done when: Lua peer completes secure handshake with a go-libp2p test peer.

## Milestone 5: Yamux Multiplexing
- Implement `/yamux/1.0.0` frame encode/decode and stream IDs.
- Support stream open, data, close, reset, and window updates (minimal flow control).
- Handle concurrent streams over one secure connection.
- Done when: multiple logical streams run in parallel over one connection.

## Milestone 6: Identify Protocol
- Implement `/ipfs/id/1.0.0` message encode/decode.
- Exchange supported protocols, listen addresses, observed address, and public key/PeerId fields.
- Update peerstore from received identify info.
- Done when: Lua node identifies with go-libp2p and stores peer metadata.

## Milestone 7: Ping Protocol
- Implement `/ipfs/ping/1.0.0` handler (echo payload).
- Implement client ping API and RTT measurement.
- Add simple health/keepalive hook.
- Done when: Lua <-> go-libp2p ping works reliably.

## Milestone 8: Peerstore (In-Memory)
- Store peer keys, addresses, protocols, last seen, RTT, tags, and TTL.
- Add a basic address selection strategy for dialing.
- Add garbage collection/expiry rules for stale records.
- Done when: identify and ping data persists per peer during runtime.

## Milestone 9: Interop + Hardening Pass
- Build integration matrix: Lua <-> Lua and Lua <-> go-libp2p for identify/ping.
- Add robustness checks for malformed frames and protocol errors.
- Tune timeouts, max frame sizes, and defensive parsing.
- Done when: stable repeated connect/identify/ping under stress.

## Milestone 10: Minimal Public API + Docs
- Expose tiny host API (`new_host`, `listen`, `dial`, `ping`, `handle`).
- Document supported protocols and explicit non-goals.
- Provide one runnable example (connect + identify + ping).
- Done when: user can copy example and interop in under 5 minutes.
