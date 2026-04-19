# lua-libp2p

Minimal, interoperable libp2p building blocks for Lua.

## Current scope

This repo currently includes:

- Base module layout (`transport`, `security`, `muxer`, `protocol`, `crypto`, `peerstore`)
- Shared error and logging helpers
- Ed25519 identity + PeerId + multiformat helpers
- Multiaddr parsing/formatting + binary codec subset (`/ip4`, `/ip6`, `/dns*`, `/tcp`, `/udp`, `/quic-v1`, `/p2p`)
- Multibase/multiformat primitives (base58btc, base32, varint, multihash, CIDv1)
- Signed envelope + peer record encode/sign/verify primitives
- TCP transport with `/ip4/.../tcp/...` multiaddr parsing, dial/listen, and connection lifecycle controls
- multistream-select framing and protocol negotiation (`/multistream/1.0.0`)
- noise-libp2p XX handshake + secure channel framing primitives (`/noise`)
- plaintext secure-channel compatibility handshake (`/plaintext/2.0.0`) for testing only
- identify protocol message codec (protobuf framing, binary multiaddr fields, optional `signedPeerRecord`), basic request/response, push helpers, and multi-message merge utility (`/ipfs/id/1.0.0`, `/ipfs/id/push/1.0.0`)
- ping protocol echo + RTT helper (`/ipfs/ping/1.0.0`)
- perf protocol upload/download helper and handler (`/perf/1.0.0`)
- yamux stream multiplexer foundation (`/yamux/1.0.0`) with frame codec and multi-stream session basics
- transport-agnostic connection/stream abstraction with pluggable muxer session support
- connection upgrader pipeline (security + muxer negotiation) for plaintext+yamux and noise+yamux
- host/node API with lifecycle (`start`/`stop`) and stream operations (`dial`, `new_stream`, `handle`)
- Lightweight integration test harness

## Project layout

- `lua_libp2p/init.lua`: package entry point
- `lua_libp2p/transport`: transport abstractions
- `lua_libp2p/security`: secure channel abstractions
- `lua_libp2p/muxer`: stream multiplexer abstractions
- `lua_libp2p/protocol`: protocol implementations
- `lua_libp2p/crypto`: key and signature helpers
- `lua_libp2p/multiformats`: varint, multibase, multihash, cid helpers
- `lua_libp2p/multiaddr.lua`: multiaddr parsing/formatting/utilities
- `lua_libp2p/network`: connection/stream abstraction layer
- `lua_libp2p/record`: signed envelopes and peer routing records
- `lua_libp2p/host.lua`: host/node setup (`start`, `dial`, `new_stream`, `handle`, `close`)
- `lua_libp2p/peerstore`: peer metadata storage
- `tests`: test harness and integration tests

## Runtime assumptions

- Lua 5.4.x (recommended)
- LuaRocks for dependency management
- Runtime dependencies:
  - `luasocket`
  - `lua-protobuf`
  - `luasodium` (ed25519)
- Tests run with the Lua interpreter directly

## Install dependencies (LuaRocks)

```bash
brew install libsodium
luarocks make lua-libp2p-0.1.0-1.rockspec
```

## Key serialization (M1)

- Public keys for PeerId are encoded as libp2p `PublicKey` protobuf bytes (`Type`, `Data`) using deterministic field order/minimal varints.
- Private/public key protobuf helpers live in `lua_libp2p/crypto/key_pb.lua`.
- For Ed25519, protobuf `Data` is raw key bytes:
  - public key: 32 bytes
  - private key: 64 bytes (`[private][public]`)
- Local `ed25519` key save/load in `lua_libp2p/crypto/ed25519.lua` stores the raw 64-byte private key bytes directly.

## Run tests

```bash
lua tests/run.lua
```

Or via Make:

```bash
make test
```

Yamux interop check against go-yamux:

```bash
make interop-yamux-go
make interop-yamux-go-reverse

# Noise interop checks against go-libp2p Noise
make interop-noise-go
make interop-noise-go-reverse

# JS perf interop check against lua host
make interop-perf-js
```

Note: multiaddr conformance tests include a go/js-derived vector set plus an explicit
go strictness delta list (tracked in `tests/helpers/multiaddr_go_deltas.lua`).
