# lua-libp2p

Minimal, interoperable libp2p building blocks for Lua.

## Current scope

This repo currently includes:

- Base module layout (`transport`, `security`, `muxer`, `protocol`, `crypto`, `peerstore`)
- Shared error and logging helpers
- Ed25519, RSA, ECDSA, and Secp256k1 identity + PeerId + multiformat helpers
- Multiaddr parsing/formatting + binary codec subset (`/ip4`, `/ip6`, `/dns*`, `/tcp`, `/udp`, `/quic-v1`, `/p2p`)
- Peer discovery abstraction + host-level bootstrap discovery config (dnsaddr-capable by default)
- Address manager for listen, announce, no-announce, observed, and relay advertisement sources
- AutoNAT v2 client service for address reachability checks and dial-back nonce verification
- UPnP IGD/SSDP NAT mapping service for creating public TCP/UDP port mappings from private transport listen addrs
- Circuit relay v2 client/AutoRelay support for reservations, relayed address advertisement, Stop handling, and reservation lifecycle events
- AutoRelay currently does not use AutoNAT reachability decisions; observed addresses are collected through identify but are not advertised by default
- Multibase/multiformat primitives (base58btc, base32, varint, multihash, CIDv1)
- Signed envelope + peer record encode/sign/verify primitives
- Kademlia kbucket routing-table module (`lua_libp2p.kbucket`)
- Kademlia DHT module with routing-table integration, FIND_NODE wire codec/handler path, public/private/custom address filtering, default libp2p bootstrapper list, discovery-driven bootstrap, and concurrent random-walk refresh (`lua_libp2p.kad_dht`)
- TCP transport with `/ip4/.../tcp/...` multiaddr parsing, dial/listen, and connection lifecycle controls
- multistream-select framing and protocol negotiation (`/multistream/1.0.0`)
- noise-libp2p XX handshake + secure channel framing primitives (`/noise`)
- noise identity signing/verification supports Ed25519, RSA, ECDSA, and Secp256k1 public keys
- plaintext secure-channel compatibility handshake (`/plaintext/2.0.0`) for testing only
- identify protocol message codec (protobuf framing, binary multiaddr fields, optional `signedPeerRecord`), basic request/response, push helpers, and multi-message merge utility (`/ipfs/id/1.0.0`, `/ipfs/id/push/1.0.0`)
- ping protocol echo + RTT helper (`/ipfs/ping/1.0.0`)
- perf protocol upload/download helper and handler (`/perf/1.0.0`)
- yamux stream multiplexer foundation (`/yamux/1.0.0`) with frame codec, multi-stream sessions, and a single-reader pump path
- transport-agnostic connection/stream abstraction with pluggable stream session support
- connection upgrader pipeline (security + muxer negotiation) for plaintext+yamux and noise+yamux
- host/node API with lifecycle (`start`/`stop`) and stream operations (`dial`, `new_stream`, `handle`)
- Host behavior is configured at `new(...)`; `start()` takes no options
- `host:dial(target, { force = true })` opens a fresh connection attempt instead of reusing or coalescing an existing connection; use `require_unlimited_connection = true` when a relayed limited connection must not be reused.
- Lightweight integration test harness

## Project layout

- `lua_libp2p/init.lua`: package entry point
- `lua_libp2p/transport_tcp`: TCP transport implementations (`luv`, `luv-native`, and socket compatibility helpers)
- `lua_libp2p/connection_encrypter_noise`: Noise connection encrypter protocol
- `lua_libp2p/connection_encrypter_plaintext`: Plaintext v2 connection encrypter protocol
- `lua_libp2p/muxer`: stream multiplexer abstractions
- `lua_libp2p/protocol_identify`: identify protocol + service
- `lua_libp2p/protocol_ping`: ping protocol + service
- `lua_libp2p/protocol_perf`: perf protocol + service
- `lua_libp2p/multistream_select`: multistream-select protocol
- `lua_libp2p/crypto`: key and signature helpers
- `lua_libp2p/multiformats`: varint, multibase, multihash, cid helpers
- `lua_libp2p/multiaddr.lua`: multiaddr parsing/formatting/utilities
- `lua_libp2p/dnsaddr.lua`: dnsaddr resolution abstraction utilities (resolver-injected)
- `lua_libp2p/autonat`: AutoNAT v2 client service
- `lua_libp2p/upnp`: SSDP discovery, UPnP IGD SOAP client, and UPnP NAT service
- `lua_libp2p/bootstrap.lua`: default bootstrap peer list and bootstrapper helpers
- `lua_libp2p/address_manager.lua`: advertised address selection and relay address tracking
- `lua_libp2p/discovery`: pluggable peer discovery manager
- `lua_libp2p/peer_discovery_bootstrap`: bootstrap peer discovery source
- `lua_libp2p/network`: connection/stream abstraction layer
- `lua_libp2p/record`: signed envelopes and peer routing records
- `lua_libp2p/host/init.lua`: host/node setup (`start`, `dial`, `new_stream`, `handle`, `close`)
- `lua_libp2p/peerstore`: peer metadata storage
- `lua_libp2p/kbucket.lua`: Kademlia kbucket routing table module
- `lua_libp2p/kad_dht`: Kademlia DHT module and wire helpers
- `lua_libp2p/transport_circuit_relay_v2`: circuit relay v2 protocol, client, and AutoRelay
- `tests`: test harness and integration tests

## Runtime assumptions

- Lua 5.4.8 (recommended)
- Lua 5.5 is not currently supported for the full dependency set because `luaossl` does not build against Lua 5.5 yet.
- LuaRocks for dependency management
- Runtime dependencies:
- `luasocket`
- `lua-protobuf`
- `luasodium` (ed25519)
- `luv` (libuv runtime backend)
- `luaossl` (native RSA noise identity verification)
- Tests run with the Lua interpreter directly

Host runtime note:
- `runtime = "auto"` is the default and resolves to `luv`.
- `runtime = "luv"` enables a libuv-backed internal scheduler and uses the native `luv` TCP transport.
- With `runtime = "luv"` and `blocking = false`, a uv loop must be running in the process.

Networking note:
- `lua_libp2p.network.MESSAGE_SIZE_MAX` is `4 * 1024 * 1024` bytes, matching the 4 MiB practical KAD RPC cap used by Go/JS implementations.
- The connection abstraction is stream-session based, not yamux-specific. TCP+Noise+Yamux is the current default stack, but future transports with native stream multiplexing, such as QUIC, can provide their own session implementation.
- A stream session is expected to provide `open_stream()`, `accept_stream_now()`, optional `process_one()`, optional `has_waiters()`, and optional `close()`.

## AutoRelay, Bootstrap, And Addresses

Host behavior is configured in `host.new(...)`; `start()` takes no options. Bootstrap discovery is configured at the host level and is shared by services such as the Kademlia DHT and AutoRelay.

Enable bootstrap discovery with the default public libp2p bootstrappers:

```lua
local host = require("lua_libp2p.host")
local identify_service = require("lua_libp2p.protocol_identify.service")
local kad_dht_service = require("lua_libp2p.kad_dht")
local peer_discovery_bootstrap = require("lua_libp2p.peer_discovery_bootstrap")

local h = assert(host.new({
  services = {
    identify = { module = identify_service },
    kad_dht = { module = kad_dht_service },
  },
  peer_discovery = {
    bootstrap = {
      module = peer_discovery_bootstrap,
      config = {}, -- empty config means use lua_libp2p.bootstrap defaults
    },
  },
}))

assert(h:start())
```

Bootstrap config notes:
- `/dnsaddr` bootstrap peers are resolved by `lua_libp2p.dnsaddr.default_resolver` unless a resolver is supplied.
- Bootstrap peers are merged into the host peerstore and tagged as `bootstrap`.
- Bootstrap discovery dials on startup by default; set `dial_on_start = false` to only seed the peerstore.
- DHT service config defaults `kad_dht.peer_discovery` to `host.peer_discovery`.

Address manager status:
- The host owns `host.address_manager`.
- It tracks listen addrs, explicit announce addrs, no-announce addrs, observed addrs, and relay addrs.
- Private addresses are automatically marked `status = "private"` with JS-style address metadata such as `type = "transport"` or `type = "observed"`.
- Public mappings added by UPnP use `type = "ip-mapping"`; relayed reservation addresses use `type = "transport"`.
- `host:get_multiaddrs_raw()` returns selected advertised addrs without appending the local peer id.
- `host:get_multiaddrs()` appends `/p2p/<self>` where needed.
- Observed addrs from identify are collected but are not advertised by default.
- Relayed `/p2p-circuit` addrs are advertised only while an AutoRelay reservation is active.

AutoNAT v2 client status:
- `services = { autonat = { module = require("lua_libp2p.autonat.client") } }` installs the client-side `/libp2p/autonat/2/dial-back` handler.
- `host.autonat:check(server, { addrs = { ... } })` opens `/libp2p/autonat/2/dial-request` to an AutoNAT v2 server.
- Dial-back nonce verification is tracked per request; the client responds with `DialBackResponse OK` for matching pending nonces.
- Anti-amplification `DialDataRequest` is supported with configurable byte caps.
- Results are stored on the address manager when available and emitted via `autonat:address:checked`, `autonat:address:reachable`, `autonat:address:unreachable`, and `autonat:request:failed` events.
- AutoNAT v2 server mode is not implemented.

UPnP NAT status:
- `services = { upnp_nat = { module = require("lua_libp2p.upnp.nat") } }` enables SSDP gateway discovery and UPnP IGD SOAP port mapping.
- The service maps eligible private, non-loopback IP transport listen addrs and adds external addresses to the address manager as `type = "ip-mapping"`.
- By default mapped addresses are added as unverified and should be confirmed by AutoNAT before advertisement.
- Set `upnp_nat = { auto_confirm_address = true }` to immediately advertise mapped addresses, useful for local testing.
- Events emitted: `upnp_nat:mapping:active` and `upnp_nat:mapping:failed`.
- If the gateway external IP is private, the service reports likely double NAT and does not add mappings.

AutoRelay status:
- Circuit relay v2 Hop/Stop protocol codecs and stream helpers are implemented.
- `services = { autorelay = { module = require("lua_libp2p.transport_circuit_relay_v2.autorelay") } }` installs the Stop handler and enables relay reservation management.
- `/p2p-circuit` in `listen_addrs` requires the `autorelay` service and is treated as a relay listen capability, not a TCP listener.
- AutoRelay discovers relay candidates from peers advertising `/libp2p/circuit/relay/0.2.0/hop` and can also reserve explicitly configured relays.
- Active reservations publish relayed `/p2p-circuit` addrs through the address manager; removed reservations remove those addrs.
- AutoRelay emits reservation lifecycle events: `relay:reservation:active`, `relay:reservation:removed`, and `relay:reservation:failed`.
- Default reservation target is small (`max_reservations = 2`), so seeing one or two active relay peers is expected.
- AutoRelay does not currently gate reservations on AutoNAT reachability results.

## Install dependencies (LuaRocks)

```bash
brew install libsodium
luarocks make lua-libp2p-0.1.0-1.rockspec
```

If using Homebrew `lua@5.4`, put Lua 5.4 first on `PATH` and load the matching LuaRocks paths before running examples/tests:

```bash
export PATH="/opt/homebrew/opt/lua@5.4/bin:$PATH"
eval "$(luarocks --lua-version=5.4 --lua-dir=/opt/homebrew/opt/lua@5.4 path)"
```

## Key serialization

- Public keys for PeerId are encoded as libp2p `PublicKey` protobuf bytes (`Type`, `Data`) using deterministic field order/minimal varints.
- Private/public key protobuf helpers live in `lua_libp2p/crypto/key_pb.lua`.
- Generic key helpers live in `lua_libp2p/crypto/keys.lua`.
- For Ed25519, protobuf `Data` is raw key bytes:
  - public key: 32 bytes
  - private key: 64 bytes (`[private][public]`)
- Local `ed25519` key save/load in `lua_libp2p/crypto/ed25519.lua` stores the raw 64-byte private key bytes directly.
- For RSA public keys, protobuf `Data` is DER-encoded PKIX/SPKI public key bytes.
- For ECDSA public keys, protobuf `Data` is DER-encoded ASN.1 public key bytes.
- For Secp256k1 public keys, protobuf `Data` is Bitcoin EC point bytes (compressed or uncompressed).
- RSA, ECDSA, and Secp256k1 local private keys generated by `lua_libp2p.crypto.keys` are currently retained as PEM for signing; public wire encoding remains spec-compatible.

Supported key operations:

| Operation | Ed25519 | RSA | ECDSA | Secp256k1 |
| --- | --- | --- | --- | --- |
| PeerId derivation | yes | yes | yes | yes |
| Spec vectors | yes | yes | yes | yes |
| Local key generation | yes | yes | yes | yes |
| Local signing | yes | yes | yes | yes |
| Signature verification | yes | yes | yes | yes |
| Host identity | yes | yes | yes | yes |
| Noise identity signing/verification | yes | yes | yes | yes |
| Signed envelopes | yes | yes | yes | yes |
| Plaintext exchange helper | yes | yes | yes | yes |

Example identity generation:

```lua
local keys = require("lua_libp2p.crypto.keys")
local host = require("lua_libp2p.host")

local identity = assert(keys.generate_keypair("ed25519"))
local h = assert(host.new({
  identity = identity,
  runtime = "auto",
}))
```

## Run tests

```bash
lua tests/run.lua
```

Or via Make:

```bash
make test
make test-luv-native
```

Yamux interop check against go-yamux:

```bash
make interop-yamux-go
make interop-yamux-go-reverse
make interop-yamux-go-luv-native
make interop-yamux-go-reverse-luv-native

# Noise interop checks against go-libp2p Noise
make interop-noise-go
make interop-noise-go-reverse
make interop-noise-go-luv-native
make interop-noise-go-reverse-luv-native

# JS perf interop check against lua host
make interop-perf-js
make interop-perf-js-luv-native
```

Note: multiaddr conformance tests include a go/js-derived vector set plus an explicit
go strictness delta list (tracked in `tests/helpers/multiaddr_go_deltas.lua`).

## DHT bootstrap demo

```bash
# terminal 1
lua examples/kad_dht_bootstrap_demo.lua server

# terminal 2 (paste a /p2p listen addr printed by terminal 1)
lua examples/kad_dht_bootstrap_demo.lua client /ip4/127.0.0.1/tcp/12345/p2p/12D3KooW...

# public libp2p bootstrap crawl
lua examples/kad_dht_bootstrap_demo.lua client --default-bootstrap
```

The public bootstrap demo:
- resolves `/dnsaddr/bootstrap.libp2p.io`, filtering to currently dialable TCP addresses,
- bootstraps against the default public libp2p bootstrap peers,
- runs one `FIND_NODE` query,
- runs one concurrent random-walk refresh with `alpha = 10` and `disjoint_paths = 10`,
- prints summarized error counts and the closest routing-table peers at the end.

The DHT defaults are tuned for the public DHT:
- `k = 20`
- `alpha = 10`
- `disjoint_paths = 10`
- `max_concurrent_queries = 32`
- `max_message_size = lua_libp2p.network.MESSAGE_SIZE_MAX` (`4 MiB`)
- `address_filter = "public"`
- provider records expire after `48h`
- provider addresses learned through provider records expire after `24h`
- background reprovide interval is `22h` when `reprovider_enabled = true`

Address filtering is configurable:

```lua
local dht = assert(kad_dht.new(host, {
  address_filter = "public", -- "public", "private", or "all"
  max_concurrent_queries = 32,
  datastore = persistent_store, -- optional shared datastore for providers/records
  provider_datastore = provider_store, -- optional provider-only datastore
  record_datastore = record_store, -- optional value-record-only datastore
  peer_diversity_max_peers_per_ip_group = 3,
  peer_diversity_max_peers_per_ip_group_per_bucket = 2,
}))

local custom = assert(kad_dht.new(host, {
  address_filter = function(addr, ctx)
    -- ctx includes fields such as peer_id and purpose.
    return addr:match("/tcp/4001") ~= nil
  end,
}))
```

The address-scope filter is separate from transport dialability filtering. For now, the public crawler stores valid peer addresses but only dials currently supported TCP addresses.

The built-in IP-group diversity limiter is opt-in for the basic DHT, matching regular go-libp2p DHT behavior. IPv4 groups use `/16`, legacy Class A allocations use `/8`, and IPv6 currently falls back to `/32` grouping.
