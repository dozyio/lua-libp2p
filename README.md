# lua-libp2p

Minimal, interoperable libp2p building blocks for Lua.

This project provides a Lua 5.4 libp2p host with TCP transport, Noise security, Yamux streams, common libp2p protocols, peer discovery, and Kademlia DHT support. The default runtime is `luv`.

## Status

`lua-libp2p` is under active development. The current stack focuses on interoperable TCP-based libp2p nodes:

- TCP transport over libuv
- Noise secure channel by default
- Experimental libp2p TLS support when explicitly enabled
- Yamux stream multiplexing
- Identify, ping, perf, AutoNAT, DCUtR, circuit relay v2, and AutoRelay services
- Peerstore, signed records, multiaddr, multiformats, and key helpers
- Kademlia DHT client/server components with bootstrap discovery

Plaintext security exists for tests and compatibility checks only.

## Requirements

- Lua 5.4
- LuaRocks
- `libsodium` on macOS before installing rocks

Lua 5.5 is not supported for the full dependency set because `luaossl` does not currently build cleanly against it.

## Install

```bash
brew install libsodium
luarocks make lua-libp2p-0.1.0-1.rockspec
```

When using Homebrew `lua@5.4`, load matching LuaRocks paths before running examples or tests:

```bash
export PATH="/opt/homebrew/opt/lua@5.4/bin:$PATH"
eval "$(luarocks --lua-version=5.4 --lua-dir=/opt/homebrew/opt/lua@5.4 path --bin)"
```

## Quick Start

```lua
local host = require("lua_libp2p.host")
local keys = require("lua_libp2p.crypto.keys")

local identity = assert(keys.generate_keypair("ed25519"))

local h = assert(host.new({
  identity = identity,
  listen_addrs = { "/ip4/127.0.0.1/tcp/0" },
}))

assert(h:start())

for _, addr in ipairs(h:get_multiaddrs()) do
  print(addr)
end
assert(h:stop())
```

Host behavior is configured in `host.new(...)`; `Host:start()` takes no options.

## Main Modules

- `lua_libp2p`: package entrypoint with common top-level modules.
- `lua_libp2p.host`: host lifecycle, listeners, dialing, streams, handlers, and services.
- `lua_libp2p.network`: connection and stream abstractions plus upgrade pipeline.
- `lua_libp2p.transport_tcp`: TCP transport implementations.
- `lua_libp2p.connection_encrypter`: security transport registry.
- `lua_libp2p.muxer`: stream multiplexer registry and Yamux support.
- `lua_libp2p.protocol_*`: built-in protocol services such as identify, ping, perf, and DCUtR.
- `lua_libp2p.kad_dht`: Kademlia DHT, routing table, provider records, and value records.
- `lua_libp2p.discovery` and `lua_libp2p.peer_discovery_bootstrap`: peer discovery sources.
- `lua_libp2p.peerstore`: peer metadata and address/protocol books.
- `lua_libp2p.record`: signed envelopes and peer records.
- `lua_libp2p.crypto`: key generation, signing, verification, and protobuf key encoding.
- `lua_libp2p.peerid`: PeerId derivation and parsing.
- `lua_libp2p.multiformats`: base32, base58btc, varint, multihash, CID, and multiaddr helpers.
- `lua_libp2p.autonat` and `lua_libp2p.port_mapping`: reachability and NAT mapping services.
- `lua_libp2p.transport_circuit_relay_v2` and `lua_libp2p.relay_discovery`: circuit relay client, AutoRelay, and relay candidate discovery.

Generated API docs are linked from `docs/api/README.md`. Module-local docs are generated beside source files, for example `lua_libp2p/host/README.md` and `lua_libp2p/kad_dht/README.md`.

## Configuration Shape

The host uses named config maps for selectable transports and muxers:

```lua
local h = assert(host.new({
  security_transports = { noise = true },
  muxers = { yamux = true },
}))
```

TLS is available only when explicitly selected and when `lua-fd-tls` is installed:

```lua
local h = assert(host.new({
  security_transports = { noise = true, tls = true },
  muxers = { yamux = true },
}))
```

Services are installed through the `services` map:

```lua
local h = assert(host.new({
  services = {
    identify = { module = require("lua_libp2p.protocol_identify.service") },
    ping = { module = require("lua_libp2p.protocol_ping.service") },
    kad_dht = { module = require("lua_libp2p.kad_dht") },
  },
}))
```

Bootstrap discovery is configured at the host level and can be shared by services:

```lua
local h = assert(host.new({
  peer_discovery = {
    bootstrap = {
      module = require("lua_libp2p.peer_discovery_bootstrap"),
      config = {},
    },
  },
}))
```

## Logging

Logs are structured text lines written to stderr.

```bash
LIBP2P_LOG=debug lua examples/kad_dht_get_closest_peers.lua
LIBP2P_LOG='kad_dht=debug,host=info,*=warn' lua examples/kad_dht_get_closest_peers.lua
```

## Examples

Examples live in `examples/`:

- `identify_ping_server.lua` and `identify_ping_client.lua`: basic host, identify, and ping flow.
- `kad_dht_bootstrap_demo.lua`: local or public bootstrap DHT demo.
- `kad_dht_find_value.lua`, `kad_dht_find_providers.lua`, and `kad_dht_get_closest_peers.lua`: DHT query examples.
- `autorelay_dht_demo.lua` and `dcutr_relay_dht_demo.lua`: relay and hole-punching demos.
- `autonat_server_hardened.lua`: AutoNAT server profile example.
- `upnp_nat_demo.lua`, `nat_pmp_demo.lua`, and `pcp_demo.lua`: NAT mapping service examples.

## Tests

```bash
make test
make test-luv-native
```

Focused single-test pattern:

```bash
lua -e 'package.path="./?.lua;./?/init.lua;"..package.path; local t=require("tests.unit.ping"); local ok,err=t.run(); if not ok then error(err) end; print(t.name)'
```

Interop targets are available through the `Makefile`, including Yamux, Noise, TLS, DCUtR, and JS perf checks.

## Docs

Generate docs with LuaLS:

```bash
make docs
```

The generated index is `docs/api/README.md`.
