# lua_libp2p.peer_discovery_bootstrap

Bootstrap peer discovery source.

### Libp2pBootstrapDiscoveryConfig

 Bootstrap peer discovery source.

```lua
Libp2pBootstrapDiscoveryConfig
```

 Bootstrap peer discovery source.

#### Fields

- `dnsaddr_resolver`: `function?` - DNSAddr resolver override.
- `peers`: `string[]?` - Static bootstrap multiaddrs.
- `resolve_dnsaddr`: `boolean?` - Resolve `/dnsaddr` entries. Default: true.

### Libp2pBootstrapDiscoverySource

```lua
Libp2pBootstrapDiscoverySource
```

#### Fields

- `discover`: `fun(self: Libp2pBootstrapDiscoverySource, opts?: table):table[]|nil, table|nil`

