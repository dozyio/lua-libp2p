# lua_libp2p.discovery

Discovery source aggregation.

### Libp2pDiscovery

```lua
Libp2pDiscovery
```

#### Fields

- `add_source`: `fun(self: Libp2pDiscovery, source: table|fun(opts?: table):table[]|nil, table|nil):true|nil, table|nil`
- `discover`: `fun(self: Libp2pDiscovery, opts?: table):table[]|nil, table|nil`

### Libp2pDiscoveryConfig

```lua
Libp2pDiscoveryConfig
```

#### Fields

- `sources`: `(table|fun(opts?: table):table[]|nil, table|nil)[]?`

### Libp2pDiscoverySource

 Discovery source aggregation.

```lua
fun(opts?: table):table[]|nil, table|nil
```

 Discovery source aggregation.

