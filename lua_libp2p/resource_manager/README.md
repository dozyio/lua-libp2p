# lua_libp2p.resource_manager

Count-based resource manager.

This is a small, Go-inspired resource manager. It enforces hard limits at
reservation time for connections and streams across system, transient, peer,
and protocol scopes. Memory and file-descriptor accounting can be layered on
top of this API later without changing host integration points.

### Libp2pResourceLimits

```lua
Libp2pResourceLimits
```

#### Fields

- `connections`: `integer?`
- `connections_inbound`: `integer?`
- `connections_inbound_per_peer`: `integer?`
- `connections_outbound`: `integer?`
- `connections_outbound_per_peer`: `integer?`
- `connections_per_peer`: `integer?`
- `peer`: `table<string, Libp2pResourceLimits>?`
- `protocol`: `table<string, Libp2pResourceLimits>?`
- `protocol_peer`: `table<string, Libp2pResourceLimits>?`
- `streams`: `integer?`
- `streams_inbound`: `integer?`
- `streams_inbound_per_peer`: `integer?`
- `streams_outbound`: `integer?`
- `streams_outbound_per_peer`: `integer?`
- `streams_per_peer`: `integer?`
- `transient_connections`: `integer?`
- `transient_streams`: `integer?`

### Libp2pResourceManagerConfig

```lua
Libp2pResourceManagerConfig
```

#### Fields

- `connections`: `integer?`
- `connections_inbound`: `integer?`
- `connections_inbound_per_peer`: `integer?`
- `connections_outbound`: `integer?`
- `connections_outbound_per_peer`: `integer?`
- `connections_per_peer`: `integer?`
- `default_limits`: `boolean?` - Start from default limits unless false.
- `limits`: `Libp2pResourceLimits?` - Explicit nested limits map.
- `peer`: `table<string, Libp2pResourceLimits>?`
- `protocol`: `table<string, Libp2pResourceLimits>?`
- `protocol_peer`: `table<string, Libp2pResourceLimits>?`
- `streams`: `integer?`
- `streams_inbound`: `integer?`
- `streams_inbound_per_peer`: `integer?`
- `streams_outbound`: `integer?`
- `streams_outbound_per_peer`: `integer?`
- `streams_per_peer`: `integer?`
- `transient_connections`: `integer?`
- `transient_streams`: `integer?`

### Libp2pResourceManagerInstance

```lua
Libp2pResourceManagerInstance
```

#### Fields

- `close`: `(fun(self: Libp2pResourceManagerInstance):any)?`
- `open_connection`: `fun(self: Libp2pResourceManagerInstance, opts?: table):table|nil, table|nil`
- `open_stream`: `(fun(self: Libp2pResourceManagerInstance, opts?: table):table|nil, table|nil)?`
- `stats`: `(fun(self: Libp2pResourceManagerInstance):table)?`

