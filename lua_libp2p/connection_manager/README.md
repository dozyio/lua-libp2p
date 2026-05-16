# lua_libp2p.connection_manager

Connection manager with dial queue and reuse policy.

### Libp2pConnectionManagerConfig

```lua
Libp2pConnectionManagerConfig
```

#### Fields

- `address_dial_timeout`: `number?`
- `dial_timeout`: `number?`
- `grace_period`: `number?`
- `high_water`: `integer?`
- `low_water`: `integer?`
- `max_connections`: `integer?`
- `max_connections_per_peer`: `integer?`
- `max_dial_addrs`: `integer?` - Alias for `max_peer_addrs_to_dial`.
- `max_dial_queue_length`: `integer?`
- `max_inbound_connections`: `integer?`
- `max_outbound_connections`: `integer?`
- `max_parallel_dials`: `integer?`
- `max_peer_addrs_to_dial`: `integer?`
- `pending_dial_ttl`: `number?`
- `silence_period`: `number?`

### Libp2pConnectionManagerInstance

```lua
Libp2pConnectionManagerInstance
```

#### Fields

- `can_open_connection`: `fun(self: Libp2pConnectionManagerInstance, state: table):boolean|nil, table|nil`
- `on_connection_closed`: `(fun(self: Libp2pConnectionManagerInstance, entry: table):any)?`
- `on_connection_opened`: `(fun(self: Libp2pConnectionManagerInstance, entry: table):boolean|nil, table|nil)?`
- `open_connection`: `(fun(self: Libp2pConnectionManagerInstance, peer_or_addr: string|table, opts?: table):table|nil, table|nil, table|nil)?`
- `stats`: `(fun(self: Libp2pConnectionManagerInstance):table)?`

