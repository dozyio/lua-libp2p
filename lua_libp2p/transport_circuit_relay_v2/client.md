# lua_libp2p.transport_circuit_relay_v2.client

Circuit Relay v2 client.
Creates relay reservations and tracks local reservation state.

### Libp2pRelayClient

```lua
Libp2pRelayClient
```

#### Fields

- `connect`: `fun(self: Libp2pRelayClient, relay_addr: string|table, destination_peer_id: string, opts?: table):table|nil, table|nil`
- `reserve`: `fun(self: Libp2pRelayClient, relay_addr: string|table, opts?: table):table|nil, table|nil`

### Libp2pRelayClientConfig

```lua
Libp2pRelayClientConfig
```

#### Fields

- `connect_timeout`: `number?`
- `reserve_timeout`: `number?`

