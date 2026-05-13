# lua_libp2p.port_mapping.nat_pmp.client

NAT-PMP client.

### Libp2pNatPmpClient

```lua
Libp2pNatPmpClient
```

#### Fields

- `close`: `fun(self: Libp2pNatPmpClient):true`
- `external_address`: `fun(self: Libp2pNatPmpClient):table|nil, table|nil`
- `map`: `fun(self: Libp2pNatPmpClient, opts: table):table|nil, table|nil`

### Libp2pNatPmpClientConfig

```lua
Libp2pNatPmpClientConfig
```

#### Fields

- `gateway`: `string?` - Gateway IP address.
- `retries`: `integer?` - Request retry count.
- `socket_factory`: `function?` - Test/custom UDP socket factory.
- `timeout`: `number?` - UDP request timeout.

