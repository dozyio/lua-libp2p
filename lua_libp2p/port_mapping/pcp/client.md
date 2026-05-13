# lua_libp2p.port_mapping.pcp.client

PCP client.

### Libp2pPcpClient

```lua
Libp2pPcpClient
```

#### Fields

- `close`: `fun(self: Libp2pPcpClient):true`
- `map`: `fun(self: Libp2pPcpClient, opts: table):table|nil, table|nil`

### Libp2pPcpClientConfig

```lua
Libp2pPcpClientConfig
```

#### Fields

- `gateway`: `string?` - Gateway IP address.
- `retries`: `integer?` - Request retry count.
- `socket_factory`: `function?` - Test/custom UDP socket factory.
- `timeout`: `number?` - UDP request timeout.

