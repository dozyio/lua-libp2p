# lua_libp2p.transport_tcp.transport

Poll-based TCP transport implementation.

### Libp2pTcpConfig

```lua
Libp2pTcpConfig
```

#### Fields

- `accept_batch`: `integer?`
- `keepalive`: `boolean?` - Defaults to enabled.
- `keepalive_initial_delay`: `number?` - Defaults to 0 when keepalive is enabled.
- `listen_backlog`: `integer?`
- `nodelay`: `boolean?` - Defaults to enabled.

