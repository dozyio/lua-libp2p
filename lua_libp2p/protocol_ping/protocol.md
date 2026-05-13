# lua_libp2p.protocol_ping.protocol

Ping protocol primitives.

### Libp2pPingHandleOptions

```lua
Libp2pPingHandleOptions
```

#### Fields

- `max_messages`: `integer?` - Stop after N messages; otherwise runs until closed.

### Libp2pPingResult

```lua
Libp2pPingResult
```

#### Fields

- `payload`: `string`
- `rtt_seconds`: `number`

