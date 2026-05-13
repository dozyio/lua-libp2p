# lua_libp2p.protocol_identify.service

Identify protocol service.
Registers `/ipfs/id/*` handlers and provides request helpers.

### Libp2pIdentifyConfig

```lua
Libp2pIdentifyConfig
```

#### Fields

- `include_push`: `boolean?` - Register `/ipfs/id/push/1.0.0`. Default: true.
- `io_timeout`: `number?` - Identify stream IO timeout.
- `poll_interval`: `number?` - Scheduler poll interval while waiting.
- `run_on_connection_open`: `boolean?` - Run identify after peer connection open. Default: true.
- `timeout`: `number?` - Identify request timeout.

