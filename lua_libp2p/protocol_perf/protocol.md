# lua_libp2p.protocol_perf.protocol

Perf protocol helpers and measurement utilities.

### Libp2pPerfOptions

 Perf protocol helpers and measurement utilities.

```lua
Libp2pPerfOptions
```

 Perf protocol helpers and measurement utilities.

#### Fields

- `write_block_size`: `integer?` - Send chunk size. Default: module default.
- `yield_every_bytes`: `integer?` - Cooperative yield cadence.

### Libp2pPerfReport

```lua
Libp2pPerfReport
```

#### Fields

- `download_bytes`: `integer`
- `time_seconds`: `number`
- `upload_bytes`: `integer`

### Libp2pPerfStats

```lua
Libp2pPerfStats
```

#### Fields

- `download_bytes`: `integer`
- `uploaded_bytes`: `integer`

