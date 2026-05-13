# lua_libp2p.muxer.yamux

Yamux stream multiplexer implementation.

### Libp2pYamuxFrame

```lua
Libp2pYamuxFrame
```

#### Fields

- `flags`: `integer`
- `length`: `integer`
- `payload`: `string?`
- `stream_id`: `integer`
- `type`: `integer`
- `version`: `integer`

### Libp2pYamuxHeader

 Yamux stream multiplexer implementation.

```lua
Libp2pYamuxHeader
```

 Yamux stream multiplexer implementation.

#### Fields

- `flags`: `integer`
- `length`: `integer`
- `stream_id`: `integer`
- `type`: `integer`
- `version`: `integer`

### Libp2pYamuxSession

```lua
Libp2pYamuxSession
```

#### Fields

- `accept_stream_now`: `fun(self: Libp2pYamuxSession):Libp2pStream|nil`
- `close`: `fun(self: Libp2pYamuxSession):true`
- `open_stream`: `fun(self: Libp2pYamuxSession):Libp2pStream|nil, table|nil`
- `pump_ready`: `fun(self: Libp2pYamuxSession, max_frames?: integer):boolean|nil, table|nil`

### Libp2pYamuxSessionOptions

```lua
Libp2pYamuxSessionOptions
```

#### Fields

- `initial_stream_window`: `integer?`
- `max_accept_backlog`: `integer?`
- `max_ack_backlog`: `integer?`
- `role`: `('client'|'server')?`
- `scheduler_driven`: `boolean?`

