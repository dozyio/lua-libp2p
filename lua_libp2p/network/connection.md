# lua_libp2p.network.connection

Connection abstraction over secure muxed sessions.

### Libp2pConnection

```lua
Libp2pConnection
```

#### Fields

- `accept_stream`: `fun(self: Libp2pConnection, router?: table):Libp2pStream|nil, string|nil, function|nil, table|nil`
- `close`: `fun(self: Libp2pConnection):boolean|nil, table|nil`
- `new_stream`: `fun(self: Libp2pConnection, protocols?: string[]):Libp2pStream|nil, string|nil, table|nil`
- `raw`: `fun(self: Libp2pConnection):table`
- `session`: `fun(self: Libp2pConnection):table|nil`

### Libp2pStream

 Connection abstraction over secure muxed sessions.

```lua
Libp2pStream
```

 Connection abstraction over secure muxed sessions.

#### Fields

- `close`: `fun(self: Libp2pStream):boolean|nil, table|nil`
- `close_write`: `(fun(self: Libp2pStream):boolean|nil, table|nil)?`
- `read`: `fun(self: Libp2pStream, length: integer):string|nil, table|nil`
- `read_now`: `(fun(self: Libp2pStream):string|nil, table|nil)?`
- `reset_now`: `(fun(self: Libp2pStream):boolean|nil, table|nil)?`
- `write`: `fun(self: Libp2pStream, payload: string):boolean|nil, table|nil`

