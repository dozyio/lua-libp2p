# lua_libp2p.host.protocols

Host protocol handler registry and limited-connection policy.

### Libp2pProtocolHandler

```lua
fun(stream: table, ctx: table):any
```

### Libp2pProtocolHandlerOptions

 Host protocol handler registry and limited-connection policy.

```lua
Libp2pProtocolHandlerOptions
```

 Host protocol handler registry and limited-connection policy.

#### Fields

- `run_on_limited_connection`: `boolean?` - Allow handler execution on limited relay connections.

