# lua_libp2p.datastore

Minimal synchronous datastore/KV interface helpers.
Datastores expose `get`, `put`, `delete`, and `list` methods.
Backends used with the current peerstore must be local/fast enough to call
synchronously from host/service code; remote or blocking IO backends need a
future async adapter instead of implementing this interface directly.

### Libp2pDatastore

```lua
Libp2pDatastore
```

#### Fields

- `close`: `(fun(self: Libp2pDatastore):true|nil, table|nil)?`
- `delete`: `fun(self: Libp2pDatastore, key: string):boolean|nil, table|nil`
- `get`: `fun(self: Libp2pDatastore, key: string):any, table|nil`
- `list`: `fun(self: Libp2pDatastore, prefix: string):string[]|nil, table|nil`
- `put`: `fun(self: Libp2pDatastore, key: string, value: any, opts?: Libp2pDatastorePutOptions):true|nil, table|nil`

### Libp2pDatastorePutOptions

 Minimal synchronous datastore/KV interface helpers.
 Datastores expose `get`, `put`, `delete`, and `list` methods.
 Backends used with the current peerstore must be local/fast enough to call
 synchronously from host/service code; remote or blocking IO backends need a
 future async adapter instead of implementing this interface directly.

```lua
Libp2pDatastorePutOptions
```

 Minimal synchronous datastore/KV interface helpers.
 Datastores expose `get`, `put`, `delete`, and `list` methods.
 Backends used with the current peerstore must be local/fast enough to call
 synchronously from host/service code; remote or blocking IO backends need a
 future async adapter instead of implementing this interface directly.

#### Fields

- `ttl`: `(number|false)?` - Time-to-live seconds, `false`, or `math.huge`.

