# lua_libp2p.datastore.memory

In-memory datastore implementation.

### Libp2pMemoryDatastore

 In-memory datastore implementation.

```lua
Libp2pMemoryDatastore
```

 In-memory datastore implementation.

#### Fields

- `close`: `(fun(self: Libp2pDatastore):true|nil, table|nil)?`
- `delete`: `fun(self: Libp2pDatastore, key: string):boolean|nil, table|nil`
- `get`: `fun(self: Libp2pDatastore, key: string):any, table|nil`
- `list`: `fun(self: Libp2pDatastore, prefix: string):string[]|nil, table|nil`
- `put`: `fun(self: Libp2pDatastore, key: string, value: any, opts?: Libp2pDatastorePutOptions):true|nil, table|nil`

