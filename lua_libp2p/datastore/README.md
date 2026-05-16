# lua_libp2p.datastore

Minimal synchronous datastore/KV interface helpers.
Datastores expose `get`, `put`, `delete`, `list`, and `query` methods.
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
- `query`: `fun(self: Libp2pDatastore, query: Libp2pDatastoreQuery):Libp2pDatastoreResults|nil, table|nil`

### Libp2pDatastoreEntry

```lua
Libp2pDatastoreEntry
```

#### Fields

- `key`: `string`
- `value`: `any`

### Libp2pDatastorePutOptions

```lua
Libp2pDatastorePutOptions
```

#### Fields

- `ttl`: `(number|false)?` - Time-to-live seconds, `false`, or `math.huge`.

### Libp2pDatastoreQuery

```lua
Libp2pDatastoreQuery
```

#### Fields

- `keys_only`: `boolean?`
- `limit`: `integer?`
- `offset`: `integer?`
- `prefix`: `string?`

### Libp2pDatastoreResults

```lua
Libp2pDatastoreResults
```

#### Fields

- `close`: `fun(self: Libp2pDatastoreResults):true|nil, table|nil`
- `next`: `fun(self: Libp2pDatastoreResults):Libp2pDatastoreEntry|nil, table|nil`
- `rest`: `fun(self: Libp2pDatastoreResults):Libp2pDatastoreEntry[]|nil, table|nil`

