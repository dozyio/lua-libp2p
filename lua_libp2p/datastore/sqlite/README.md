# lua_libp2p.datastore.sqlite

SQLite datastore implementation backed by LuaSQL (`luasql.sqlite3`).

### Libp2pSqliteDatastore

```lua
Libp2pSqliteDatastore
```

#### Fields

- `close`: `(fun(self: Libp2pDatastore):true|nil, table|nil)?`
- `delete`: `fun(self: Libp2pDatastore, key: string):boolean|nil, table|nil`
- `get`: `fun(self: Libp2pDatastore, key: string):any, table|nil`
- `list`: `fun(self: Libp2pDatastore, prefix: string):string[]|nil, table|nil`
- `put`: `fun(self: Libp2pDatastore, key: string, value: any, opts?: Libp2pDatastorePutOptions):true|nil, table|nil`
- `query`: `fun(self: Libp2pDatastore, query: Libp2pDatastoreQuery):Libp2pDatastoreResults|nil, table|nil`

### Libp2pSqliteDatastoreConfig

```lua
Libp2pSqliteDatastoreConfig
```

#### Fields

- `filename`: `string?` - Alias for `path`.
- `path`: `string?` - Database file path.
- `table_name`: `string?` - KV table name. Default: `kv`.

