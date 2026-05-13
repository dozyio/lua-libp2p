# lua_libp2p.operation

Operation wrapper for sync/async result handling.

### Libp2pOperation

```lua
Libp2pOperation
```

#### Fields

- `cancel`: `fun(self: Libp2pOperation):true|nil, table|nil`
- `id`: `fun(self: Libp2pOperation):integer|nil`
- `result`: `fun(self: Libp2pOperation, opts?: Libp2pOperationResultOptions):any`
- `status`: `fun(self: Libp2pOperation):string`
- `task`: `fun(self: Libp2pOperation):table|nil`

### Libp2pOperationOptions

```lua
Libp2pOperationOptions
```

#### Fields

- `result_count`: `integer?` - Number of result slots before trailing error.

### Libp2pOperationResultOptions

```lua
Libp2pOperationResultOptions
```

#### Fields

- `ctx`: `table?`
- `poll_interval`: `number?`
- `timeout`: `number?`

