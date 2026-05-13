# lua_libp2p.host.tasks

Host cooperative task scheduler internals.

### Libp2pRunUntilTaskOptions

```lua
Libp2pRunUntilTaskOptions
```

#### Fields

- `poll_interval`: `number?`
- `timeout`: `number?`

### Libp2pTaskFunction

```lua
fun(ctx: table):any
```

### Libp2pTaskOptions

```lua
Libp2pTaskOptions
```

#### Fields

- `priority`: `'front'?`
- `service`: `string?`

