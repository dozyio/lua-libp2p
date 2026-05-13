# lua_libp2p.log

Lightweight structured logging helpers.

### Libp2pLogLevel

```lua
'debug'|'error'|'info'|'warn'
```

```lua
--  Lightweight structured logging helpers.
Libp2pLogLevel:
    | 'debug'
    | 'info'
    | 'warn'
    | 'error'
```

### Libp2pLogSnapshot

```lua
Libp2pLogSnapshot
```

#### Fields

- `current_level`: `integer`
- `default_subsystem_level`: `integer?`
- `subsystem_levels`: `table<string, integer>?`

### Libp2pLogger

```lua
Libp2pLogger
```

#### Fields

- `debug`: `fun(message: string, fields?: table):true|nil, string|nil`
- `enabled`: `fun(level_name: 'debug'|'error'|'info'|'warn', fields?: table):boolean|nil, string|nil`
- `error`: `fun(message: string, fields?: table):true|nil, string|nil`
- `info`: `fun(message: string, fields?: table):true|nil, string|nil`
- `log`: `fun(level_name: 'debug'|'error'|'info'|'warn', message: string, fields?: table):true|nil, string|nil`
- `warn`: `fun(message: string, fields?: table):true|nil, string|nil`

