# lua_libp2p.os_routing

Cross-platform route and neighbor discovery facade.

### Libp2pOsRoute

```lua
Libp2pOsRoute
```

#### Fields

- `gateway`: `string?`
- `interface`: `string?`

### Libp2pOsRoutingOptions

 Cross-platform route and neighbor discovery facade.

```lua
Libp2pOsRoutingOptions
```

 Cross-platform route and neighbor discovery facade.

#### Fields

- `platform`: `('linux'|'macos'|'windows')?`

### Libp2pOsRoutingSnapshot

```lua
Libp2pOsRoutingSnapshot
```

#### Fields

- `default_route_v4`: `Libp2pOsRoute?`
- `default_route_v6`: `Libp2pOsRoute?`
- `neighbors_v6`: `table[]?`
- `platform`: `string`
- `router_candidates_v6`: `table[]?`

