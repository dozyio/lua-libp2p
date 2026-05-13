# lua_libp2p.relay_discovery

Relay candidate discovery service.
Coordinates AutoRelay candidate replenishment using host peer discovery and
KAD random walks without embedding that policy in the host runtime.

### Libp2pRelayDiscoveryConfig

```lua
Libp2pRelayDiscoveryConfig
```

#### Fields

- `auto_start`: `boolean?`
- `discover_timeout`: `number?`
- `interval`: `number?`
- `max_candidates`: `integer?`
- `min_candidates`: `integer?`
- `random_walk`: `boolean?`

