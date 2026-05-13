# lua_libp2p.port_mapping.nat_pmp.service

NAT-PMP mapping service.

NAT-PMP creates public TCP/UDP mappings for eligible private transport listen
addresses and adds the mapped external address to the host address manager.
The service emits `nat_pmp:mapping:active` and `nat_pmp:mapping:failed`
events.

### Libp2pNatPmpServiceConfig

```lua
Libp2pNatPmpServiceConfig
```

#### Fields

- `client`: `Libp2pNatPmpClient?`
- `enabled`: `boolean?`
- `external_port`: `integer?`
- `gateway`: `string?`
- `internal_port`: `integer?`
- `protocol`: `('tcp'|'udp')?`
- `ttl`: `number?`

