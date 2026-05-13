# lua_libp2p.port_mapping.pcp.service

PCP mapping service.

PCP creates public TCP/UDP mappings for eligible private transport listen
addresses and adds the mapped external address to the host address manager.
The service emits `pcp:mapping:active` and `pcp:mapping:failed` events.

### Libp2pPcpServiceConfig

```lua
Libp2pPcpServiceConfig
```

#### Fields

- `client`: `Libp2pPcpClient?`
- `enabled`: `boolean?`
- `external_port`: `integer?`
- `gateway`: `string?`
- `internal_client`: `string?`
- `internal_port`: `integer?`
- `protocol`: `('tcp'|'udp')?`
- `ttl`: `number?`

