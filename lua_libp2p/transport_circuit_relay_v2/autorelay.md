# lua_libp2p.transport_circuit_relay_v2.autorelay

AutoRelay service.
Maintains relay reservations and candidate discovery.

`/p2p-circuit` listen addresses require this service; they describe relay
listen capability rather than a TCP listener. AutoRelay can reserve explicitly
configured relays and can discover candidates from peers advertising
`/libp2p/circuit/relay/0.2.0/hop`.

Active reservations publish relayed `/p2p-circuit` addresses through the
address manager. Removed or expired reservations remove those addresses.
Reservation lifecycle is emitted through `relay:reservation:active`,
`relay:reservation:removed`, and `relay:reservation:failed` events.

### Libp2pAutoRelayConfig

```lua
Libp2pAutoRelayConfig
```

#### Fields

- `backoff_seconds`: `number?` - Failed relay backoff. Default: 60.
- `discover`: `(boolean|table)?` - Enable/use relay discovery integration.
- `fail_fast`: `boolean?` - Fail host start when reservations fail. Default: false.
- `keepalive_interval`: `number?` - Keepalive interval; nil uses module default.
- `keepalive_timeout`: `number?` - Keepalive ping timeout. Default: 5.
- `max_queue_length`: `integer?` - Maximum queued reservation targets. Default: 32.
- `max_reservations`: `integer?` - Maximum active relay reservations. Default: 2.
- `min_reservation_ttl`: `number?` - Minimum acceptable reservation TTL. Default: 10.
- `refresh_margin`: `number?` - Reservation refresh margin. Default: 60.
- `refresh_timeout`: `number?` - Refresh timeout. Default: 300.
- `refresh_timeout_min`: `number?` - Minimum refresh timeout. Default: 30.
- `relays`: `table[]?` - Static bootstrap relay target list.
- `reservation_concurrency`: `integer?` - Concurrent reservation attempts. Default: 1.
- `reserve_opts`: `table?` - Options passed to relay reservation calls.
- `tick_interval`: `number?` - Maintenance tick interval. Default: 1.

