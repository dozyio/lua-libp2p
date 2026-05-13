# lua_libp2p.transport_circuit_relay_v2.protocol

Circuit Relay v2 protocol codec and helpers.

### Libp2pRelayLimit

```lua
Libp2pRelayLimit
```

#### Fields

- `data`: `integer?`
- `duration`: `integer?`

### Libp2pRelayMessage

```lua
Libp2pRelayMessage
```

#### Fields

- `limit`: `Libp2pRelayLimit?`
- `peer`: `Libp2pRelayPeer?` -  Circuit Relay v2 protocol codec and helpers.
- `reservation`: `Libp2pRelayReservation?`
- `status`: `integer?`
- `type`: `integer`

### Libp2pRelayPeer

 Circuit Relay v2 protocol codec and helpers.

```lua
Libp2pRelayPeer
```

 Circuit Relay v2 protocol codec and helpers.

#### Fields

- `addrs`: `string[]?`
- `id`: `string?`
- `peer_id`: `string?`

### Libp2pRelayReservation

```lua
Libp2pRelayReservation
```

#### Fields

- `addrs`: `string[]?`
- `expire`: `integer?`
- `voucher`: `string?`

