# lua_libp2p.multiformats.multiaddr

Multiaddr parsing, formatting, and helpers.

### Libp2pMultiaddr

```lua
Libp2pMultiaddr
```

#### Fields

- `components`: `Libp2pMultiaddrComponent[]`
- `text`: `string`

### Libp2pMultiaddrComponent

 Multiaddr parsing, formatting, and helpers.

```lua
Libp2pMultiaddrComponent
```

 Multiaddr parsing, formatting, and helpers.

#### Fields

- `protocol`: `string`
- `value`: `string?`

### Libp2pRelayInfo

```lua
Libp2pRelayInfo
```

#### Fields

- `circuit_addr`: `string`
- `circuit_index`: `integer`
- `destination_peer_id`: `string?`
- `relay_addr`: `string`
- `relay_peer_id`: `string`

### Libp2pTcpEndpoint

```lua
Libp2pTcpEndpoint
```

#### Fields

- `host`: `string`
- `host_protocol`: `string`
- `port`: `integer`

