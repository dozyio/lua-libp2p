# lua_libp2p.protocol_identify.protocol

Identify protocol codec and helpers.

### Libp2pIdentifyMessage

```lua
Libp2pIdentifyMessage
```

#### Fields

- `agentVersion`: `string?`
- `listenAddrs`: `string[]?` - Multiaddr bytes or text.
- `observedAddr`: `string?` - Multiaddr bytes or text.
- `protocolVersion`: `string?`
- `protocols`: `string[]?`
- `publicKey`: `string?`
- `signedPeerRecord`: `string?`

### Libp2pIdentifyVerifyOptions

```lua
Libp2pIdentifyVerifyOptions
```

#### Fields

- `expected_peer_id`: `string?`

