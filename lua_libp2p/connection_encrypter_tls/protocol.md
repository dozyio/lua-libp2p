# lua_libp2p.connection_encrypter_tls.protocol

TLS transport handshake and secure channel.

### Libp2pTlsHandshakeOptions

```lua
Libp2pTlsHandshakeOptions
```

#### Fields

- `ctx`: `table?`
- `expected_remote_peer_id`: `string?`
- `identity_keypair`: `Libp2pIdentityKeypair`
- `muxer_protocols`: `string[]?`

### Libp2pTlsHandshakeState

```lua
Libp2pTlsHandshakeState
```

#### Fields

- `remote_peer_id`: `string`
- `remote_public_key`: `string?`
- `selected_muxer`: `string?`
- `used_early_muxer_negotiation`: `boolean?`

