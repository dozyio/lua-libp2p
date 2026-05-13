# lua_libp2p.connection_encrypter_noise.protocol

Noise transport handshake and secure channel.

### Libp2pNoiseExtensions

```lua
Libp2pNoiseExtensions
```

#### Fields

- `stream_muxers`: `string[]?`
- `webtransport_certhashes`: `string[]?`

### Libp2pNoiseHandshakeOptions

```lua
Libp2pNoiseHandshakeOptions
```

#### Fields

- `expected_remote_peer_id`: `string?`
- `extensions`: `Libp2pNoiseExtensions?`
- `identity_keypair`: `Libp2pIdentityKeypair`
- `static_keypair`: `Libp2pNoiseStaticKeypair?` -  Noise transport handshake and secure channel.

### Libp2pNoiseStaticKeypair

```lua
Libp2pNoiseStaticKeypair
```

#### Fields

- `private_key`: `string`
- `public_key`: `string`

