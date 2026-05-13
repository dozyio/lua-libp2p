# lua_libp2p.peerid

Peer ID parsing and derivation helpers.

### Libp2pPeerId

 Peer ID parsing and derivation helpers.

```lua
Libp2pPeerId
```

 Peer ID parsing and derivation helpers.

#### Fields

- `bytes`: `string` - Raw multihash bytes.
- `cid`: `string` - CIDv1 libp2p-key form.
- `id`: `string` - Base58btc peer ID.
- `public_key`: `string?` - Raw public key when inlined.
- `public_key_proto`: `string?` - Protobuf-encoded public key when available.
- `type`: `(string|integer|'ecdsa'|'ed25519'|'rsa'...(+1))?` -  Key loading and signing helpers.

