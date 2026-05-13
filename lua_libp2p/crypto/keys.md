# lua_libp2p.crypto.keys

Key loading and signing helpers.

### Libp2pGenerateKeypairOptions

```lua
Libp2pGenerateKeypairOptions
```

#### Fields

- `bits`: `integer?` - RSA key size. Default: 2048.
- `curve`: `string?` - ECDSA curve. Default: `prime256v1`.

### Libp2pIdentityKeypair

```lua
Libp2pIdentityKeypair
```

#### Fields

- `private_key`: `string?`
- `private_key_pem`: `string?`
- `public_key`: `string`
- `public_key_proto`: `string?`
- `type`: `(integer|'ecdsa'|'ed25519'|'rsa'|'secp256k1')?` -  Key loading and signing helpers.

### Libp2pIdentityType

```lua
'ecdsa'|'ed25519'|'rsa'|'secp256k1'
```

```lua
--  Key loading and signing helpers.
Libp2pIdentityType:
    | 'ed25519'
    | 'rsa'
    | 'ecdsa'
    | 'secp256k1'
```

