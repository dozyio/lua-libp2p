# lua_libp2p.connection_encrypter

Security transport registry.
Keeps protocol IDs centralized while loading implementations only when used.

### Libp2pSecurityTransportConfig

 Security transport registry.
 Keeps protocol IDs centralized while loading implementations only when used.

```lua
Libp2pSecurityTransportConfig
```

 Security transport registry.
 Keeps protocol IDs centralized while loading implementations only when used.

#### Fields

- `noise`: `boolean?` - Enable Noise (`/noise`). Default: true.
- `plaintext`: `boolean?` - Enable plaintext (`/plaintext/2.0.0`). Intended for tests only.
- `tls`: `boolean?` - Enable experimental libp2p TLS (`/tls/1.0.0`). Requires `fd_tls` for luv-native TLS.

