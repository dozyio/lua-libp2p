# lua_libp2p.address_manager

Address manager and advertisement policy.

### Libp2pAddressManagerConfig

 Address manager and advertisement policy.

```lua
Libp2pAddressManagerConfig
```

 Address manager and advertisement policy.

#### Fields

- `advertise_observed`: `boolean?`
- `announce_addrs`: `string[]?`
- `listen_addrs`: `string[]?`
- `no_announce_addrs`: `string[]?`
- `observed_addrs`: `string[]?`
- `public_mapping_addrs`: `string[]?`
- `relay_addrs`: `string[]?`

### Libp2pAddressManagerInstance

```lua
Libp2pAddressManagerInstance
```

#### Fields

- `get_advertised_addrs`: `fun(self: Libp2pAddressManagerInstance):string[]?`
- `get_listen_addrs`: `fun(self: Libp2pAddressManagerInstance):string[]`
- `set_listen_addrs`: `(fun(self: Libp2pAddressManagerInstance, addrs: string[]):boolean|nil, table|nil)?`

