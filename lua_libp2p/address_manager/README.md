# lua_libp2p.address_manager

Address manager and advertisement policy.

The address manager combines listen addresses, explicit announce addresses,
no-announce filters, observed addresses, public NAT mappings, and relay
reservation addresses into the host's advertised address set.

`host:get_multiaddrs_raw()` returns selected advertised addresses without a
`/p2p/<peer>` suffix. `host:get_multiaddrs()` appends the local peer ID where
needed. Observed addresses learned through identify are collected but are not
advertised unless `advertise_observed` is enabled. Relayed `/p2p-circuit`
addresses are advertised only while an AutoRelay reservation is active.

### Libp2pAddressManagerConfig

```lua
Libp2pAddressManagerConfig
```

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

