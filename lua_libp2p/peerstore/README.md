# lua_libp2p.peerstore

Peerstore.

### Libp2pPeerstoreConfig

 Peerstore.

```lua
Libp2pPeerstoreConfig
```

 Peerstore.

#### Fields

- `datastore`: `table?` - Custom datastore backend.
- `default_addr_ttl`: `number?` - Default address TTL in seconds.

### Libp2pPeerstoreInstance

```lua
Libp2pPeerstoreInstance
```

#### Fields

- `add_addrs`: `fun(self: Libp2pPeerstoreInstance, peer_id: string, addrs: string[], opts?: table):integer|nil, table|nil`
- `add_protocols`: `fun(self: Libp2pPeerstoreInstance, peer_id: string, protocols: string[]):integer|nil, table|nil`
- `get_addrs`: `fun(self: Libp2pPeerstoreInstance, peer_id: string):string[]`
- `get_protocols`: `fun(self: Libp2pPeerstoreInstance, peer_id: string):string[]?`

