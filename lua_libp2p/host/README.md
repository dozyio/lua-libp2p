# lua_libp2p.host

Host construction and runtime orchestration.
Hosts manage listeners, connections, protocol handlers, services, and
cooperative background tasks.

### Libp2pHost

```lua
Libp2pHost
```

#### Fields

- `connection_manager`: `table|nil`
- `muxers`: `Libp2pMuxerConfig` -  Stream muxer registry.
- `security_transports`: `Libp2pSecurityTransportConfig` -  Security transport registry.
- `services`: `table<string, table>`

### Libp2pHostConfig

```lua
Libp2pHostConfig
```

#### Fields

- `accept_timeout`: `number?`
- `address_manager`: `Libp2pAddressManagerInstance?`
- `advertise_observed`: `boolean?`
- `announce_addrs`: `string[]?`
- `autonat`: `Libp2pAutoNatConfig?` -  AutoNAT v2 client/service.
- `autorelay`: `Libp2pAutoRelayConfig?` -  AutoRelay service.
- `blocking`: `boolean?`
- `connect_timeout`: `number?`
- `connection_manager`: `(Libp2pConnectionManagerConfig|Libp2pConnectionManagerInstance|false)?` - Options map, prebuilt manager object, or false.
- `debug_connection_events`: `boolean?`
- `identify`: `Libp2pIdentifyConfig?` -  Identify protocol service.
- `identity`: `Libp2pIdentityKeypair?` - Local identity keypair. Generated when omitted.
- `identity_type`: `('ecdsa'|'ed25519'|'rsa'|'secp256k1')?` - Key type used when generating identity. Default: `ed25519`.
- `io_timeout`: `number?`
- `kad_dht`: `Libp2pKadDhtConfig?` -  Kademlia DHT service and client operations.
- `listen_addrs`: `string[]?`
- `muxers`: `Libp2pMuxerConfig?` -  Stream muxer registry.
- `no_announce_addrs`: `string[]?`
- `on`: `table<string, fun(event: table):any>?`
- `on_started`: `fun(host: Libp2pHost)?`
- `peer_discovery`: `table<string, table|Libp2pServiceSpec>?`
- `peerstore`: `Libp2pPeerstoreInstance?`
- `peerstore_options`: `Libp2pPeerstoreConfig?` -  Peerstore.
- `perf`: `Libp2pPerfConfig?` -  Perf protocol service.
- `relay_addrs`: `string[]?`
- `resource_manager`: `(Libp2pResourceManagerConfig|Libp2pResourceManagerInstance|false)?`
- `resource_manager_options`: `Libp2pResourceManagerConfig?`
- `runtime`: `('auto'|'luv')?`
- `security_transports`: `Libp2pSecurityTransportConfig?` -  Security transport registry.
- `self_observed_addrs`: `string[]?`
- `services`: `table<string, table|Libp2pServiceSpec>?`
- `task_prune_interval`: `number?`
- `task_resume_budget`: `integer?`
- `task_retention`: `number?`
- `tcp`: `Libp2pTcpConfig?` -  Poll-based TCP transport implementation.
- `transports`: `'tcp'[]?`
- `upnp_nat`: `Libp2pUpnpNatConfig?` -  UPnP NAT mapping service.

### Libp2pHostEventHandler

```lua
fun(event: table):any
```

### Libp2pTransportName

```lua
'tcp'
```

```lua
Libp2pTransportName:
    | 'tcp'
```

