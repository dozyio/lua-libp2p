# lua_libp2p.peer_discovery_mdns

Multicast DNS peer discovery service.

### Libp2pMdnsConfig

```lua
Libp2pMdnsConfig
```

#### Fields

- `allow_public_addrs`: `boolean?` - Whether to advertise public IP addresses. Default `false`.
- `bind_addr`: `string?` - IPv4 bind address. Default `0.0.0.0`.
- `bind_addr6`: `string?` - IPv6 bind address. Default `::`.
- `broadcast`: `boolean?` - Whether to answer mDNS queries. Default `true`.
- `interval`: `number?` - Query interval in seconds. Default `10`; `0` disables periodic queries.
- `ipv4`: `boolean?` - Whether to open IPv4 mDNS socket. Default `true`.
- `ipv6`: `boolean?` - Whether to open IPv6 mDNS socket. Default `true`.
- `multicast_addr`: `string?` - IPv4 multicast group. Default `224.0.0.251`.
- `multicast_addr6`: `string?` - IPv6 multicast group. Default `ff02::fb`.
- `peer_name`: `string?` - DNS-SD instance label. Random 32-byte label when omitted.
- `peer_name_length`: `integer?` - Random peer name length when `peer_name` is omitted.
- `peer_ttl`: `number?` - Discovered peer TTL in seconds. Default `120`.
- `port`: `integer?` - UDP port. Default `5353`.
- `query_on_start`: `boolean?` - Whether to send an immediate query when started. Default `true`.
- `require_membership`: `boolean?` - Whether multicast membership failure is fatal for a socket. Default `true`.
- `service_name`: `string?` - mDNS service name. Default `_p2p._udp.local`.
- `socket_factory`: `(fun(service: Libp2pMdnsService):table)?` - Test hook returning socket-like object.

### Libp2pMdnsService

```lua
Libp2pMdnsService
```

#### Fields

- `allow_public_addrs`: `boolean`
- `bind_addr`: `string`
- `bind_addr6`: `string`
- `broadcast`: `boolean`
- `host`: `table|nil`
- `interval`: `number`
- `ipv4`: `boolean`
- `ipv6`: `boolean`
- `multicast_addr`: `string`
- `multicast_addr6`: `string`
- `peer_name`: `string`
- `peer_ttl`: `number`
- `port`: `integer`
- `query_on_start`: `boolean`
- `require_membership`: `boolean`
- `service_name`: `string`
- `started`: `boolean`

