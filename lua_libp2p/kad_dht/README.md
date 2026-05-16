# lua_libp2p.kad_dht

Kademlia DHT service and client operations.

The default profile targets the public libp2p DHT: `k = 20`,
`alpha = 10`, `disjoint_paths = 10`, and `max_concurrent_queries = 32`.
DHT messages use `lua_libp2p.network.MESSAGE_SIZE_MAX` by default, matching
the practical 4 MiB KAD RPC cap used by Go and JS implementations.

Address filtering is separate from transport dialability. The built-in modes
are `public`, `private`, and `all`; callers may also pass a function for
custom policy. Provider records default to a 48 hour TTL, provider addresses
learned from provider records default to 24 hours, and background reprovide
runs every 22 hours when enabled.

The DHT starts in client mode by default. Set `mode = "server"` or enable
server-mode behavior explicitly when the node should serve and advertise KAD.

### Libp2pKadDhtConfig

```lua
Libp2pKadDhtConfig
```

#### Fields

- `address_filter`: `(string|function|table)?` - Address filter or filter mode.
- `address_filter_mode`: `string?` - Alias for `address_filter`.
- `alpha`: `integer?` - Query parallelism. Default: module default.
- `auto_server_mode`: `boolean?` - Enable automatic server-mode advertisement.
- `bootstrappers`: `string[]?` - Bootstrap peer multiaddrs.
- `bucket_size`: `integer?` - Alias for `k` when creating routing table.
- `datastore`: `table?` - Shared datastore for provider/record stores.
- `disjoint_paths`: `integer?` - Number of disjoint query paths.
- `dnsaddr_resolver`: `function?`
- `filtered_addr_cache_size`: `integer?`
- `filtered_addr_cache_ttl_seconds`: `number?`
- `hash_function`: `function?` - Routing table hash function.
- `k`: `integer?` - Bucket size / result count. Default: module default.
- `local_peer_id`: `string?` - Override local peer ID; normally derived from host.
- `maintenance_enabled`: `boolean?` - Default: true.
- `maintenance_interval_seconds`: `number?`
- `maintenance_max_checks`: `integer?`
- `maintenance_min_recheck_seconds`: `number?`
- `maintenance_protocol_check_timeout`: `number?`
- `maintenance_startup_retry_max_seconds`: `number?`
- `maintenance_startup_retry_seconds`: `number?`
- `maintenance_walk_every`: `integer?`
- `maintenance_walk_timeout`: `number?`
- `max_concurrent_queries`: `integer?`
- `max_failed_checks_before_evict`: `integer?`
- `max_message_size`: `integer?`
- `mode`: `('auto'|'client'|'server')?` - DHT mode. Default: `client`.
- `now`: `(fun():number)?`
- `peer_discovery`: `table?` - Peer discovery source.
- `peer_diversity_filter`: `function?`
- `peer_diversity_max_peers_per_ip_group`: `(integer|false)?`
- `peer_diversity_max_peers_per_ip_group_per_bucket`: `(integer|false)?`
- `peer_id_bytes_cache_size`: `integer?`
- `populate_from_peerstore_limit`: `integer?` - Default: 1000.
- `populate_from_peerstore_on_start`: `boolean?` - Default: true.
- `populate_from_peerstore_protocol_check_timeout`: `number?`
- `protocol_id`: `string?` - DHT protocol ID override.
- `provider_addr_ttl_seconds`: `number?` - Provider address TTL.
- `provider_datastore`: `table?` - Provider datastore override.
- `provider_store`: `table?` - Prebuilt provider store.
- `provider_ttl_seconds`: `number?` - Provider record TTL.
- `query_filter`: `function?`
- `record_datastore`: `table?` - Record datastore override.
- `record_selector`: `function?`
- `record_selectors`: `table?`
- `record_store`: `table?` - Prebuilt record store.
- `record_ttl_seconds`: `number?` - Value record TTL.
- `record_validator`: `function?`
- `record_validators`: `table?`
- `reprovider_batch_size`: `integer?`
- `reprovider_enabled`: `boolean?`
- `reprovider_initial_delay_seconds`: `number?`
- `reprovider_interval_seconds`: `number?`
- `reprovider_jitter_seconds`: `number?`
- `reprovider_max_parallel`: `integer?`
- `reprovider_random`: `function?`
- `reprovider_timeout`: `number?`
- `routing_table`: `table?` - Prebuilt routing table.
- `routing_table_filter`: `function?`

