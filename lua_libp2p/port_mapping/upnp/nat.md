# lua_libp2p.port_mapping.upnp.nat

UPnP NAT mapping service.

### Libp2pUpnpNatConfig

```lua
Libp2pUpnpNatConfig
```

#### Fields

- `auto_confirm_address`: `boolean?` - Advertise confirmed external address. Default: false.
- `client`: `table?` - Prebuilt IGD client.
- `debug_raw`: `boolean?` - Enable raw HTTP/SSDP diagnostics.
- `debug_soap`: `boolean?` - Enable SOAP diagnostics.
- `description`: `string?` - Port mapping description.
- `discover_client`: `table?` - Prebuilt discovery client.
- `external_port`: `integer?` - External port override.
- `fail_on_start_error`: `boolean?` - Fail service start on mapping error. Default: false.
- `internal_client`: `string?` - Internal client IP override.
- `replace_existing`: `boolean?` - Remove stale mappings before adding. Default: false.
- `ttl`: `number?` - Mapping TTL seconds. Default: 720.
- `wanppp_only`: `boolean?` - Prefer/restrict to WANPPP service.

