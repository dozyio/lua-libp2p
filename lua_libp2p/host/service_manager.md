# lua_libp2p.host.service_manager

Host service graph construction and dependency ordering.

### Libp2pServiceSpec

```lua
Libp2pServiceSpec
```

#### Fields

- `config`: `table?` - Service-specific configuration passed to `module.new`.
- `module`: `table` - Service module with `new(host, config, name)`.
- `provides`: `string[]?` - Capabilities provided by this service.
- `requires`: `string[]?` - Capabilities required before this service starts.

### Libp2pServicesConfig

```lua
table<string, table|Libp2pServiceSpec>
```

