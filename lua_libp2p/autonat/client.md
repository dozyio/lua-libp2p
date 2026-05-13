# lua_libp2p.autonat.client

AutoNAT v2 client/service.

### Libp2pAutoNatConfig

 AutoNAT v2 client/service.

```lua
Libp2pAutoNatConfig
```

 AutoNAT v2 client/service.

#### Fields

- `allow_dial_data`: `boolean?` - Allow AutoNAT v2 dial-data requests. Default: true.
- `dial_data_chunk_size`: `integer?` - Dial-data chunk size.
- `max_dial_data_bytes`: `integer?` - Maximum dial-data bytes accepted/sent.
- `max_message_size`: `integer?` - Maximum AutoNAT message size.
- `monitor_on_start`: `boolean?` - Start monitor when host starts. Default: false.
- `monitor_opts`: `table?` - Alias for `monitor_start_opts`.
- `monitor_start_opts`: `table?` - Monitor options used when `monitor_on_start=true`.

