# lua_libp2p.autonat.server

AutoNAT server service supporting v1 and v2.

The server can handle `/libp2p/autonat/1.0.0` and
`/libp2p/autonat/2/dial-request`. Public relay or operator nodes should use
conservative rate limits, low per-peer concurrency, and v2 dial-data limits to
reduce amplification risk. See `examples/autonat_server_hardened.lua` for a
runnable hardened profile.

No public LuaLS symbols are documented for this module yet.

