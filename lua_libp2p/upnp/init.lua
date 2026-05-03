--- UPnP module entrypoint.
-- @module lua_libp2p.upnp
return {
  igd = require("lua_libp2p.upnp.igd"),
  nat = require("lua_libp2p.upnp.nat"),
  ssdp = require("lua_libp2p.upnp.ssdp"),
}
