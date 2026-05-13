--- UPnP module entrypoint.
--
-- The UPnP NAT service discovers IGD gateways via SSDP and creates TCP/UDP port
-- mappings for eligible private, non-loopback transport listen addresses. Mapped
-- public addresses are added to the address manager as public mapping addresses.
-- By default they are unverified and should be confirmed by AutoNAT before being
-- advertised; `auto_confirm_address = true` is useful for local testing.
return {
  igd = require("lua_libp2p.port_mapping.upnp.igd"),
  nat = require("lua_libp2p.port_mapping.upnp.nat"),
  ssdp = require("lua_libp2p.port_mapping.upnp.ssdp"),
}
