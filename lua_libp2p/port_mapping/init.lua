--- Port mapping services and protocol clients.
--
-- This namespace groups NAT traversal helpers that create public TCP/UDP port
-- mappings for private transport listen addresses. Supported backends are
-- NAT-PMP, PCP, and UPnP IGD.
local M = {
  nat_pmp = require("lua_libp2p.port_mapping.nat_pmp.service"),
  pcp = require("lua_libp2p.port_mapping.pcp.service"),
  upnp = require("lua_libp2p.port_mapping.upnp"),
}

return M
