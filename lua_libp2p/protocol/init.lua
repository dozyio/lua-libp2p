local M = {
  name = "protocol",
  autonat_v2 = require("lua_libp2p.protocol.autonat_v2"),
  identify = require("lua_libp2p.protocol_identify.protocol"),
  mss = require("lua_libp2p.protocol.mss"),
  perf = require("lua_libp2p.protocol_perf.protocol"),
  ping = require("lua_libp2p.protocol_ping.protocol"),
  plaintext = require("lua_libp2p.protocol.plaintext"),
}

return M
