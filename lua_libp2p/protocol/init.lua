--- Protocol module entrypoint.
local M = {
  name = "protocol",
  autonat_v1 = require("lua_libp2p.protocol.autonat_v1"),
  autonat_v2 = require("lua_libp2p.protocol.autonat_v2"),
  identify = require("lua_libp2p.protocol_identify.protocol"),
  mss = require("lua_libp2p.multistream_select.protocol"),
  perf = require("lua_libp2p.protocol_perf.protocol"),
  ping = require("lua_libp2p.protocol_ping.protocol"),
  plaintext = require("lua_libp2p.connection_encrypter_plaintext.protocol"),
}

return M
