local M = {
  name = "protocol",
  identify = require("lua_libp2p.protocol.identify"),
  mss = require("lua_libp2p.protocol.mss"),
  plaintext = require("lua_libp2p.protocol.plaintext"),
}

return M
