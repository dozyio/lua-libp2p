local M = {
  name = "crypto",
  ed25519 = require("lua_libp2p.crypto.ed25519"),
  key_pb = require("lua_libp2p.crypto.key_pb"),
}

return M
