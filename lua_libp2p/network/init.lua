--- Network module entrypoint.
local M = {
  MESSAGE_SIZE_MAX = 4 * 1024 * 1024,
  connection = require("lua_libp2p.network.connection"),
  upgrader = require("lua_libp2p.network.upgrader"),
}

return M
