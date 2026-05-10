--- AutoNAT module entrypoint.
-- @module lua_libp2p.autonat
local client = require("lua_libp2p.autonat.client")
local server = require("lua_libp2p.autonat.server")

return {
  client = client,
  server = server,
  new = client.new,
}
