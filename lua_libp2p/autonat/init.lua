--- AutoNAT module entrypoint.
-- @module lua_libp2p.autonat
local client = require("lua_libp2p.autonat.client")

return {
  client = client,
  new = client.new,
}
