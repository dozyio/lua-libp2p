--- Record module entrypoint.
-- @module lua_libp2p.record
local M = {
  peer_record = require("lua_libp2p.record.peer_record"),
  signed_envelope = require("lua_libp2p.record.signed_envelope"),
}

return M
