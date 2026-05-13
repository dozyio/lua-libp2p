--- Multiformats entrypoint.
local M = {
  varint = require("lua_libp2p.multiformats.varint"),
  multihash = require("lua_libp2p.multiformats.multihash"),
  base58btc = require("lua_libp2p.multiformats.base58btc"),
  base32 = require("lua_libp2p.multiformats.base32"),
  cid = require("lua_libp2p.multiformats.cid"),
}

return M
