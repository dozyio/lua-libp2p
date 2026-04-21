local M = {
  _VERSION = "0.0.0",
  discovery = require("lua_libp2p.discovery"),
  kad_dht = require("lua_libp2p.kad_dht"),
  kbucket = require("lua_libp2p.kbucket"),
  host = require("lua_libp2p.host"),
  network = require("lua_libp2p.network"),
}

return M
