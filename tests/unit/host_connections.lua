local ed25519 = require("lua_libp2p.crypto.ed25519")
local host_connections = require("lua_libp2p.host.connections")
local host_mod = require("lua_libp2p.host")

local function run()
  local h = assert(host_mod.new({
    identity = assert(ed25519.generate_keypair()),
    runtime = "luv",
    listen_addrs = {},
  }))

  local c1 = { close = function() return true end }
  local c2 = { close = function() return true end }
  local limited = { close = function() return true end }
  local e1 = host_connections.add(h, c1, { remote_peer_id = "peer-a" })
  local e2 = host_connections.add(h, c2, { remote_peer_id = "peer-a" })
  local e3 = host_connections.add(h, limited, {
    remote_peer_id = "peer-limited",
    relay = { limit_kind = "limited" },
  })

  if e1.id ~= 1 or e2.id ~= 2 or h:_find_connection_by_id(e2.id) ~= e2 then
    return nil, "connection ids and id index should be assigned"
  end
  if h:_find_connection("peer-a") == nil then
    return nil, "peer index should locate connections"
  end
  if h:_find_connection("peer-limited") ~= e3 then
    return nil, "limited connection should be returned when unlimited is not required"
  end
  if h:_find_connection("peer-limited", { require_unlimited_connection = true }) ~= nil then
    return nil, "limited connection should be hidden when unlimited is required"
  end

  local snapshot = host_connections.snapshot(h)
  if #snapshot ~= 3 then
    return nil, "snapshot should copy active connections"
  end
  if not host_connections.remove(h, e1) then
    return nil, "remove should succeed"
  end
  if h:_find_connection_by_id(e1.id) ~= nil then
    return nil, "remove should clear id index"
  end
  if h._connections_by_peer["peer-a"] == nil then
    return nil, "peer map should remain while another peer connection exists"
  end
  host_connections.remove(h, e2)
  if h._connections_by_peer["peer-a"] ~= nil then
    return nil, "peer map should be removed after last peer connection"
  end
  host_connections.reset(h)
  if #h._connections ~= 0 or next(h._connections_by_id) ~= nil or next(h._connections_by_peer) ~= nil then
    return nil, "reset should clear all connection indexes"
  end

  return true
end

return {
  name = "host connection indexes",
  run = run,
}
