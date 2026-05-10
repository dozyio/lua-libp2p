local ed25519 = require("lua_libp2p.crypto.ed25519")
local error_mod = require("lua_libp2p.error")
local host_connections = require("lua_libp2p.host.connections")
local host_mod = require("lua_libp2p.host")

local function run()
  local h = assert(host_mod.new({
    identity = assert(ed25519.generate_keypair()),
    runtime = "luv",
    listen_addrs = {},
  }))

  local c1 = {
    close = function()
      return true
    end,
  }
  local c2 = {
    close = function()
      return true
    end,
  }
  local limited = {
    close = function()
      return true
    end,
  }
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

  local stale = {
    _closed = true,
    close = function()
      return true
    end,
  }
  local stale_entry = host_connections.add(h, stale, { remote_peer_id = "peer-a" })
  local fresh = {
    close = function()
      return true
    end,
  }
  local fresh_entry = host_connections.add(h, fresh, { remote_peer_id = "peer-a" })
  local selected = h:_find_connection("peer-a")
  if selected ~= fresh_entry then
    return nil, "find_connection should prefer the newest usable connection"
  end
  if h:_find_connection_by_id(stale_entry.id) ~= nil then
    return nil, "find_connection should reap stale closed connections from indexes"
  end

  if h:_find_connection("peer-limited") ~= e3 then
    return nil, "limited connection should be returned when unlimited is not required"
  end
  if h:_find_connection("peer-limited", { require_unlimited_connection = true }) ~= nil then
    return nil, "limited connection should be hidden when unlimited is required"
  end

  local snapshot = host_connections.snapshot(h)
  if #snapshot ~= 4 then
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
  host_connections.remove(h, fresh_entry)
  if h._connections_by_peer["peer-a"] ~= nil then
    return nil, "peer map should be removed after last peer connection"
  end
  host_connections.reset(h)
  if #h._connections ~= 0 or next(h._connections_by_id) ~= nil or next(h._connections_by_peer) ~= nil then
    return nil, "reset should clear all connection indexes"
  end

  local retry_host = assert(host_mod.new({
    identity = assert(ed25519.generate_keypair()),
    runtime = "luv",
    listen_addrs = {},
  }))
  local stale_state = { remote_peer_id = "peer-retry" }
  local fresh_state = { remote_peer_id = "peer-retry" }
  local stale_conn = {
    close = function()
      return true
    end,
    new_stream = function()
      return nil, nil, error_mod.new("closed", "write on closed connection")
    end,
  }
  local stream_opened = false
  local fresh_conn = {
    close = function()
      return true
    end,
    new_stream = function(_, protocols)
      stream_opened = true
      return {
        close = function()
          return true
        end,
      }, protocols[1]
    end,
  }
  local retry_stale_entry = host_connections.add(retry_host, stale_conn, stale_state)
  local retry_fresh_entry = host_connections.add(retry_host, fresh_conn, fresh_state)
  local original_dial = retry_host.dial
  retry_host.dial = function(_, _, opts)
    if opts and opts.force then
      return retry_fresh_entry.conn, retry_fresh_entry.state
    end
    return retry_stale_entry.conn, retry_stale_entry.state
  end
  local stream, retry_selected, _, stream_err = retry_host:new_stream("peer-retry", { "/tests/retry/1.0.0" }, {})
  retry_host.dial = original_dial
  if not stream then
    return nil, stream_err
  end
  if retry_selected ~= "/tests/retry/1.0.0" or not stream_opened then
    return nil, "new_stream should retry once with force after stale closed reuse"
  end
  if retry_host:_find_connection_by_id(stale_entry.id) ~= nil then
    return nil, "stale reused connection should be unregistered after closed stream error"
  end

  return true
end

return {
  name = "host connection indexes",
  run = run,
}
