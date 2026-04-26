local ed25519 = require("lua_libp2p.crypto.ed25519")
local host_mod = require("lua_libp2p.host")

local function run()
  local keypair = assert(ed25519.generate_keypair())
  local connected_count = 0
  local disconnected_count = 0

  local host, host_err = host_mod.new({
    identity = keypair,
    on = {
      peer_connected = function(payload)
        if payload and payload.peer_id == "peer-a" then
          connected_count = connected_count + 1
        end
      end,
      peer_disconnected = function(payload)
        if payload and payload.peer_id == "peer-a" then
          disconnected_count = disconnected_count + 1
        end
      end,
    },
  })
  if not host then
    return nil, host_err
  end

  local bus_all, bus_all_err = host:subscribe()
  if not bus_all then
    return nil, bus_all_err
  end
  local bus_disconnected = assert(host:subscribe("peer_disconnected"))
  local bus_self = assert(host:subscribe("self_peer_update"))

  local conn = {
    closed = false,
  }
  function conn:close()
    self.closed = true
    return true
  end

  local entry, register_err = host:_register_connection(conn, {
    remote_peer_id = "peer-a",
  })
  if not entry then
    return nil, register_err
  end
  if connected_count ~= 1 then
    return nil, "expected peer_connected callback to fire"
  end

  local ev1 = host:next_event(bus_all)
  if not ev1 or ev1.name ~= "peer_connected" then
    return nil, "expected peer_connected from event bus subscription"
  end
  local ev1b = host:next_event(bus_all)
  if not ev1b or ev1b.name ~= "connection_opened" then
    return nil, "expected connection_opened from event bus subscription"
  end

  local closed, close_err = host:close()
  if not closed then
    return nil, close_err
  end
  if not conn.closed then
    return nil, "expected close() to close tracked connection"
  end
  if disconnected_count ~= 1 then
    return nil, "expected peer_disconnected callback to fire"
  end

  local ev2 = host:next_event(bus_all)
  if not ev2 or ev2.name ~= "peer_disconnected" then
    return nil, "expected peer_disconnected from event bus subscription"
  end

  local ev2b = host:next_event(bus_disconnected)
  if not ev2b or ev2b.name ~= "peer_disconnected" then
    return nil, "expected filtered subscription to receive peer_disconnected"
  end

  local called = 0
  local handler = function()
    called = called + 1
  end
  assert(host:on("peer_connected", handler))
  if not host:off("peer_connected", handler) then
    return nil, "expected off() to remove registered handler"
  end

  local c2 = { close = function() return true end }
  local e2, e2_err = host:_register_connection(c2, { remote_peer_id = "peer-b" })
  if not e2 then
    return nil, e2_err
  end
  if called ~= 0 then
    return nil, "removed handler should not be called"
  end

  if not host:unsubscribe(bus_disconnected) then
    return nil, "expected unsubscribe to remove subscription"
  end
  local self_update_ok, self_update_err = host:_emit_self_peer_update_if_changed()
  if not self_update_ok then
    return nil, self_update_err
  end
  local self_update = host:next_event(bus_self)
  if not self_update or self_update.name ~= "self_peer_update" then
    return nil, "expected self_peer_update event"
  end
  local no_update_ok, no_update_err = host:_emit_self_peer_update_if_changed()
  if not no_update_ok then
    return nil, no_update_err
  end
  if host:next_event(bus_self) ~= nil then
    return nil, "self_peer_update should only emit when addrs change"
  end
  if not host:unsubscribe(bus_self) then
    return nil, "expected unsubscribe to remove self subscription"
  end
  if not host:unsubscribe(bus_all) then
    return nil, "expected unsubscribe to remove subscription"
  end

  return true
end

return {
  name = "host lifecycle events queue and hooks",
  run = run,
}
