--- Host connection indexing internals.
-- @module lua_libp2p.host.connections
local M = {}

local error_mod = require("lua_libp2p.error")

local function now_seconds()
  local ok_socket, socket = pcall(require, "socket")
  if ok_socket and type(socket.gettime) == "function" then
    return socket.gettime()
  end
  return os.time()
end

local function entry_is_usable(entry)
  if not entry or not entry.conn then
    return false
  end
  local task = entry.scheduler_pump_task
  if task and (task.status == "failed" or task.status == "cancelled") then
    return false
  end
  local conn = entry.conn
  if conn._closed == true then
    return false
  end
  if type(conn.is_closed) == "function" then
    local ok, closed = pcall(conn.is_closed, conn)
    if ok and closed then
      return false
    end
  end
  return true
end

function M.init(host)
  host._connections = {}
  host._connections_by_peer = {}
  host._connections_by_id = {}
  host._next_connection_id = 1
end

function M.add(host, conn, state)
  state = state or {}
  state.direction = state.direction or "unknown"

  local connection_id = host._next_connection_id
  host._next_connection_id = host._next_connection_id + 1

  local entry = {
    id = connection_id,
    conn = conn,
    state = state,
    opened_at = now_seconds(),
  }
  entry.state.connection_id = connection_id

  host._connections[#host._connections + 1] = entry
  host._connections_by_id[connection_id] = entry

  local peer_id = entry.state.remote_peer_id
  if peer_id then
    host._connections_by_peer[peer_id] = host._connections_by_peer[peer_id] or {}
    host._connections_by_peer[peer_id][connection_id] = entry
  end

  return entry
end

function M.remove(host, entry, index)
  if not entry then
    return false
  end

  local actual_index = index
  if actual_index == nil or host._connections[actual_index] ~= entry then
    actual_index = nil
    for i = #host._connections, 1, -1 do
      if host._connections[i] == entry then
        actual_index = i
        break
      end
    end
  end

  if actual_index then
    table.remove(host._connections, actual_index)
  end

  if entry.id ~= nil then
    host._connections_by_id[entry.id] = nil
  end

  local peer_id = entry.state and entry.state.remote_peer_id
  if peer_id and host._connections_by_peer[peer_id] then
    if entry.id ~= nil then
      host._connections_by_peer[peer_id][entry.id] = nil
    end
    if next(host._connections_by_peer[peer_id]) == nil then
      host._connections_by_peer[peer_id] = nil
    end
  end

  return true
end

function M.snapshot(host)
  local out = {}
  for i, entry in ipairs(host._connections) do
    out[i] = entry
  end
  return out
end

function M.reset(host)
  host._connections = {}
  host._connections_by_peer = {}
  host._connections_by_id = {}
end

function M.install(Host)
  --- Locate existing connection for peer.
  -- `opts.allow_limited_connection=true` permits returning limited relay links.
  function Host:_find_connection(peer_id, opts)
    if not peer_id then
      return nil
    end
    local options = opts or {}
    local by_peer = self._connections_by_peer[peer_id]
    if not by_peer then
      return nil
    end
    local stale = {}
    local usable = {}
    for _, entry in pairs(by_peer) do
      if entry_is_usable(entry) then
        usable[#usable + 1] = entry
      else
        stale[#stale + 1] = entry
      end
    end
    for _, entry in ipairs(stale) do
      self:_unregister_connection(nil, entry, error_mod.new("closed", "stale connection reaped during lookup"))
    end

    table.sort(usable, function(a, b)
      return (a.id or 0) > (b.id or 0)
    end)

    local limited = nil
    for _, entry in ipairs(usable) do
      if self:_connection_is_limited(entry.state) then
        limited = limited or entry
      else
        return entry
      end
    end
    if options.require_unlimited_connection then
      return nil
    end
    return limited
  end

  function Host:_find_connection_by_id(connection_id)
    return self._connections_by_id[connection_id]
  end
end

return M
