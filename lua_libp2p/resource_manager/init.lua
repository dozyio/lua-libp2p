--- Count-based resource manager.
--
-- This is a small, Go-inspired resource manager. It enforces hard limits at
-- reservation time for connections and streams across system, transient, peer,
-- and protocol scopes. Memory and file-descriptor accounting can be layered on
-- top of this API later without changing host integration points.
-- @module lua_libp2p.resource_manager
local error_mod = require("lua_libp2p.error")

local M = {}

local ResourceManager = {}
ResourceManager.__index = ResourceManager

local function copy_limits(limits)
  local out = {}
  for k, v in pairs(limits or {}) do
    if type(v) == "table" then
      out[k] = copy_limits(v)
    else
      out[k] = v
    end
  end
  return out
end

local function merge_limits(base, overrides)
  local out = copy_limits(base)
  for k, v in pairs(overrides or {}) do
    if k ~= "limits" and k ~= "default_limits" then
      if type(v) == "table" and type(out[k]) == "table" then
        out[k] = merge_limits(out[k], v)
      else
        out[k] = copy_limits({ value = v }).value
      end
    end
  end
  return out
end

local DEFAULT_LIMITS = {
  connections = 1024,
  connections_inbound = 800,
  connections_outbound = 1024,
  transient_connections = 256,

  streams = 2048,
  streams_inbound = 1024,
  streams_outbound = 2048,
  transient_streams = 256,

  connections_per_peer = 8,
  connections_inbound_per_peer = 8,
  connections_outbound_per_peer = 8,
  streams_per_peer = 512,
  streams_inbound_per_peer = 256,
  streams_outbound_per_peer = 512,

  protocol = {
    default = {
      streams = 2048,
      streams_inbound = 512,
      streams_outbound = 2048,
    },
  },

  protocol_peer = {
    default = {
      streams = 256,
      streams_inbound = 64,
      streams_outbound = 128,
    },
  },
}

local function bump(map, key, delta)
  if key == nil then
    return
  end
  local next_value = (map[key] or 0) + delta
  if next_value <= 0 then
    map[key] = nil
  else
    map[key] = next_value
  end
end

local function direction_key(prefix, direction)
  if direction == "inbound" then
    return prefix .. "_inbound"
  end
  if direction == "outbound" then
    return prefix .. "_outbound"
  end
  return nil
end

local function limit_error(resource, scope, limit, current, details)
  local context = details or {}
  context.resource = resource
  context.scope = scope
  context.limit = limit
  context.current = current
  return error_mod.new("resource", "resource limit exceeded", context)
end

local function check_limit(limits, name, current, details)
  local limit = limits[name]
  if limit ~= nil and limit >= 0 and current >= limit then
    return nil, limit_error(name, details and details.scope or "system", limit, current, details)
  end
  return true
end

function ResourceManager:_peer_limit(peer_id, name)
  local peer_limits = self.limits.peer
  if type(peer_limits) == "table" then
    local specific = peer_id and peer_limits[peer_id]
    if type(specific) == "table" and specific[name] ~= nil then
      return specific[name]
    end
    local defaults = peer_limits.default
    if type(defaults) == "table" and defaults[name] ~= nil then
      return defaults[name]
    end
  end
  return self.limits[name .. "_per_peer"]
end

function ResourceManager:_protocol_limit(protocol_id, name)
  local protocol_limits = self.limits.protocol
  if type(protocol_limits) == "table" then
    local specific = protocol_id and protocol_limits[protocol_id]
    if type(specific) == "table" and specific[name] ~= nil then
      return specific[name]
    end
    local defaults = protocol_limits.default
    if type(defaults) == "table" and defaults[name] ~= nil then
      return defaults[name]
    end
  end
  return nil
end

function ResourceManager:_protocol_peer_limit(protocol_id, name)
  local protocol_peer_limits = self.limits.protocol_peer
  if type(protocol_peer_limits) == "table" then
    local specific = protocol_id and protocol_peer_limits[protocol_id]
    if type(specific) == "table" and specific[name] ~= nil then
      return specific[name]
    end
    local defaults = protocol_peer_limits.default
    if type(defaults) == "table" and defaults[name] ~= nil then
      return defaults[name]
    end
  end
  return nil
end

function ResourceManager:_check_peer_limit(peer_id, resource, current)
  local limit = self:_peer_limit(peer_id, resource)
  current = current or 0
  if limit ~= nil and limit >= 0 and current >= limit then
    return nil, limit_error(resource, "peer", limit, current, { peer_id = peer_id })
  end
  return true
end

function ResourceManager:_check_protocol_limit(protocol_id, resource, current)
  local limit = self:_protocol_limit(protocol_id, resource)
  current = current or 0
  if limit ~= nil and limit >= 0 and current >= limit then
    return nil, limit_error(resource, "protocol", limit, current, { protocol = protocol_id })
  end
  return true
end

function ResourceManager:_check_protocol_peer_limit(protocol_id, peer_id, resource, current)
  local limit = self:_protocol_peer_limit(protocol_id, resource)
  current = current or 0
  if limit ~= nil and limit >= 0 and current >= limit then
    return nil,
      limit_error(resource, "protocol_peer", limit, current, {
        protocol = protocol_id,
        peer_id = peer_id,
      })
  end
  return true
end

function ResourceManager:_protocol_peer_stat(protocol_id, peer_id)
  local by_protocol = protocol_id and self._protocol_peer[protocol_id]
  if not by_protocol or not peer_id then
    return self:_new_counter()
  end
  return by_protocol[peer_id] or self:_new_counter()
end

function ResourceManager:_bump_protocol_peer(protocol_id, peer_id, key, delta)
  if not protocol_id or not peer_id then
    return
  end
  self._protocol_peer[protocol_id] = self._protocol_peer[protocol_id] or {}
  self._protocol_peer[protocol_id][peer_id] = self._protocol_peer[protocol_id][peer_id] or self:_new_counter()
  bump(self._protocol_peer[protocol_id][peer_id], key, delta)
  if next(self._protocol_peer[protocol_id][peer_id]) == nil then
    self._protocol_peer[protocol_id][peer_id] = nil
  end
  if next(self._protocol_peer[protocol_id]) == nil then
    self._protocol_peer[protocol_id] = nil
  end
end

function ResourceManager:_check_protocol_peer_stream_limits(peer_id, direction, protocol_id)
  if not peer_id or not protocol_id then
    return true
  end
  local stats = self:_protocol_peer_stat(protocol_id, peer_id)
  local ok, err = self:_check_protocol_peer_limit(protocol_id, peer_id, "streams", stats.streams)
  if not ok then
    return nil, err
  end
  local dir_limit_name = direction_key("streams", direction)
  if dir_limit_name then
    ok, err = self:_check_protocol_peer_limit(protocol_id, peer_id, dir_limit_name, stats[dir_limit_name])
    if not ok then
      return nil, err
    end
  end
  return true
end

--- Reserve a connection slot.
-- @tparam string direction `inbound`, `outbound`, or `unknown`.
-- @tparam[opt] string peer_id Remote peer id when already known.
-- @tparam[opt] table opts Supports `transient=true`.
-- @treturn table|nil scope
-- @treturn[opt] table err
function ResourceManager:open_connection(direction, peer_id, opts)
  local dir = direction or "unknown"
  local options = opts or {}
  local stats = self._stats

  local ok, err = check_limit(self.limits, "connections", stats.connections, { scope = "system", direction = dir })
  if not ok then
    return nil, err
  end
  local dir_limit_name = direction_key("connections", dir)
  if dir_limit_name then
    ok, err = check_limit(self.limits, dir_limit_name, stats[dir_limit_name], { scope = "system", direction = dir })
    if not ok then
      return nil, err
    end
  end
  if options.transient or not peer_id then
    ok, err = check_limit(self.limits, "transient_connections", stats.transient_connections, {
      scope = "transient",
      direction = dir,
    })
    if not ok then
      return nil, err
    end
  end
  if peer_id then
    local peer_stats = self:peer_stat(peer_id)
    ok, err = self:_check_peer_limit(peer_id, "connections", peer_stats.connections)
    if not ok then
      return nil, err
    end
    local peer_dir_limit_name = direction_key("connections", dir)
    if peer_dir_limit_name then
      ok, err = self:_check_peer_limit(peer_id, peer_dir_limit_name, peer_stats[peer_dir_limit_name])
      if not ok then
        return nil, err
      end
    end
  end

  local scope = {
    manager = self,
    kind = "connection",
    direction = dir,
    peer_id = nil,
    transient = options.transient or not peer_id,
    closed = false,
  }
  stats.connections = stats.connections + 1
  if dir_limit_name then
    stats[dir_limit_name] = stats[dir_limit_name] + 1
  end
  if scope.transient then
    stats.transient_connections = stats.transient_connections + 1
  end
  if peer_id then
    self:set_connection_peer(scope, peer_id)
  end
  return scope
end

function ResourceManager:set_connection_peer(scope, peer_id)
  if not scope or scope.closed or scope.peer_id == peer_id then
    return true
  end
  local ok, err = self:_check_peer_limit(peer_id, "connections", self:peer_stat(peer_id).connections)
  if not ok then
    return nil, err
  end
  local peer_stats = self:peer_stat(peer_id)
  local peer_dir_limit_name = direction_key("connections", scope.direction)
  if peer_dir_limit_name then
    ok, err = self:_check_peer_limit(peer_id, peer_dir_limit_name, peer_stats[peer_dir_limit_name])
    if not ok then
      return nil, err
    end
  end
  if scope.peer_id then
    bump(self._peer[scope.peer_id], "connections", -1)
    if peer_dir_limit_name then
      bump(self._peer[scope.peer_id], peer_dir_limit_name, -1)
    end
  end
  scope.peer_id = peer_id
  self._peer[peer_id] = self._peer[peer_id] or self:_new_counter()
  bump(self._peer[peer_id], "connections", 1)
  if peer_dir_limit_name then
    bump(self._peer[peer_id], peer_dir_limit_name, 1)
  end
  if scope.transient then
    self._stats.transient_connections = math.max(0, self._stats.transient_connections - 1)
    scope.transient = false
  end
  return true
end

function ResourceManager:close_connection(scope)
  if not scope or scope.closed then
    return true
  end
  scope.closed = true
  local stats = self._stats
  stats.connections = math.max(0, stats.connections - 1)
  local dir_limit_name = direction_key("connections", scope.direction)
  if dir_limit_name then
    stats[dir_limit_name] = math.max(0, stats[dir_limit_name] - 1)
  end
  if scope.transient then
    stats.transient_connections = math.max(0, stats.transient_connections - 1)
  end
  if scope.peer_id and self._peer[scope.peer_id] then
    bump(self._peer[scope.peer_id], "connections", -1)
    local peer_dir_limit_name = direction_key("connections", scope.direction)
    if peer_dir_limit_name then
      bump(self._peer[scope.peer_id], peer_dir_limit_name, -1)
    end
  end
  return true
end

--- Reserve a stream slot.
-- Streams start in transient scope until a protocol is known.
function ResourceManager:open_stream(peer_id, direction, protocol_id)
  local dir = direction or "unknown"
  local stats = self._stats
  local ok, err = check_limit(self.limits, "streams", stats.streams, { scope = "system", direction = dir })
  if not ok then
    return nil, err
  end
  local dir_limit_name = direction_key("streams", dir)
  if dir_limit_name then
    ok, err = check_limit(self.limits, dir_limit_name, stats[dir_limit_name], { scope = "system", direction = dir })
    if not ok then
      return nil, err
    end
  end
  if not protocol_id then
    ok, err = check_limit(self.limits, "transient_streams", stats.transient_streams, {
      scope = "transient",
      direction = dir,
    })
    if not ok then
      return nil, err
    end
  end
  if peer_id then
    local peer_stats = self:peer_stat(peer_id)
    ok, err = self:_check_peer_limit(peer_id, "streams", peer_stats.streams)
    if not ok then
      return nil, err
    end
    local peer_dir_limit_name = direction_key("streams", dir)
    if peer_dir_limit_name then
      ok, err = self:_check_peer_limit(peer_id, peer_dir_limit_name, peer_stats[peer_dir_limit_name])
      if not ok then
        return nil, err
      end
    end
  end
  if protocol_id then
    local protocol_stats = self:protocol_stat(protocol_id)
    ok, err = self:_check_protocol_limit(protocol_id, "streams", protocol_stats.streams)
    if not ok then
      return nil, err
    end
    local protocol_dir_limit_name = direction_key("streams", dir)
    if protocol_dir_limit_name then
      ok, err =
        self:_check_protocol_limit(protocol_id, protocol_dir_limit_name, protocol_stats[protocol_dir_limit_name])
      if not ok then
        return nil, err
      end
    end
  end
  ok, err = self:_check_protocol_peer_stream_limits(peer_id, dir, protocol_id)
  if not ok then
    return nil, err
  end

  local scope = {
    manager = self,
    kind = "stream",
    direction = dir,
    peer_id = peer_id,
    protocol = nil,
    transient = protocol_id == nil,
    closed = false,
  }
  stats.streams = stats.streams + 1
  if dir_limit_name then
    stats[dir_limit_name] = stats[dir_limit_name] + 1
  end
  if scope.transient then
    stats.transient_streams = stats.transient_streams + 1
  end
  if peer_id then
    self._peer[peer_id] = self._peer[peer_id] or self:_new_counter()
    bump(self._peer[peer_id], "streams", 1)
    local peer_dir_limit_name = direction_key("streams", dir)
    if peer_dir_limit_name then
      bump(self._peer[peer_id], peer_dir_limit_name, 1)
    end
  end
  if protocol_id then
    local set_ok, set_err = self:set_stream_protocol(scope, protocol_id)
    if not set_ok then
      self:close_stream(scope)
      return nil, set_err
    end
  end
  return scope
end

function ResourceManager:set_stream_protocol(scope, protocol_id)
  if not scope or scope.closed or scope.protocol == protocol_id then
    return true
  end
  local protocol_stats = self:protocol_stat(protocol_id)
  local ok, err = self:_check_protocol_limit(protocol_id, "streams", protocol_stats.streams)
  if not ok then
    return nil, err
  end
  local protocol_dir_limit_name = direction_key("streams", scope.direction)
  if protocol_dir_limit_name then
    ok, err = self:_check_protocol_limit(protocol_id, protocol_dir_limit_name, protocol_stats[protocol_dir_limit_name])
    if not ok then
      return nil, err
    end
  end
  ok, err = self:_check_protocol_peer_stream_limits(scope.peer_id, scope.direction, protocol_id)
  if not ok then
    return nil, err
  end
  if scope.protocol and self._protocol[scope.protocol] then
    bump(self._protocol[scope.protocol], "streams", -1)
    if protocol_dir_limit_name then
      bump(self._protocol[scope.protocol], protocol_dir_limit_name, -1)
    end
    self:_bump_protocol_peer(scope.protocol, scope.peer_id, "streams", -1)
    if protocol_dir_limit_name then
      self:_bump_protocol_peer(scope.protocol, scope.peer_id, protocol_dir_limit_name, -1)
    end
  end
  scope.protocol = protocol_id
  self._protocol[protocol_id] = self._protocol[protocol_id] or self:_new_counter()
  bump(self._protocol[protocol_id], "streams", 1)
  if protocol_dir_limit_name then
    bump(self._protocol[protocol_id], protocol_dir_limit_name, 1)
  end
  self:_bump_protocol_peer(protocol_id, scope.peer_id, "streams", 1)
  if protocol_dir_limit_name then
    self:_bump_protocol_peer(protocol_id, scope.peer_id, protocol_dir_limit_name, 1)
  end
  if scope.transient then
    self._stats.transient_streams = math.max(0, self._stats.transient_streams - 1)
    scope.transient = false
  end
  return true
end

function ResourceManager:close_stream(scope)
  if not scope or scope.closed then
    return true
  end
  scope.closed = true
  local stats = self._stats
  stats.streams = math.max(0, stats.streams - 1)
  local dir_limit_name = direction_key("streams", scope.direction)
  if dir_limit_name then
    stats[dir_limit_name] = math.max(0, stats[dir_limit_name] - 1)
  end
  if scope.transient then
    stats.transient_streams = math.max(0, stats.transient_streams - 1)
  end
  if scope.peer_id and self._peer[scope.peer_id] then
    bump(self._peer[scope.peer_id], "streams", -1)
    local peer_dir_limit_name = direction_key("streams", scope.direction)
    if peer_dir_limit_name then
      bump(self._peer[scope.peer_id], peer_dir_limit_name, -1)
    end
  end
  if scope.protocol and self._protocol[scope.protocol] then
    bump(self._protocol[scope.protocol], "streams", -1)
    local protocol_dir_limit_name = direction_key("streams", scope.direction)
    if protocol_dir_limit_name then
      bump(self._protocol[scope.protocol], protocol_dir_limit_name, -1)
    end
    self:_bump_protocol_peer(scope.protocol, scope.peer_id, "streams", -1)
    if protocol_dir_limit_name then
      self:_bump_protocol_peer(scope.protocol, scope.peer_id, protocol_dir_limit_name, -1)
    end
  end
  return true
end

function ResourceManager:_new_counter()
  return {
    connections = 0,
    connections_inbound = 0,
    connections_outbound = 0,
    streams = 0,
    streams_inbound = 0,
    streams_outbound = 0,
  }
end

function ResourceManager:peer_stat(peer_id)
  return self._peer[peer_id] or self:_new_counter()
end

function ResourceManager:protocol_stat(protocol_id)
  return self._protocol[protocol_id] or self:_new_counter()
end

function ResourceManager:stats()
  local peer = {}
  for k, v in pairs(self._peer) do
    peer[k] = {
      connections = v.connections or 0,
      connections_inbound = v.connections_inbound or 0,
      connections_outbound = v.connections_outbound or 0,
      streams = v.streams or 0,
      streams_inbound = v.streams_inbound or 0,
      streams_outbound = v.streams_outbound or 0,
    }
  end
  local protocol = {}
  for k, v in pairs(self._protocol) do
    protocol[k] = {
      connections = v.connections or 0,
      streams = v.streams or 0,
      streams_inbound = v.streams_inbound or 0,
      streams_outbound = v.streams_outbound or 0,
    }
  end
  local protocol_peer = {}
  for protocol_id, by_peer in pairs(self._protocol_peer) do
    protocol_peer[protocol_id] = {}
    for peer_id, v in pairs(by_peer) do
      protocol_peer[protocol_id][peer_id] = {
        connections = v.connections or 0,
        connections_inbound = v.connections_inbound or 0,
        connections_outbound = v.connections_outbound or 0,
        streams = v.streams or 0,
        streams_inbound = v.streams_inbound or 0,
        streams_outbound = v.streams_outbound or 0,
      }
    end
  end
  return {
    system = {
      connections = self._stats.connections,
      connections_inbound = self._stats.connections_inbound,
      connections_outbound = self._stats.connections_outbound,
      streams = self._stats.streams,
      streams_inbound = self._stats.streams_inbound,
      streams_outbound = self._stats.streams_outbound,
    },
    transient = {
      connections = self._stats.transient_connections,
      streams = self._stats.transient_streams,
    },
    peer = peer,
    protocol = protocol,
    protocol_peer = protocol_peer,
    limits = copy_limits(self.limits),
  }
end

function ResourceManager:close()
  self.closed = true
  return true
end

function M.default_limits()
  return copy_limits(DEFAULT_LIMITS)
end

--- Construct a count-based resource manager.
-- Limits default to a practical Go-inspired count subset. A limit of `0` blocks the
-- resource. Pass `default_limits=false` to start from no limits. Supported top-level limits include: `connections`,
-- `connections_inbound`, `connections_outbound`, `transient_connections`,
-- `connections_per_peer`, `connections_inbound_per_peer`,
-- `connections_outbound_per_peer`, `streams`, `streams_inbound`,
-- `streams_outbound`, `transient_streams`, `streams_per_peer`,
-- `streams_inbound_per_peer`, and `streams_outbound_per_peer`. `protocol`,
-- `protocol_peer`, and `peer` maps can
-- override limits with `{ default = {...}, [id] = {...} }` entries.
function M.new(opts)
  local options = opts or {}
  local overrides = options.limits or options
  local limits = options.default_limits == false and merge_limits({}, overrides)
    or merge_limits(DEFAULT_LIMITS, overrides)
  return setmetatable({
    limits = limits,
    _stats = {
      connections = 0,
      connections_inbound = 0,
      connections_outbound = 0,
      streams = 0,
      streams_inbound = 0,
      streams_outbound = 0,
      transient_connections = 0,
      transient_streams = 0,
    },
    _peer = {},
    _protocol = {},
    _protocol_peer = {},
    closed = false,
  }, ResourceManager)
end

return M
