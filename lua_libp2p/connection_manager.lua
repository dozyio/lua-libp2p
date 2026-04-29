local error_mod = require("lua_libp2p.error")
local multiaddr = require("lua_libp2p.multiaddr")

local M = {}

local DEFAULT_MAX_PARALLEL_DIALS = 100
local DEFAULT_MAX_DIAL_QUEUE_LENGTH = 500
local DEFAULT_MAX_PEER_ADDRS_TO_DIAL = 3
local DEFAULT_ADDRESS_DIAL_TIMEOUT = 6

local ConnectionManager = {}
ConnectionManager.__index = ConnectionManager

local function target_key(target)
  if type(target) == "table" then
    if type(target.peer_id) == "string" and target.peer_id ~= "" then
      return "peer:" .. target.peer_id
    end
    if type(target.addr) == "string" and target.addr ~= "" then
      return "addr:" .. target.addr
    end
  elseif type(target) == "string" and target ~= "" then
    return "target:" .. target
  end
  return nil
end

local function is_dialable_tcp_addr(addr)
  local parsed = multiaddr.parse(addr)
  if not parsed or type(parsed.components) ~= "table" or #parsed.components < 2 then
    return false
  end
  local host_part = parsed.components[1]
  local tcp_part = parsed.components[2]
  if host_part.protocol ~= "ip4" and host_part.protocol ~= "dns" and host_part.protocol ~= "dns4" and host_part.protocol ~= "dns6" then
    return false
  end
  if tcp_part.protocol ~= "tcp" then
    return false
  end
  for i = 3, #parsed.components do
    if parsed.components[i].protocol ~= "p2p" then
      return false
    end
  end
  return true
end

function M.new(host, opts)
  local options = opts or {}
  return setmetatable({
    host = host,
    max_parallel_dials = options.max_parallel_dials == nil and DEFAULT_MAX_PARALLEL_DIALS or options.max_parallel_dials,
    max_dial_queue_length = options.max_dial_queue_length == nil and DEFAULT_MAX_DIAL_QUEUE_LENGTH or options.max_dial_queue_length,
    max_peer_addrs_to_dial = options.max_peer_addrs_to_dial or options.max_dial_addrs or DEFAULT_MAX_PEER_ADDRS_TO_DIAL,
    address_dial_timeout = options.address_dial_timeout or DEFAULT_ADDRESS_DIAL_TIMEOUT,
    dial_timeout = options.dial_timeout,
    max_connections = options.max_connections,
    max_inbound_connections = options.max_inbound_connections,
    max_outbound_connections = options.max_outbound_connections,
    max_connections_per_peer = options.max_connections_per_peer,
    low_water = options.low_water,
    high_water = options.high_water or options.max_connections,
    active_dials = 0,
    queue = {},
    pending_by_key = {},
    connections_by_id = {},
    connections_by_peer = {},
    connections_by_direction = {
      inbound = {},
      outbound = {},
      unknown = {},
    },
    protected_peers = {},
    peer_tags = {},
    dialed = 0,
    joined = 0,
    failed = 0,
    pruned = 0,
  }, ConnectionManager)
end

function ConnectionManager:stats()
  return {
    active_dials = self.active_dials,
    queued_dials = #self.queue,
    pending_dials = self:pending_count(),
    dialed = self.dialed,
    joined = self.joined,
    failed = self.failed,
    pruned = self.pruned,
    connections = self:connection_count(),
    max_parallel_dials = self.max_parallel_dials,
    max_dial_queue_length = self.max_dial_queue_length,
    max_peer_addrs_to_dial = self.max_peer_addrs_to_dial,
    address_dial_timeout = self.address_dial_timeout,
    max_connections = self.max_connections,
    max_inbound_connections = self.max_inbound_connections,
    max_outbound_connections = self.max_outbound_connections,
    max_connections_per_peer = self.max_connections_per_peer,
    inbound_connections = self:connection_count("inbound"),
    outbound_connections = self:connection_count("outbound"),
    high_water = self.high_water,
    low_water = self.low_water,
  }
end

function ConnectionManager:connection_count(direction)
  if direction then
    local by_direction = self.connections_by_direction[direction] or {}
    local count = 0
    for _ in pairs(by_direction) do
      count = count + 1
    end
    return count
  end
  local count = 0
  for _ in pairs(self.connections_by_id) do
    count = count + 1
  end
  return count
end

function ConnectionManager:peer_connection_count(peer_id)
  local by_peer = peer_id and self.connections_by_peer[peer_id]
  if not by_peer then
    return 0
  end
  local count = 0
  for _ in pairs(by_peer) do
    count = count + 1
  end
  return count
end

function ConnectionManager:pending_count()
  local count = 0
  for _ in pairs(self.pending_by_key) do
    count = count + 1
  end
  return count
end

function ConnectionManager:_remove_token(token)
  for i = 1, #self.queue do
    if self.queue[i] == token then
      table.remove(self.queue, i)
      return true
    end
  end
  return false
end

function ConnectionManager:rank_addrs(addrs, opts)
  local options = opts or {}
  local seen = {}
  local public_tcp = {}
  local other_tcp = {}
  local relays = {}
  for _, addr in ipairs(addrs or {}) do
    if type(addr) == "string" and addr ~= "" and not seen[addr] then
      seen[addr] = true
      if is_dialable_tcp_addr(addr) then
        if multiaddr.is_public_addr(addr) then
          public_tcp[#public_tcp + 1] = addr
        else
          other_tcp[#other_tcp + 1] = addr
        end
      elseif multiaddr.is_relay_addr(addr) then
        relays[#relays + 1] = addr
      end
    end
  end

  local out = {}
  local max = options.max_peer_addrs_to_dial or options.max_dial_addrs or self.max_peer_addrs_to_dial
  local function append(values)
    for _, addr in ipairs(values) do
      if max <= 0 or #out < max then
        out[#out + 1] = addr
      end
    end
  end
  append(public_tcp)
  append(other_tcp)
  append(relays)
  return out
end

function ConnectionManager:prepare_dial_options(opts)
  local out = {}
  for k, v in pairs(opts or {}) do
    out[k] = v
  end
  if out.address_dial_timeout == nil then
    out.address_dial_timeout = self.address_dial_timeout
  end
  if out.dial_timeout == nil then
    out.dial_timeout = self.dial_timeout
  end
  if out.timeout == nil and out.address_dial_timeout ~= nil then
    out.timeout = out.address_dial_timeout
  end
  return out
end

function ConnectionManager:_run_dial(target, opts, key, ctx)
  local token = {}
  self.queue[#self.queue + 1] = token
  while (self.queue[1] ~= token or self.active_dials >= self.max_parallel_dials) do
    if ctx:cancelled() then
      self:_remove_token(token)
      return nil, error_mod.new("cancelled", "dial task cancelled")
    end
    local slept, sleep_err = ctx:sleep(0.01)
    if slept == nil and sleep_err then
      self:_remove_token(token)
      return nil, sleep_err
    end
  end

  table.remove(self.queue, 1)
  self.active_dials = self.active_dials + 1
  self.dialed = self.dialed + 1

  local dial_opts = self:prepare_dial_options(opts)
  dial_opts.ctx = ctx

  local conn, state, dial_err = self.host:_dial_direct(target, dial_opts)
  self.active_dials = self.active_dials - 1
  self.pending_by_key[key] = nil
  if not conn then
    self.failed = self.failed + 1
    return nil, dial_err
  end
  return { conn = conn, state = state }
end

function ConnectionManager:open_connection(target, opts)
  local options = opts or {}
  local ctx = options.ctx
  if not ctx or type(ctx.await_task) ~= "function" then
    return self.host:_dial_direct(target, self:prepare_dial_options(options))
  end

  local key = target_key(target)
  if not key then
    return nil, nil, error_mod.new("input", "dial target must include peer id or address")
  end

  local pending = self.pending_by_key[key]
  if pending then
    self.joined = self.joined + 1
    local joined, join_err = ctx:await_task(pending.task)
    if not joined then
      return nil, nil, join_err
    end
    return joined.conn, joined.state
  end

  if #self.queue >= self.max_dial_queue_length then
    return nil, nil, error_mod.new("resource", "dial queue full", {
      max_dial_queue_length = self.max_dial_queue_length,
    })
  end

  local pending_entry = {}
  self.pending_by_key[key] = pending_entry
  local task, task_err = self.host:spawn_task("connection_manager.dial", function(task_ctx)
    return self:_run_dial(target, options, key, task_ctx)
  end, {
    service = "connection_manager",
  })
  if not task then
    self.pending_by_key[key] = nil
    return nil, nil, task_err
  end
  pending_entry.task = task

  local result, wait_err = ctx:await_task(task)
  if not result then
    return nil, nil, wait_err
  end
  return result.conn, result.state
end

function ConnectionManager:protect(peer_id, tag)
  if type(peer_id) ~= "string" or peer_id == "" then
    return nil, error_mod.new("input", "peer id is required")
  end
  self.protected_peers[peer_id] = self.protected_peers[peer_id] or {}
  self.protected_peers[peer_id][tag or "protected"] = true
  return true
end

function ConnectionManager:unprotect(peer_id, tag)
  local tags = self.protected_peers[peer_id]
  if not tags then
    return false
  end
  tags[tag or "protected"] = nil
  if next(tags) == nil then
    self.protected_peers[peer_id] = nil
  end
  return true
end

function ConnectionManager:tag_peer(peer_id, tag, value)
  if type(peer_id) ~= "string" or peer_id == "" then
    return nil, error_mod.new("input", "peer id is required")
  end
  self.peer_tags[peer_id] = self.peer_tags[peer_id] or {}
  self.peer_tags[peer_id][tag or "value"] = tonumber(value) or 0
  return true
end

function ConnectionManager:peer_value(peer_id)
  local total = 0
  for _, value in pairs(self.peer_tags[peer_id] or {}) do
    total = total + (tonumber(value) or 0)
  end
  return total
end

function ConnectionManager:can_open_connection(state)
  local conn_state = state or {}
  local direction = conn_state.direction or "unknown"
  local peer_id = conn_state.remote_peer_id

  if self.max_connections_per_peer and peer_id and self:peer_connection_count(peer_id) >= self.max_connections_per_peer then
    return nil, error_mod.new("resource", "per-peer connection limit reached", {
      peer_id = peer_id,
      max_connections_per_peer = self.max_connections_per_peer,
    })
  end

  local direction_limit = nil
  if direction == "inbound" then
    direction_limit = self.max_inbound_connections
  elseif direction == "outbound" then
    direction_limit = self.max_outbound_connections
  end
  if direction_limit and self:connection_count(direction) >= direction_limit then
    local pruned, prune_err = self:prune_if_needed(true, { direction = direction, high = direction_limit, low = math.max(direction_limit - 1, 0) })
    if not pruned then
      return nil, prune_err
    end
    if self:connection_count(direction) >= direction_limit then
      return nil, error_mod.new("resource", direction .. " connection limit reached", {
        direction = direction,
        limit = direction_limit,
      })
    end
  end

  if self.max_connections and self:connection_count() >= self.max_connections then
    local pruned, prune_err = self:prune_if_needed(true)
    if not pruned then
      return nil, prune_err
    end
    if self:connection_count() >= self.max_connections then
      return nil, error_mod.new("resource", "connection limit reached", {
        max_connections = self.max_connections,
      })
    end
  end

  return true
end

function ConnectionManager:on_connection_opened(entry)
  if not entry or entry.id == nil then
    return true
  end
  self.connections_by_id[entry.id] = entry
  local peer_id = entry.state and entry.state.remote_peer_id
  if peer_id then
    self.connections_by_peer[peer_id] = self.connections_by_peer[peer_id] or {}
    self.connections_by_peer[peer_id][entry.id] = entry
  end
  local direction = entry.state and entry.state.direction or "unknown"
  self.connections_by_direction[direction] = self.connections_by_direction[direction] or {}
  self.connections_by_direction[direction][entry.id] = entry
  return self:prune_if_needed()
end

function ConnectionManager:on_connection_closed(entry)
  if not entry or entry.id == nil then
    return true
  end
  self.connections_by_id[entry.id] = nil
  local peer_id = entry.state and entry.state.remote_peer_id
  local by_peer = peer_id and self.connections_by_peer[peer_id]
  if by_peer then
    by_peer[entry.id] = nil
    if next(by_peer) == nil then
      self.connections_by_peer[peer_id] = nil
    end
  end
  local direction = entry.state and entry.state.direction or "unknown"
  local by_direction = self.connections_by_direction[direction]
  if by_direction then
    by_direction[entry.id] = nil
  end
  return true
end

function ConnectionManager:prune_if_needed(force, opts)
  local options = opts or {}
  local direction = options.direction
  local high = options.high or self.high_water or self.max_connections
  local current = self:connection_count(direction)
  if not high or (not force and current <= high) or (force and current < high) then
    return true
  end
  local low = options.low or self.low_water or math.max(high - 1, 0)
  while self:connection_count(direction) > low do
    local candidate = nil
    local candidate_value = nil
    for _, entry in pairs(self.connections_by_id) do
      local peer_id = entry.state and entry.state.remote_peer_id
      local entry_direction = entry.state and entry.state.direction or "unknown"
      if (not direction or entry_direction == direction) and not (peer_id and self.protected_peers[peer_id]) then
        local value = self:peer_value(peer_id)
        if not candidate or value < candidate_value or (value == candidate_value and (entry.opened_at or 0) < (candidate.opened_at or 0)) then
          candidate = entry
          candidate_value = value
        end
      end
    end
    if not candidate then
      return nil, error_mod.new("resource", "connection limit reached and no prunable connections available", {
        high_water = high,
        low_water = low,
      })
    end
    if candidate.conn and type(candidate.conn.close) == "function" then
      pcall(function()
        candidate.conn:close()
      end)
    end
    local ok, err = self.host:_unregister_connection(nil, candidate, error_mod.new("resource", "connection pruned"))
    if not ok then
      return nil, err
    end
    self.pruned = self.pruned + 1
  end
  return true
end

M.ConnectionManager = ConnectionManager

return M
