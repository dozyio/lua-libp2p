--- Connection manager with dial queue and reuse policy.
-- @module lua_libp2p.connection_manager
---@class Libp2pConnectionManagerConfig
---@field max_parallel_dials? integer
---@field max_dial_queue_length? integer
---@field max_peer_addrs_to_dial? integer
---@field max_dial_addrs? integer Alias for `max_peer_addrs_to_dial`.
---@field address_dial_timeout? number
---@field dial_timeout? number
---@field max_connections? integer
---@field max_inbound_connections? integer
---@field max_outbound_connections? integer
---@field max_connections_per_peer? integer
---@field low_water? integer
---@field high_water? integer
---@field grace_period? number
---@field silence_period? number

---@class Libp2pConnectionManagerInstance
---@field can_open_connection fun(self: Libp2pConnectionManagerInstance, state: table): boolean|nil, table|nil
---@field on_connection_opened? fun(self: Libp2pConnectionManagerInstance, entry: table): boolean|nil, table|nil
---@field on_connection_closed? fun(self: Libp2pConnectionManagerInstance, entry: table): any
---@field open_connection? fun(self: Libp2pConnectionManagerInstance, peer_or_addr: string|table, opts?: table): table|nil, table|nil, table|nil
---@field stats? fun(self: Libp2pConnectionManagerInstance): table
local error_mod = require("lua_libp2p.error")
local multiaddr = require("lua_libp2p.multiaddr")

local M = {}

local DEFAULT_MAX_PARALLEL_DIALS = 100
local DEFAULT_MAX_DIAL_QUEUE_LENGTH = 500
local DEFAULT_MAX_PEER_ADDRS_TO_DIAL = 3
local DEFAULT_ADDRESS_DIAL_TIMEOUT = 6
local DEFAULT_HIGH_WATER = 192
local DEFAULT_LOW_WATER = 160
local DEFAULT_GRACE_PERIOD = 60
local DEFAULT_SILENCE_PERIOD = 10

local ConnectionManager = {}
ConnectionManager.__index = ConnectionManager

local function now_seconds()
  local ok_socket, socket = pcall(require, "socket")
  if ok_socket and type(socket.gettime) == "function" then
    return socket.gettime()
  end
  return os.time()
end

local function target_key(target)
  if type(target) == "table" then
    local addr = type(target.addr) == "string" and target.addr or nil
    if addr and multiaddr.is_relay_addr(addr) then
      local relay, relay_err = multiaddr.relay_info(addr)
      if relay then
        return "relay:"
          .. tostring(relay.relay_peer_id or "")
          .. "->"
          .. tostring(relay.destination_peer_id or target.peer_id or "")
      end
      return "relay_addr:" .. addr .. ":" .. tostring(relay_err)
    end
    if type(target.peer_id) == "string" and target.peer_id ~= "" then
      return "peer:" .. target.peer_id
    end
    if addr and addr ~= "" then
      return "addr:" .. addr
    end
  elseif type(target) == "string" and target ~= "" then
    if target:sub(1, 1) == "/" then
      if multiaddr.is_relay_addr(target) then
        local relay, relay_err = multiaddr.relay_info(target)
        if relay then
          return "relay:" .. tostring(relay.relay_peer_id or "") .. "->" .. tostring(relay.destination_peer_id or "")
        end
        return "relay_addr:" .. target .. ":" .. tostring(relay_err)
      end
      return "addr:" .. target
    end
    return "peer:" .. target
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
  if
    host_part.protocol ~= "ip4"
    and host_part.protocol ~= "ip6"
    and host_part.protocol ~= "dns"
    and host_part.protocol ~= "dns4"
    and host_part.protocol ~= "dns6"
  then
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

--- Construct a connection manager.
-- `opts` supports queue/dial limits and connection watermarks:
-- `max_parallel_dials`, `max_dial_queue_length`, `max_peer_addrs_to_dial`,
-- `address_dial_timeout`, `dial_timeout`, `max_connections`,
-- `max_inbound_connections`, `max_outbound_connections`,
-- `max_connections_per_peer`, `low_water`, `high_water`, `grace_period`, and `silence_period`.
-- @tparam table host Host instance.
-- @tparam[opt] table opts
-- @treturn table manager
function M.new(host, opts)
  local options = opts or {}
  return setmetatable({
    host = host,
    max_parallel_dials = options.max_parallel_dials == nil and DEFAULT_MAX_PARALLEL_DIALS or options.max_parallel_dials,
    max_dial_queue_length = options.max_dial_queue_length == nil and DEFAULT_MAX_DIAL_QUEUE_LENGTH
      or options.max_dial_queue_length,
    max_peer_addrs_to_dial = options.max_peer_addrs_to_dial or options.max_dial_addrs or DEFAULT_MAX_PEER_ADDRS_TO_DIAL,
    address_dial_timeout = options.address_dial_timeout or DEFAULT_ADDRESS_DIAL_TIMEOUT,
    dial_timeout = options.dial_timeout,
    max_connections = options.max_connections,
    max_inbound_connections = options.max_inbound_connections,
    max_outbound_connections = options.max_outbound_connections,
    max_connections_per_peer = options.max_connections_per_peer,
    low_water = options.low_water == nil and DEFAULT_LOW_WATER or options.low_water,
    high_water = options.high_water or options.max_connections or DEFAULT_HIGH_WATER,
    grace_period = options.grace_period == nil and DEFAULT_GRACE_PERIOD or options.grace_period,
    silence_period = options.silence_period == nil and DEFAULT_SILENCE_PERIOD or options.silence_period,
    last_prune_at = nil,
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
    grace_period = self.grace_period,
    silence_period = self.silence_period,
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

--- Rank and filter candidate addresses for dialing.
-- `opts.max_peer_addrs_to_dial` / `opts.max_dial_addrs` limits returned count.
-- @tparam table addrs
-- @tparam[opt] table opts
-- @treturn table ranked
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

--- Normalize dial options with manager defaults.
-- `opts` may include `address_dial_timeout`, `dial_timeout`, `timeout`, `ctx`,
-- `require_unlimited_connection`, and `allow_limited_connection`.
-- @tparam[opt] table opts
-- @treturn table options
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

--- Internal queued dial execution.
-- `opts` is normalized via @{prepare_dial_options}; `ctx` provides cancellation
-- and cooperative wait primitives.
-- `opts.timeout` / `opts.address_dial_timeout` / `opts.dial_timeout` affect dial timing.
function ConnectionManager:_run_dial(target, opts, key, ctx)
  local token = {}
  self.queue[#self.queue + 1] = token
  while self.queue[1] ~= token or self.active_dials >= self.max_parallel_dials do
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

--- Open or join a connection dial task for a target.
-- `opts` supports `ctx`, `force`, `timeout`, `address_dial_timeout`,
-- `dial_timeout`, `require_unlimited_connection`, and `allow_limited_connection`.
-- @param target Dial target.
-- @tparam[opt] table opts
-- @treturn table|nil conn
-- @treturn[opt] table state
-- @treturn[opt] table err
function ConnectionManager:open_connection(target, opts)
  local options = opts or {}
  local ctx = options.ctx
  if options.force == true then
    return self.host:_dial_direct(target, self:prepare_dial_options(options))
  end
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
    return nil,
      nil,
      error_mod.new("resource", "dial queue full", {
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

function ConnectionManager:untag_peer(peer_id, tag)
  if type(peer_id) ~= "string" or peer_id == "" then
    return nil, error_mod.new("input", "peer id is required")
  end
  local tags = self.peer_tags[peer_id]
  if not tags then
    return false
  end
  if tag == nil then
    self.peer_tags[peer_id] = nil
    return true
  end
  tags[tag] = nil
  if next(tags) == nil then
    self.peer_tags[peer_id] = nil
  end
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

  if
    self.max_connections_per_peer
    and peer_id
    and self:peer_connection_count(peer_id) >= self.max_connections_per_peer
  then
    return nil,
      error_mod.new("resource", "per-peer connection limit reached", {
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
    local pruned, prune_err = self:prune_if_needed(
      true,
      { direction = direction, high = direction_limit, low = math.max(direction_limit - 1, 0) }
    )
    if not pruned then
      return nil, prune_err
    end
    if self:connection_count(direction) >= direction_limit then
      return nil,
        error_mod.new("resource", direction .. " connection limit reached", {
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
      return nil,
        error_mod.new("resource", "connection limit reached", {
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

--- Prune low-value connections to satisfy limits.
-- `opts` supports `direction`, `high`, and `low` watermarks.
-- @tparam boolean force
-- @tparam[opt] table opts
-- @treturn true|nil ok
-- @treturn[opt] table err
function ConnectionManager:prune_if_needed(force, opts)
  local options = opts or {}
  local direction = options.direction
  local high = options.high or self.high_water or self.max_connections
  local current = self:connection_count(direction)
  if not high or (not force and current <= high) or (force and current < high) then
    return true
  end
  local now = now_seconds()
  if not force and self.silence_period and self.silence_period > 0 and self.last_prune_at then
    if (now - self.last_prune_at) < self.silence_period then
      return true
    end
  end
  local low = options.low or self.low_water or math.max(high - 1, 0)
  local cutoff = nil
  if not force and self.grace_period and self.grace_period > 0 then
    cutoff = now - self.grace_period
  end
  local pruned_any = false
  while self:connection_count(direction) > low do
    local candidate = nil
    local candidate_value = nil
    for _, entry in pairs(self.connections_by_id) do
      local peer_id = entry.state and entry.state.remote_peer_id
      local entry_direction = entry.state and entry.state.direction or "unknown"
      local old_enough = true
      if cutoff then
        old_enough = (entry.opened_at or 0) <= cutoff
      end
      if
        old_enough
        and (not direction or entry_direction == direction)
        and not (peer_id and self.protected_peers[peer_id])
      then
        local value = self:peer_value(peer_id)
        if
          not candidate
          or value < candidate_value
          or (value == candidate_value and (entry.opened_at or 0) < (candidate.opened_at or 0))
        then
          candidate = entry
          candidate_value = value
        end
      end
    end
    if not candidate then
      if cutoff then
        return true
      else
        return nil,
          error_mod.new("resource", "connection limit reached and no prunable connections available", {
            high_water = high,
            low_water = low,
          })
      end
    else
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
      pruned_any = true
    end
  end
  if not force and pruned_any then
    self.last_prune_at = now
  end
  return true
end

M.ConnectionManager = ConnectionManager

return M
