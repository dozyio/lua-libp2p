--- KAD-DHT random-walk routing table refresh.
-- @module lua_libp2p.kad_dht.random_walk
local error_mod = require("lua_libp2p.error")
local multiaddr = require("lua_libp2p.multiaddr")
local operation = require("lua_libp2p.operation")
local protocol = require("lua_libp2p.kad_dht.protocol")
local query = require("lua_libp2p.kad_dht.query")

local M = {}

local function is_capacity_error(err)
  return error_mod.is_error(err) and err.kind == "capacity"
end

local function is_dialable_tcp_addr(addr)
  local parsed = multiaddr.parse(addr)
  if not parsed or type(parsed.components) ~= "table" or #parsed.components < 2 then return false end
  local host_part = parsed.components[1]
  local tcp_part = parsed.components[2]
  if host_part.protocol ~= "ip4" and host_part.protocol ~= "dns" and host_part.protocol ~= "dns4" and host_part.protocol ~= "dns6" then return false end
  if tcp_part.protocol ~= "tcp" then return false end
  for i = 3, #parsed.components do
    if parsed.components[i].protocol ~= "p2p" then return false end
  end
  return true
end

local function dialable_tcp_addrs(addrs)
  local out = {}
  for _, addr in ipairs(addrs or {}) do
    if is_dialable_tcp_addr(addr) then out[#out + 1] = addr end
  end
  return out
end

--- Execute a random walk immediately and return its report.
-- This is the internal workflow used when the caller already controls any
-- scheduler context. Public callers should use @{spawn} via `DHT:random_walk`.
-- @tparam table dht DHT instance
-- @tparam[opt] table opts Walk/query controls, including `ctx` and `yield`.
-- @treturn table|nil report
-- @treturn[opt] table err
function M.run(dht, opts)
  local options = opts or {}
  local report = { queried = 0, responses = 0, failed = 0, added = 0, skipped = 0, discovered = 0, errors = {} }

  local target_key = options.target_key or dht.local_peer_id
  local target_bytes, target_err = protocol.peer_bytes(target_key)
  if not target_bytes then return nil, target_err end
  local target_hash, hash_err = dht.routing_table:_hash(target_bytes)
  if not target_hash then return nil, hash_err end

  local alpha = options.alpha or dht.alpha
  if type(alpha) ~= "number" or alpha <= 0 then return nil, error_mod.new("input", "alpha must be > 0") end
  local disjoint_paths = options.disjoint_paths or dht.disjoint_paths
  if type(disjoint_paths) ~= "number" or disjoint_paths <= 0 then return nil, error_mod.new("input", "disjoint_paths must be > 0") end

  local initial_peers = dht.routing_table:all_peers()
  if options.legacy_walker ~= true then
    local seeds = {}
    local initial_peer_ids = {}
    for _, entry in ipairs(initial_peers) do
      initial_peer_ids[entry.peer_id] = true
      local addrs = {}
      if dht.host and dht.host.peerstore then
        addrs = dialable_tcp_addrs(dht:_filter_addrs(dht.host.peerstore:get_addrs(entry.peer_id), { peer_id = entry.peer_id, purpose = "random_walk_seed" }))
      end
      if #addrs > 0 then
        seeds[#seeds + 1] = { peer_id = entry.peer_id, addrs = addrs }
      elseif not (dht.host and dht.host.peerstore) then
        seeds[#seeds + 1] = { peer_id = entry.peer_id }
      end
    end
    local closest, lookup = dht:_get_closest_peers(target_key, {
      peers = seeds,
      alpha = alpha,
      disjoint_paths = disjoint_paths,
      count = dht.k,
      scheduler_task = options.scheduler_task,
      ctx = options.ctx,
    })
    if not closest then return nil, lookup end
    report.queried = lookup.queried or 0
    report.responses = lookup.responses or 0
    report.failed = lookup.failed or 0
    report.errors = lookup.errors or {}
    report.discovered = #(lookup.closest_peers or {})
    report.termination = lookup.termination
    report.active_peak = lookup.active_peak
    for _, peer in ipairs(lookup.queried_peers or {}) do
      if initial_peer_ids[peer.peer_id] and dht:get_local_peer(peer.peer_id) then goto continue end
      initial_peer_ids[peer.peer_id] = true
      if dht:get_local_peer(peer.peer_id) then report.added = report.added + 1; goto continue end
      if not dht:_peerstore_supports_kad(peer.peer_id) then report.skipped = report.skipped + 1; goto continue end
      local added, add_err = dht:add_peer(peer.peer_id, { allow_replace = options.allow_replace })
      if added then
        report.added = report.added + 1
      elseif add_err and is_capacity_error(add_err) then
        report.skipped = report.skipped + 1
      elseif add_err and not options.ignore_add_errors then
        report.errors[#report.errors + 1] = add_err
      end
      ::continue::
    end
    return report
  end

  local paths = {}
  for i = 1, disjoint_paths do paths[i] = {} end
  local queued = {}
  local queried_peers = {}
  local next_seed_path = 1
  local function enqueue(path_index, peer, referrer_distance)
    if not peer or not peer.peer_id or peer.peer_id == dht.local_peer_id then return end
    if queued[peer.peer_id] or queried_peers[peer.peer_id] then return end
    if peer.addrs ~= nil and type(peer.addrs) == "table" and #dialable_tcp_addrs(dht:_filter_addrs(peer.addrs, { peer_id = peer.peer_id, purpose = "random_walk_enqueue" })) == 0 then return end
    local distance, distance_err = dht:_distance_to_target(peer.peer_id, target_hash)
    if not distance then
      if not options.ignore_add_errors then report.errors[#report.errors + 1] = distance_err end
      return
    end
    if referrer_distance and query.compare_distance(distance, referrer_distance) >= 0 then return end
    peer._distance = distance
    queued[peer.peer_id] = true
    paths[path_index][#paths[path_index] + 1] = peer
  end

  for _, entry in ipairs(initial_peers) do
    enqueue(next_seed_path, entry)
    next_seed_path = (next_seed_path % disjoint_paths) + 1
  end

  local yield = type(options.yield) == "function" and options.yield or nil
  local active = true
  while active do
    active = false
    for path_index, queue_items in ipairs(paths) do
      if #queue_items > 0 then
        active = true
        table.sort(queue_items, function(a, b)
          local cmp = query.compare_distance(a._distance, b._distance)
          if cmp == 0 then return a.peer_id < b.peer_id end
          return cmp < 0
        end)
        local batch = {}
        for _ = 1, math.min(alpha, #queue_items) do batch[#batch + 1] = table.remove(queue_items, 1) end
        for _, entry in ipairs(batch) do
          queued[entry.peer_id] = nil
          if queried_peers[entry.peer_id] then goto continue_queries end
          queried_peers[entry.peer_id] = true
          report.queried = report.queried + 1
          local query_target = entry
          if not entry.addr and not entry.addrs then query_target = entry.peer_id end
          local closest, closest_err = dht:_find_node(query_target, target_key, options.find_node_opts)
          if yield then
            local yield_ok, yield_err = yield()
            if yield_ok == nil and yield_err then return nil, yield_err end
          end
          if not closest then
            report.failed = report.failed + 1
            report.errors[#report.errors + 1] = closest_err
            if options.fail_fast then return nil, closest_err end
            goto continue_queries
          end
          report.responses = report.responses + 1
          report.discovered = report.discovered + #closest
          for _, candidate in ipairs(closest) do
            if candidate.peer_id then
              local added, add_err = dht:add_peer(candidate.peer_id, { allow_replace = options.allow_replace })
              if added then
                report.added = report.added + 1
              elseif add_err and is_capacity_error(add_err) then
                report.skipped = report.skipped + 1
              elseif add_err and not options.ignore_add_errors then
                report.errors[#report.errors + 1] = add_err
              end
              enqueue(path_index, candidate, entry._distance)
            end
          end
          ::continue_queries::
        end
      end
    end
  end
  return report
end

--- Start a scheduler-backed random walk operation.
-- This wraps @{run} in a host task, injects the task `ctx`, and returns the
-- repository-standard operation object for public `DHT:random_walk` callers.
-- @tparam table dht DHT instance
-- @tparam[opt] table opts Walk/query controls.
-- @treturn table|nil op
-- @treturn[opt] table err
function M.spawn(dht, opts)
  if not (dht.host and type(dht.host.spawn_task) == "function") then
    return nil, error_mod.new("state", "kad_dht random_walk requires host task scheduler")
  end
  local options = opts or {}
  local task, task_err = dht.host:spawn_task("kad.random_walk", function(ctx)
    local task_opts = {}
    for k, v in pairs(options) do task_opts[k] = v end
    task_opts.scheduler_task = true
    task_opts.ctx = task_opts.ctx or ctx
    task_opts.yield = task_opts.yield or function() return ctx:checkpoint() end
    task_opts.find_node_opts = task_opts.find_node_opts or {}
    task_opts.find_node_opts.ctx = task_opts.find_node_opts.ctx or ctx
    return dht:_random_walk(task_opts)
  end, { service = "kad_dht" })
  if not task then return nil, task_err end
  return operation.new(dht.host, task)
end

return M
