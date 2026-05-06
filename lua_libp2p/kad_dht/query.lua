--- Shared KAD-DHT iterative query engine.
-- @module lua_libp2p.kad_dht.query
local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log").subsystem("kad_dht")
local protocol = require("lua_libp2p.kad_dht.protocol")

local M = {}

function M.compare_distance(left, right)
  local size = #left
  if #right < size then size = #right end
  for i = 1, size do
    local a = left:byte(i)
    local b = right:byte(i)
    if a < b then return -1 end
    if a > b then return 1 end
  end
  if #left < #right then return -1 end
  if #left > #right then return 1 end
  return 0
end

local function add_error_fields(out, prefix, err, depth)
  local remaining = depth or 3
  out[prefix] = tostring(err)
  if remaining <= 0 or not error_mod.is_error(err) then
    return out
  end
  out[prefix .. "_kind"] = err.kind
  if type(err.context) == "table" then
    for k, v in pairs(err.context) do
      local key = prefix .. "_" .. tostring(k)
      if k == "cause" then
        out[key] = tostring(v)
        if error_mod.is_error(v) then
          add_error_fields(out, key, v, remaining - 1)
        end
      elseif type(v) ~= "table" and type(v) ~= "function" and type(v) ~= "thread" then
        out[key] = v
      end
    end
  end
  return out
end

function M.sort_candidates(dht, target_hash, candidates)
  table.sort(candidates, function(a, b)
    local da = dht:_distance_to_target(a.peer_id or "", target_hash) or string.rep("\255", 32)
    local db = dht:_distance_to_target(b.peer_id or "", target_hash) or string.rep("\255", 32)
    return M.compare_distance(da, db) < 0
  end)
end

function M.strict_complete(dht, target_hash, states, k)
  local peers = {}
  local heard_or_waiting = 0
  for _, state in pairs(states) do
    if state.state ~= "unreachable" then
      peers[#peers + 1] = state
      if state.state == "heard" or state.state == "waiting" then
        heard_or_waiting = heard_or_waiting + 1
      end
    end
  end
  M.sort_candidates(dht, target_hash, peers)
  if #peers == 0 then return heard_or_waiting == 0, "starvation" end
  local limit = math.min(k or dht.k, #peers)
  for i = 1, limit do
    if peers[i].state ~= "queried" then return false end
  end
  if #peers < (k or dht.k) then
    return heard_or_waiting == 0, "closest_available_queried"
  end
  return true, "closest_queried"
end

function M.run_client_lookup(dht, key, seed_peers, query_func, opts)
  local options = opts or {}
  local target_bytes, target_err = protocol.peer_bytes(key)
  if not target_bytes then target_bytes = key end
  local target_hash, hash_err = dht.routing_table:_hash(target_bytes)
  if not target_hash then return nil, hash_err or target_err end

  local states = {}
  local queue = {}
  local query_filter = options.query_filter or dht.query_filter
  local filter_errors = {}
  local function enqueue(peer)
    if type(peer) ~= "table" or type(peer.peer_id) ~= "string" or peer.peer_id == dht.local_peer_id then return end
    if states[peer.peer_id] then return end
    if query_filter ~= nil then
      if type(query_filter) ~= "function" then
        filter_errors[#filter_errors + 1] = error_mod.new("input", "kad-dht query_filter must be a function")
        return
      end
      local ok, allowed_or_err, filter_err = pcall(query_filter, peer, {
        dht = dht,
        target = key,
        opts = options,
      })
      if not ok then
        filter_errors[#filter_errors + 1] = error_mod.new("protocol", "kad-dht query_filter failed", { cause = allowed_or_err })
        return
      end
      if allowed_or_err == nil and filter_err ~= nil then
        filter_errors[#filter_errors + 1] = filter_err
        return
      end
      if allowed_or_err == false then return end
    end
    local addrs, addrs_err = dht:_dialable_tcp_addrs(peer.addrs or (peer.addr and { peer.addr }) or {})
    if not addrs then
      filter_errors[#filter_errors + 1] = addrs_err
      return
    end
    if #addrs == 0 and dht.host and dht.host.peerstore then
      addrs, addrs_err = dht:_dialable_tcp_addrs(dht:_filter_addrs(dht.host.peerstore:get_addrs(peer.peer_id), {
        peer_id = peer.peer_id,
        purpose = "client_query_enqueue",
      }))
      if not addrs then
        filter_errors[#filter_errors + 1] = addrs_err
        return
      end
    end
    local has_connection = dht.host and type(dht.host._find_connection) == "function" and dht.host:_find_connection(peer.peer_id) ~= nil
    if #addrs == 0 and dht.host and dht.host.peerstore and not has_connection then return end
    local entry = { peer_id = peer.peer_id, addrs = addrs, addr = peer.addr or addrs[1], state = "heard" }
    states[peer.peer_id] = entry
    queue[#queue + 1] = entry
  end
  for _, peer in ipairs(seed_peers or {}) do enqueue(peer) end

  local alpha = options.alpha or dht.alpha
  local requested_inflight = alpha * (options.disjoint_paths or dht.disjoint_paths or 1)
  local max_concurrent = options.max_concurrent_queries or dht.max_concurrent_queries or requested_inflight
  local max_inflight = math.min(requested_inflight, max_concurrent)
  local result = {
    queried = 0,
    responses = 0,
    failed = 0,
    cancelled = 0,
    errors = {},
    closest_peers = {},
    queried_peers = {},
    termination = nil,
    active_peak = 0,
    alpha = alpha,
    disjoint_paths = options.disjoint_paths or dht.disjoint_paths or 1,
    requested_concurrency = requested_inflight,
    max_concurrent_queries = max_concurrent,
    effective_concurrency = max_inflight,
  }
  for _, err in ipairs(filter_errors) do result.errors[#result.errors + 1] = err end
  local strict_k = options.lookup_k or dht.k
  local stop = false
  local active = 0
  local yield = type(options.yield) == "function" and options.yield or nil
  log.debug("kad dht lookup started", {
    key_size = type(key) == "string" and #key or nil,
    seed_peers = #(seed_peers or {}),
    queued_peers = #queue,
    alpha = alpha,
    disjoint_paths = result.disjoint_paths,
    effective_concurrency = max_inflight,
    scheduler_task = options.scheduler_task == true,
  })

  local function complete(peer, ok, response_or_err)
    active = active - 1
    if ok then
      peer.state = "queried"
      result.responses = result.responses + 1
      result.queried_peers[#result.queried_peers + 1] = peer
      dht:_record_kad_peer(peer.peer_id, peer.addrs)
      local response = response_or_err or {}
      if response.stop then stop = true end
      log.debug("kad dht peer query succeeded", {
        peer_id = peer.peer_id,
        closer_peers = #(response.closer_peers or {}),
        stop = response.stop == true,
      })
      for _, closer in ipairs(response.closer_peers or {}) do
        result.closest_peers[#result.closest_peers + 1] = closer
        enqueue(closer)
      end
    else
      peer.state = "unreachable"
      result.failed = result.failed + 1
      if response_or_err then result.errors[#result.errors + 1] = response_or_err end
      log.debug("kad dht peer query failed", add_error_fields({
        peer_id = peer.peer_id,
      }, "cause", response_or_err))
    end
  end

  local function spawn(peer)
    peer.state = "waiting"
    active = active + 1
    if active > result.active_peak then result.active_peak = active end
    result.queried = result.queried + 1
    local co = coroutine.create(function()
      local response, err = query_func(peer)
      if not response then return false, err end
      return true, response
    end)
    return { co = co, peer = peer }
  end

  if options.scheduler_task == true and options.ctx and dht.host and type(dht.host.spawn_task) == "function" then
    local tasks = {}
    local function spawn_task(peer)
      peer.state = "waiting"
      active = active + 1
      if active > result.active_peak then result.active_peak = active end
      result.queried = result.queried + 1
      local task, task_err = dht.host:spawn_task("kad.client_query", function(task_ctx)
        local response, err = query_func(peer, task_ctx)
        if not response then return nil, err end
        return response
      end, { service = "kad_dht" })
      if not task then complete(peer, false, task_err); return end
      tasks[#tasks + 1] = { task = task, peer = peer }
    end
    local function fill_tasks()
      M.sort_candidates(dht, target_hash, queue)
      while #queue > 0 and active < max_inflight and not stop do
        local peer = table.remove(queue, 1)
        if peer.state == "heard" then spawn_task(peer) end
      end
    end
    local function reap_tasks()
      for i = #tasks, 1, -1 do
        local item = tasks[i]
        local task = item.task
        if task.status == "completed" then
          complete(item.peer, true, task.result)
          table.remove(tasks, i)
        elseif task.status == "failed" then
          complete(item.peer, false, task.error or task.status)
          table.remove(tasks, i)
        elseif task.status == "cancelled" then
          active = active - 1
          item.peer.state = "unreachable"
          result.cancelled = result.cancelled + 1
          table.remove(tasks, i)
        end
      end
    end
    local function cancel_active_tasks()
      if not (dht.host and type(dht.host.cancel_task) == "function") then return end
      for _, item in ipairs(tasks) do
        local task = item.task
        if task and task.status ~= "completed" and task.status ~= "failed" and task.status ~= "cancelled" then
          dht.host:cancel_task(task.id)
          result.cancelled = result.cancelled + 1
        end
      end
      tasks = {}
      active = 0
    end
    local function running_tasks()
      local out = {}
      for _, item in ipairs(tasks) do
        local task = item.task
        if task and task.status ~= "completed" and task.status ~= "failed" and task.status ~= "cancelled" then out[#out + 1] = task end
      end
      return out
    end
    while true do
      reap_tasks(); fill_tasks(); reap_tasks()
      local done, reason = M.strict_complete(dht, target_hash, states, strict_k)
      if stop then cancel_active_tasks(); result.termination = result.termination or "application"; break end
      if done then cancel_active_tasks(); result.termination = reason; break end
      if active == 0 and #queue == 0 then result.termination = "starvation"; break end
      local waiting = running_tasks()
      local checkpoint_ok, checkpoint_err
      if #waiting > 0 and type(options.ctx.await_any_task) == "function" then
        checkpoint_ok, checkpoint_err = options.ctx:await_any_task(waiting)
      else
        checkpoint_ok, checkpoint_err = options.ctx:checkpoint()
      end
      if checkpoint_ok == nil and checkpoint_err then
        result.termination = "yield_error"
        result.errors[#result.errors + 1] = checkpoint_err
        break
      end
    end
    M.sort_candidates(dht, target_hash, result.closest_peers)
    log.debug("kad dht lookup completed", {
      termination = result.termination,
      queried = result.queried,
      responses = result.responses,
      failed = result.failed,
      cancelled = result.cancelled,
      active_peak = result.active_peak,
      closest_peers = #result.closest_peers,
    })
    return result
  elseif options.scheduler_task == true then
    while not stop do
      M.sort_candidates(dht, target_hash, queue)
      local peer
      while #queue > 0 and not peer do
        local candidate = table.remove(queue, 1)
        if candidate.state == "heard" then peer = candidate end
      end
      if not peer then
        local done, reason = M.strict_complete(dht, target_hash, states, strict_k)
        result.termination = done and reason or "starvation"
        break
      end
      peer.state = "waiting"
      active = active + 1
      if active > result.active_peak then result.active_peak = active end
      result.queried = result.queried + 1
      local response, response_err = query_func(peer)
      complete(peer, response ~= nil, response or response_err)
      if yield then
        local yield_ok, yield_err = yield()
        if yield_ok == nil and yield_err then
          result.termination = "yield_error"
          result.errors[#result.errors + 1] = yield_err
          stop = true
        end
      end
      local done, reason = M.strict_complete(dht, target_hash, states, strict_k)
      if stop then if not result.termination then result.termination = "application" end; break end
      if done then result.termination = reason; break end
    end
    M.sort_candidates(dht, target_hash, result.closest_peers)
    log.debug("kad dht lookup completed", {
      termination = result.termination,
      queried = result.queried,
      responses = result.responses,
      failed = result.failed,
      cancelled = result.cancelled,
      active_peak = result.active_peak,
      closest_peers = #result.closest_peers,
    })
    return result
  end

  local workers = {}
  local function step_lookup()
    M.sort_candidates(dht, target_hash, queue)
    while #queue > 0 and active < max_inflight and not stop do
      local peer = table.remove(queue, 1)
      if peer.state == "heard" then workers[#workers + 1] = spawn(peer) end
    end
    for i = #workers, 1, -1 do
      local worker = workers[i]
      local ok, success, response_or_err = coroutine.resume(worker.co)
      if not ok then
        success = false
        response_or_err = error_mod.new("protocol", "kad client query coroutine failed", { cause = success })
      end
      if coroutine.status(worker.co) == "dead" then
        complete(worker.peer, success == true, response_or_err)
        table.remove(workers, i)
      end
    end
    if yield then
      local yield_ok, yield_err = yield()
      if yield_ok == nil and yield_err then
        result.termination = "yield_error"
        result.errors[#result.errors + 1] = yield_err
        stop = true
        return true
      end
    end
    local done, reason = M.strict_complete(dht, target_hash, states, strict_k)
    if stop then result.termination = "application"; return true end
    if done then result.termination = reason; return true end
    if active == 0 and #queue == 0 then result.termination = "starvation"; return true end
    return false
  end
  while not step_lookup() do
    if #workers == 0 and #queue == 0 then break end
  end
  log.debug("kad dht lookup completed", {
    termination = result.termination,
    queried = result.queried,
    responses = result.responses,
    failed = result.failed,
    cancelled = result.cancelled,
    active_peak = result.active_peak,
    closest_peers = #result.closest_peers,
  })
  return result
end

return M
