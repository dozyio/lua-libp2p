--- KAD-DHT maintenance loop and peer refresh helpers.
-- @module lua_libp2p.kad_dht.maintenance
local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log").subsystem("kad_dht")

local M = {}

M.DEFAULT_ENABLED = true
M.DEFAULT_INTERVAL_SECONDS = 10 * 60
M.DEFAULT_STARTUP_RETRY_SECONDS = 5
M.DEFAULT_STARTUP_RETRY_MAX_SECONDS = 30
M.DEFAULT_MIN_RECHECK_SECONDS = 10 * 60
M.DEFAULT_MAX_CHECKS = 10
M.DEFAULT_WALK_EVERY = 1
-- Go parity: after the grace window, a single failed liveness/protocol check
-- evicts the peer from the routing table.
M.DEFAULT_MAX_FAILED_CHECKS_BEFORE_EVICT = 1
M.DEFAULT_WALK_TIMEOUT = 20
M.DEFAULT_PROTOCOL_CHECK_TIMEOUT = 10

local function emit_event(host, name, payload)
  if host and type(host.emit) == "function" then
    return host:emit(name, payload)
  end
  return true
end

local function has_routing_peers(dht)
  return dht.routing_table and #(dht.routing_table:all_peers() or {}) > 0
end

local function run_cycle(dht, ctx, tick)
  local had_routing_peers = has_routing_peers(dht)
  local refresh_report, refresh_err = dht:refresh_once({
    min_recheck_seconds = dht._maintenance_min_recheck_seconds,
    max_checks = dht._maintenance_max_checks,
    protocol_check_opts = {
      timeout = dht._maintenance_protocol_check_timeout,
      stream_opts = { ctx = ctx },
    },
  })
  if not refresh_report and refresh_err then
    log.debug("kad dht maintenance refresh failed", {
      cause = tostring(refresh_err),
    })
  else
    emit_event(dht.host, "kad_dht:maintenance:refresh", refresh_report)
  end

  local walk_report = nil
  if dht._maintenance_walk_every > 0 and (tick % dht._maintenance_walk_every) == 0 and had_routing_peers then
    local walk_op = dht:random_walk({
      alpha = dht.alpha,
      disjoint_paths = dht.disjoint_paths,
      find_node_opts = { ctx = ctx },
    })
    if walk_op and type(walk_op.result) == "function" then
      walk_report = walk_op:result({ ctx = ctx, timeout = dht._maintenance_walk_timeout })
      emit_event(dht.host, "kad_dht:maintenance:walk", walk_report)
    end
  end

  return true, nil, {
    had_routing_peers = had_routing_peers,
    walk_report = walk_report,
  }
end

local function cycle_established(cycle)
  if not cycle or not cycle.had_routing_peers then
    return false
  end
  local report = cycle.walk_report
  if not report then
    return false
  end
  return (tonumber(report.responses) or 0) > 0
end

local function run_guarded_cycle(dht, ctx, tick)
  if dht._maintenance_running then
    return true, nil, { skipped = "running" }
  end

  dht._maintenance_running = true
  local ok, err, cycle = run_cycle(dht, ctx, tick)
  dht._maintenance_running = false
  if not ok then
    return ok, err, cycle
  end
  if not dht._maintenance_established and cycle_established(cycle) then
    dht._maintenance_established = true
    dht._maintenance_startup_retry_current_seconds = dht._maintenance_startup_retry_seconds
  end
  return ok, err, cycle
end

local function next_sleep_seconds(dht)
  if dht._maintenance_established then
    return dht._maintenance_interval_seconds
  end

  local current = dht._maintenance_startup_retry_current_seconds or dht._maintenance_startup_retry_seconds
  local next_retry = current * 2
  if next_retry > dht._maintenance_startup_retry_max_seconds then
    next_retry = dht._maintenance_startup_retry_max_seconds
  end
  dht._maintenance_startup_retry_current_seconds = next_retry
  return current
end

function M.start(dht)
  if not (dht._maintenance_enabled and dht.host and type(dht.host.spawn_task) == "function") then
    return true
  end

  local task, task_err = dht.host:spawn_task("kad.maintenance", function(ctx)
    local tick = 0
    while dht._running do
      tick = tick + 1
      run_guarded_cycle(dht, ctx, tick)

      local sleep_seconds = next_sleep_seconds(dht)
      local sleep_ok, sleep_err = ctx:sleep(sleep_seconds)
      if sleep_ok == nil and sleep_err then
        return nil, sleep_err
      end
    end
    return true
  end, { service = "kad_dht" })
  if not task then
    return nil, task_err
  end
  dht._maintenance_task = task
  return true
end

function M.trigger(dht)
  if not (dht._maintenance_enabled and dht._running and dht.host and type(dht.host.spawn_task) == "function") then
    return true
  end
  if dht._maintenance_running then
    return true
  end
  if dht._maintenance_trigger_task then
    return true
  end

  local task, task_err = dht.host:spawn_task("kad.maintenance.trigger", function(ctx)
    local tick = dht._maintenance_walk_every > 0 and dht._maintenance_walk_every or 1
    local ok, err = run_guarded_cycle(dht, ctx, tick)
    dht._maintenance_trigger_task = nil
    return ok, err
  end, { service = "kad_dht" })
  if not task then
    return nil, task_err
  end
  dht._maintenance_trigger_task = task
  return true
end

function M.stop(dht)
  if dht._maintenance_task and dht.host and type(dht.host.cancel_task) == "function" then
    dht.host:cancel_task(dht._maintenance_task.id)
  end
  dht._maintenance_task = nil
  dht._maintenance_running = false
  if dht._maintenance_trigger_task and dht.host and type(dht.host.cancel_task) == "function" then
    dht.host:cancel_task(dht._maintenance_trigger_task.id)
  end
  dht._maintenance_trigger_task = nil
  return true
end

function M.refresh_once(dht, opts)
  local options = opts or {}
  local now = os.time()
  local min_recheck = options.min_recheck_seconds or M.DEFAULT_MIN_RECHECK_SECONDS
  local max_checks = options.max_checks or dht.alpha
  local max_failed_checks_before_evict = options.max_failed_checks_before_evict or dht._max_failed_checks_before_evict
  if type(max_checks) ~= "number" or max_checks <= 0 then
    return nil, error_mod.new("input", "max_checks must be > 0")
  end
  if type(max_failed_checks_before_evict) ~= "number" or max_failed_checks_before_evict <= 0 then
    return nil, error_mod.new("input", "max_failed_checks_before_evict must be > 0")
  end

  local peers = dht.routing_table:all_peers()
  local report = { checked = 0, healthy = 0, removed = 0, skipped = 0, errors = {} }
  for _, entry in ipairs(peers) do
    if report.checked >= max_checks then break end
    local peer_id = entry.peer_id
    local health = dht._peer_health[peer_id] or {}
    local failed_checks = tonumber(health.failed_checks) or (health.stale == true and 1 or 0)
    local stale = failed_checks > 0
    local last_verified = health.last_verified_at or 0
    local recent = (now - last_verified) < min_recheck
    if not stale and recent then
      report.skipped = report.skipped + 1
      goto continue_peers
    end

    report.checked = report.checked + 1
    local ok, check_err = dht:_supports_kad_protocol(peer_id, options.protocol_check_opts)
    if ok then
      health.failed_checks = 0
      health.stale = false
      health.last_verified_at = now
      dht._peer_health[peer_id] = health
      report.healthy = report.healthy + 1
    else
      report.errors[#report.errors + 1] = check_err
      failed_checks = failed_checks + 1
      if failed_checks >= max_failed_checks_before_evict then
        local removed, remove_err = dht:remove_peer(peer_id)
        if removed then
          report.removed = report.removed + 1
        elseif remove_err then
          report.errors[#report.errors + 1] = remove_err
        end
      else
        health.failed_checks = failed_checks
        health.stale = true
        health.last_disconnected_at = health.last_disconnected_at or now
        dht._peer_health[peer_id] = health
      end
    end

    ::continue_peers::
  end
  return report
end

return M
