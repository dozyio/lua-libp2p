--- KAD-DHT maintenance loop and peer refresh helpers.
-- @module lua_libp2p.kad_dht.maintenance
local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log").subsystem("kad_dht")

local M = {}

M.DEFAULT_ENABLED = false
M.DEFAULT_INTERVAL_SECONDS = 30
M.DEFAULT_MIN_RECHECK_SECONDS = 60
M.DEFAULT_MAX_CHECKS = 10
M.DEFAULT_WALK_EVERY = 4
M.DEFAULT_MAX_FAILED_CHECKS_BEFORE_EVICT = 2
M.DEFAULT_WALK_TIMEOUT = 20
M.DEFAULT_PROTOCOL_CHECK_TIMEOUT = 4

local function emit_event(host, name, payload)
  if host and type(host.emit) == "function" then
    return host:emit(name, payload)
  end
  return true
end

function M.start(dht)
  if not (dht._maintenance_enabled and dht.host and type(dht.host.spawn_task) == "function") then
    return true
  end

  local task, task_err = dht.host:spawn_task("kad.maintenance", function(ctx)
    local tick = 0
    while dht._running do
      tick = tick + 1
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

      if dht._maintenance_walk_every > 0 and (tick % dht._maintenance_walk_every) == 0 then
        local walk_op = dht:random_walk({
          alpha = dht.alpha,
          disjoint_paths = dht.disjoint_paths,
          find_node_opts = { ctx = ctx },
        })
        if walk_op and type(walk_op.result) == "function" then
          local walk_report = walk_op:result({ ctx = ctx, timeout = dht._maintenance_walk_timeout })
          emit_event(dht.host, "kad_dht:maintenance:walk", walk_report)
        end
      end

      local sleep_ok, sleep_err = ctx:sleep(dht._maintenance_interval_seconds)
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

function M.stop(dht)
  if dht._maintenance_task and dht.host and type(dht.host.cancel_task) == "function" then
    dht.host:cancel_task(dht._maintenance_task.id)
  end
  dht._maintenance_task = nil
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
