--- KAD-DHT provider re-announcement helpers.
-- @module lua_libp2p.kad_dht.reprovider
local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log").subsystem("kad_dht")

local M = {}

M.DEFAULT_ENABLED = false
-- IPFS KAD-DHT providers republish every 22h, before the 48h provider record
-- validity expires, to tolerate churn without letting records go stale.
M.DEFAULT_INTERVAL_SECONDS = 22 * 60 * 60
M.DEFAULT_INITIAL_DELAY_SECONDS = nil
M.DEFAULT_JITTER_SECONDS = 0
M.DEFAULT_BATCH_SIZE = 100
M.DEFAULT_MAX_PARALLEL = 1
M.DEFAULT_TIMEOUT = 60

local function emit_event(host, name, payload)
  if host and type(host.emit) == "function" then
    return host:emit(name, payload)
  end
  return true
end

local function scheduled_delay(dht, base_seconds)
  local base = base_seconds or 0
  local jitter = dht._reprovider_jitter_seconds or 0
  if jitter <= 0 then
    return base
  end
  local random = dht._reprovider_random or math.random
  local fraction = random()
  if type(fraction) ~= "number" then
    fraction = 0
  end
  if fraction < 0 then
    fraction = 0
  elseif fraction > 1 then
    fraction = 1
  end
  return base + (fraction * jitter)
end

--- Re-announce local provider records.
-- `dht` must provide `list_local_provider_keys` and `_provide`.
function M.reprovide(dht, opts)
  local options = opts or {}
  local max_parallel = tonumber(options.max_parallel or dht._reprovider_max_parallel or M.DEFAULT_MAX_PARALLEL) or 1
  if max_parallel < 1 then
    return nil, error_mod.new("input", "reprovider_max_parallel must be >= 1")
  end
  local keys = options.keys
  if keys == nil then
    local listed, list_err = dht:list_local_provider_keys({
      limit = options.batch_size or options.limit,
    })
    if not listed then
      return nil, list_err
    end
    keys = listed
  elseif type(keys) ~= "table" then
    return nil, error_mod.new("input", "reprovide keys must be a list")
  end
  log.debug("kad dht reprovide started", {
    keys = #keys,
    max_parallel = max_parallel,
    batch_size = options.batch_size or options.limit,
  })

  local report = {
    keys = {},
    attempted = 0,
    succeeded = 0,
    failed = 0,
    results = {},
    errors = {},
    items = {},
  }

  local function record_result(key, result, err)
    if result then
      report.succeeded = report.succeeded + 1
      report.results[#report.results + 1] = result
      local item = { key = key, ok = true, result = result }
      report.items[#report.items + 1] = item
      emit_event(dht.host, "kad_dht:reprovide:item", item)
      log.debug("kad dht reprovide item succeeded", {
        key_size = type(key) == "string" and #key or nil,
      })
      return true
    end
    report.failed = report.failed + 1
    report.errors[#report.errors + 1] = err
    local item = { key = key, ok = false, error = err }
    report.items[#report.items + 1] = item
    emit_event(dht.host, "kad_dht:reprovide:item", item)
    log.debug("kad dht reprovide item failed", {
      key_size = type(key) == "string" and #key or nil,
      cause = tostring(err),
    })
    if options.fail_fast then
      return nil, err
    end
    return true
  end

  local function provide_key(key)
    local provide_opts = {}
    for k, v in pairs(options) do
      if k ~= "keys" and k ~= "batch_size" and k ~= "limit" and k ~= "max_parallel" then
        provide_opts[k] = v
      end
    end
    return dht:_provide(key, provide_opts)
  end

  for _, key in ipairs(keys) do
    if type(key) ~= "string" or key == "" then
      return nil, error_mod.new("input", "reprovide key must be non-empty bytes")
    end
    report.keys[#report.keys + 1] = key
    report.attempted = report.attempted + 1
  end

  if max_parallel > 1 and dht.host and type(dht.host.spawn_task) == "function" and options.ctx and type(options.ctx.await_any_task) == "function" then
    local running = {}
    local index = 1
    local function reap_done()
      local kept = {}
      for _, item in ipairs(running) do
        local task = item.task
        if task.status == "completed" or task.status == "failed" or task.status == "cancelled" then
          local ok, err = record_result(item.key, task.result, task.error)
          if not ok then return nil, err end
        else
          kept[#kept + 1] = item
        end
      end
      running = kept
      return true
    end
    while index <= #keys or #running > 0 do
      while index <= #keys and #running < max_parallel do
        local key = keys[index]
        index = index + 1
        local task, task_err = dht.host:spawn_task("kad.reprovide.item", function()
          return provide_key(key)
        end, { service = "kad_dht" })
        if not task then return nil, task_err end
        running[#running + 1] = { key = key, task = task }
      end
      local reaped, reap_err = reap_done()
      if not reaped then return nil, reap_err end
      if #running > 0 then
        local waited, wait_err = options.ctx:await_any_task((function()
          local tasks = {}
          for _, item in ipairs(running) do tasks[#tasks + 1] = item.task end
          return tasks
        end)())
        if waited == nil and wait_err then return nil, wait_err end
      end
    end
    log.debug("kad dht reprovide completed", {
      attempted = report.attempted,
      succeeded = report.succeeded,
      failed = report.failed,
    })
    return report
  end

  for _, key in ipairs(keys) do
    local result, err = provide_key(key)
    local ok, record_err = record_result(key, result, err)
    if not ok then return nil, record_err end
  end

  log.debug("kad dht reprovide completed", {
    attempted = report.attempted,
    succeeded = report.succeeded,
    failed = report.failed,
  })
  return report
end

--- Start background reprovider task if enabled.
function M.start(dht)
  if not (dht._reprovider_enabled and dht.host and type(dht.host.spawn_task) == "function") then
    return true
  end

  local task, task_err = dht.host:spawn_task("kad.reprovider", function(ctx)
    local next_delay = dht._reprovider_initial_delay_seconds or dht._reprovider_interval_seconds
    log.debug("kad dht reprovider task started", {
      initial_delay_seconds = next_delay,
      interval_seconds = dht._reprovider_interval_seconds,
      batch_size = dht._reprovider_batch_size,
      max_parallel = dht._reprovider_max_parallel,
    })
    while dht._running do
      local delay = scheduled_delay(dht, next_delay)
      log.debug("kad dht reprovider sleeping", {
        delay_seconds = delay,
      })
      local sleep_ok, sleep_err = ctx:sleep(delay)
      if sleep_ok == nil and sleep_err then
        return nil, sleep_err
      end
      next_delay = dht._reprovider_interval_seconds
      if not dht._running then
        break
      end

      emit_event(dht.host, "kad_dht:reprovide:start", {
        batch_size = dht._reprovider_batch_size,
        max_parallel = dht._reprovider_max_parallel,
      })
      local report, err = dht:_reprovide({
        batch_size = dht._reprovider_batch_size,
        timeout = dht._reprovider_timeout,
        max_parallel = dht._reprovider_max_parallel,
        ctx = ctx,
        scheduler_task = true,
        yield = function()
          return ctx:checkpoint()
        end,
      })
      if report then
        emit_event(dht.host, "kad_dht:reprovide:complete", report)
        log.debug("kad dht reprovider cycle completed", {
          attempted = report.attempted,
          succeeded = report.succeeded,
          failed = report.failed,
        })
      else
        emit_event(dht.host, "kad_dht:reprovide:error", err)
        log.debug("kad dht reprovider cycle failed", {
          cause = tostring(err),
        })
      end
    end
    return true
  end, { service = "kad_dht" })
  if not task then
    return nil, task_err
  end
  dht._reprovider_task = task
  log.debug("kad dht reprovider task scheduled", {
    task_id = task.id,
  })
  return true
end

function M.stop(dht)
  if dht._reprovider_task and dht.host and type(dht.host.cancel_task) == "function" then
    log.debug("kad dht reprovider task cancelling", {
      task_id = dht._reprovider_task.id,
    })
    dht.host:cancel_task(dht._reprovider_task.id)
  end
  dht._reprovider_task = nil
  return true
end

return M
