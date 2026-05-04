--- KAD-DHT provider re-announcement helpers.
-- @module lua_libp2p.kad_dht.reprovider
local error_mod = require("lua_libp2p.error")

local M = {}

M.DEFAULT_ENABLED = false
M.DEFAULT_INTERVAL_SECONDS = 12 * 60 * 60
M.DEFAULT_INITIAL_DELAY_SECONDS = nil
M.DEFAULT_JITTER_SECONDS = 0
M.DEFAULT_BATCH_SIZE = 100
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

  local report = {
    keys = {},
    attempted = 0,
    succeeded = 0,
    failed = 0,
    results = {},
    errors = {},
    items = {},
  }

  for _, key in ipairs(keys) do
    if type(key) ~= "string" or key == "" then
      return nil, error_mod.new("input", "reprovide key must be non-empty bytes")
    end
    report.keys[#report.keys + 1] = key
    report.attempted = report.attempted + 1

    local provide_opts = {}
    for k, v in pairs(options) do
      if k ~= "keys" and k ~= "batch_size" and k ~= "limit" then
        provide_opts[k] = v
      end
    end

    local result, err = dht:_provide(key, provide_opts)
    if result then
      report.succeeded = report.succeeded + 1
      report.results[#report.results + 1] = result
      local item = {
        key = key,
        ok = true,
        result = result,
      }
      report.items[#report.items + 1] = item
      emit_event(dht.host, "kad_dht:reprovide:item", item)
    else
      report.failed = report.failed + 1
      report.errors[#report.errors + 1] = err
      local item = {
        key = key,
        ok = false,
        error = err,
      }
      report.items[#report.items + 1] = item
      emit_event(dht.host, "kad_dht:reprovide:item", item)
      if options.fail_fast then
        return nil, err
      end
    end
  end

  return report
end

--- Start background reprovider task if enabled.
function M.start(dht)
  if not (dht._reprovider_enabled and dht.host and type(dht.host.spawn_task) == "function") then
    return true
  end

  local task, task_err = dht.host:spawn_task("kad.reprovider", function(ctx)
    local next_delay = dht._reprovider_initial_delay_seconds or dht._reprovider_interval_seconds
    while dht._running do
      local sleep_ok, sleep_err = ctx:sleep(scheduled_delay(dht, next_delay))
      if sleep_ok == nil and sleep_err then
        return nil, sleep_err
      end
      next_delay = dht._reprovider_interval_seconds
      if not dht._running then
        break
      end

      emit_event(dht.host, "kad_dht:reprovide:start", {
        batch_size = dht._reprovider_batch_size,
      })
      local report, err = dht:_reprovide({
        batch_size = dht._reprovider_batch_size,
        timeout = dht._reprovider_timeout,
        ctx = ctx,
        scheduler_task = true,
        yield = function()
          return ctx:checkpoint()
        end,
      })
      if report then
        emit_event(dht.host, "kad_dht:reprovide:complete", report)
      else
        emit_event(dht.host, "kad_dht:reprovide:error", err)
      end
    end
    return true
  end, { service = "kad_dht" })
  if not task then
    return nil, task_err
  end
  dht._reprovider_task = task
  return true
end

function M.stop(dht)
  if dht._reprovider_task and dht.host and type(dht.host.cancel_task) == "function" then
    dht.host:cancel_task(dht._reprovider_task.id)
  end
  dht._reprovider_task = nil
  return true
end

return M
