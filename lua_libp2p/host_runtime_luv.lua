local error_mod = require("lua_libp2p.error")
local host_runtime_poll = require("lua_libp2p.host_runtime_poll")

local ok_luv, uv = pcall(require, "luv")

local M = {}

local function map_size(map)
  local n = 0
  for _ in pairs(map or {}) do
    n = n + 1
  end
  return n
end

local function socket_fd(sock)
  if not sock or type(sock.getfd) ~= "function" then
    return nil
  end
  local ok, fd = pcall(sock.getfd, sock)
  if not ok then
    return nil
  end
  if type(fd) ~= "number" or fd < 0 then
    return nil
  end
  return fd
end

local function pump_waiting_connections(host)
  if not (host._tcp_transport and host._tcp_transport.BACKEND == "luv-native") then
    return true
  end
  local ready = {}
  local has_ready = false
  for _, entry in ipairs(host._connections) do
    if entry.conn and type(entry.conn.has_waiters) == "function" and entry.conn:has_waiters() then
      ready[entry] = true
      has_ready = true
    end
  end
  if not has_ready then
    return true
  end
  return host:_poll_once_with_ready_map(0, ready)
end

function M.is_available()
  return ok_luv
end

function M.start_scheduler(repeat_ms, on_tick)
  if not ok_luv then
    return nil, "luv not available"
  end
  if type(on_tick) ~= "function" then
    return nil, "on_tick callback is required"
  end

  local timer, timer_err = uv.new_timer()
  if not timer then
    return nil, timer_err
  end

  local ok, start_err = timer:start(0, repeat_ms, on_tick)
  if not ok then
    pcall(function()
      timer:close()
    end)
    return nil, start_err
  end

  return timer
end

function M.stop_scheduler(timer)
  if timer == nil then
    return true
  end
  pcall(function()
    timer:stop()
    timer:close()
  end)
  return true
end

function M.start(host)
  if not ok_luv then
    return nil, error_mod.new("unsupported", "runtime=luv requires 'luv' module")
  end
  if type(host) ~= "table" then
    return nil, error_mod.new("input", "host table is required")
  end

  if host._luv_tick_timer == nil then
    local sync_ok, sync_err = M.sync_watchers(host)
    if not sync_ok then
      return nil, sync_err
    end

    local poll_interval = host._start_poll_interval or 0.01
    local repeat_ms = math.max(1, math.floor(poll_interval * 1000))
    local timer, timer_err = M.start_scheduler(repeat_ms, function()
      M.tick(host)
    end)
    if not timer then
      return nil, error_mod.new("io", "failed to start luv scheduler", { cause = timer_err })
    end
    host._luv_tick_timer = timer
  end

  if host._start_blocking then
    M.run("default")
  end

  return true
end

function M.stop(host)
  if type(host) ~= "table" then
    return true
  end
  M.stop_scheduler(host._luv_tick_timer)
  host._luv_tick_timer = nil
  M.close_watchers(host)
  return true
end

function M.tick(host)
  if type(host) ~= "table" or not host._running then
    return true
  end

  local ok, err = M.sync_watchers(host)
  if not ok then
    host:_set_runtime_error("luv", err)
    return nil, err
  end

  if #host._pending_inbound > 0 then
    ok, err = host:poll_once(0)
    if not ok then
      host:_set_runtime_error("luv", err)
      return nil, err
    end
  end

  if map_size(host._luv_ready) > 0 then
    ok, err = host:poll_once(0)
    if not ok then
      host:_set_runtime_error("luv", err)
      return nil, err
    end
  end

  ok, err = pump_waiting_connections(host)
  if not ok then
    host:_set_runtime_error("luv", err)
    return nil, err
  end

  if #host._handler_tasks > 0 then
    local task_ok, task_err = host:_run_handler_tasks()
    if not task_ok then
      host:_set_runtime_error("luv", task_err)
      return nil, task_err
    end
    ok, err = pump_waiting_connections(host)
    if not ok then
      host:_set_runtime_error("luv", err)
      return nil, err
    end
  end

  if map_size(host._luv_ready) > 0 then
    ok, err = host:poll_once(0)
    if not ok then
      host:_set_runtime_error("luv", err)
      return nil, err
    end
  end

  return true
end

function M.run(mode)
  if not ok_luv then
    return nil, "luv not available"
  end
  return uv.run(mode)
end

function M.close_watchers(host)
  for target, watcher in pairs(host._luv_watchers) do
    if watcher and watcher.custom and type(watcher.unwatch) == "function" then
      pcall(watcher.unwatch)
    elseif watcher and watcher.handle then
      pcall(function()
        watcher.handle:stop()
        watcher.handle:close()
      end)
    end
    host._luv_watchers[target] = nil
  end
  host._luv_ready = {}
end

function M.sync_watchers(host)
  if not ok_luv then
    return nil, error_mod.new("unsupported", "runtime=luv requires 'luv' module")
  end

  local active = {}
  for _, listener in ipairs(host._listeners) do
    active[listener] = {
      kind = "listener",
      watchable = listener,
      socket = host_runtime_poll.unwrap_socket(listener),
    }
  end
  for _, entry in ipairs(host._connections) do
    active[entry] = {
      kind = "connection",
      watchable = entry.conn,
      socket = host_runtime_poll.unwrap_socket(entry.conn),
    }
  end
  for _, pending in ipairs(host._pending_inbound or {}) do
    local raw_conn = pending
    if type(pending) == "table" and pending.raw_conn ~= nil then
      raw_conn = pending.raw_conn
    end
    active[pending] = {
      kind = "pending_inbound",
      watchable = raw_conn,
      socket = host_runtime_poll.unwrap_socket(raw_conn),
    }
  end

  for target, watcher in pairs(host._luv_watchers) do
    if not active[target] then
      if watcher and watcher.custom and type(watcher.unwatch) == "function" then
        pcall(watcher.unwatch)
      elseif watcher and watcher.handle then
        pcall(function()
          watcher.handle:stop()
          watcher.handle:close()
        end)
      end
      host._luv_watchers[target] = nil
      host._luv_ready[target] = nil
    end
  end

  for target, item in pairs(active) do
    if host._luv_watchers[target] == nil then
      if item.watchable and type(item.watchable.watch_luv_readable) == "function" then
        local ok, unwatch_or_err = pcall(item.watchable.watch_luv_readable, item.watchable, function()
          host._luv_ready[target] = true
        end)
        if not ok then
          return nil, error_mod.new("io", "failed to register luv readable watcher", {
            kind = item.kind,
            cause = unwatch_or_err,
          })
        end
        if type(unwatch_or_err) ~= "function" then
          goto continue_targets
        end

        host._luv_watchers[target] = {
          custom = true,
          kind = item.kind,
          unwatch = unwatch_or_err,
        }
      else
        local fd = socket_fd(item.socket)
        if fd == nil then
          goto continue_targets
        end

        local poll_handle, poll_err = uv.new_poll(fd)
        if not poll_handle then
          return nil, error_mod.new("io", "failed to create luv poll handle", {
            kind = item.kind,
            cause = poll_err,
          })
        end

        local ok, start_err = poll_handle:start("r", function(err)
          if err then
            host:_set_runtime_error("luv", error_mod.new("io", "luv poll callback error", {
              kind = item.kind,
              cause = err,
            }))
            return
          end
          host._luv_ready[target] = true
        end)
        if not ok then
          pcall(function()
            poll_handle:close()
          end)
          return nil, error_mod.new("io", "failed to start luv poll handle", {
            kind = item.kind,
            cause = start_err,
          })
        end

        host._luv_watchers[target] = {
          handle = poll_handle,
          kind = item.kind,
        }
      end

      ::continue_targets::
    end
  end

  return true
end

return M
