--- Host runtime adapter for luv loop management.
-- @module lua_libp2p.host.runtime_luv
local error_mod = require("lua_libp2p.error")

local ok_luv, uv = pcall(require, "luv")

local M = {}

local function now_seconds()
  local ok_socket, socket = pcall(require, "socket")
  if ok_socket and type(socket.gettime) == "function" then
    return socket.gettime()
  end
  return os.time()
end

local function sleep_seconds(seconds)
  if seconds <= 0 then
    return
  end
  local ok_socket, socket = pcall(require, "socket")
  if ok_socket and type(socket.sleep) == "function" then
    socket.sleep(seconds)
  end
end

local function debug_perf_add(host, key, elapsed_seconds)
  local perf = type(host) == "table" and rawget(host, "_debug_perf") or nil
  if type(perf) ~= "table" then
    return
  end
  perf[key .. "_calls"] = (perf[key .. "_calls"] or 0) + 1
  perf[key .. "_ms"] = (perf[key .. "_ms"] or 0) + (elapsed_seconds * 1000)
end

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

local function unwrap_socket(value, seen)
  if type(value) ~= "table" then
    return nil
  end
  local visited = seen or {}
  if visited[value] then
    return nil
  end
  visited[value] = true

  if type(value.socket) == "function" then
    local ok, sock = pcall(value.socket, value)
    if ok and sock then
      return sock
    end
  end

  if value._socket then
    return value._socket
  end
  if value._server then
    return value._server
  end

  if value._raw then
    local sock = unwrap_socket(value._raw, visited)
    if sock then
      return sock
    end
  end
  if value._raw_conn then
    local sock = unwrap_socket(value._raw_conn, visited)
    if sock then
      return sock
    end
  end

  return nil
end

M.unwrap_socket = unwrap_socket

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
  return host:_process_runtime_events(0, ready)
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
    ok, err = M.poll_once(host, 0)
    if not ok then
      host:_set_runtime_error("luv", err)
      return nil, err
    end
  end

  if map_size(host._luv_ready) > 0 then
    ok, err = M.poll_once(host, 0)
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

  if type(host._run_background_tasks) == "function" then
    ok, err = host:_run_background_tasks()
    if not ok then
      host:_set_runtime_error("luv", err)
      return nil, err
    end
  end

  if map_size(host._luv_ready) > 0 then
    ok, err = M.poll_once(host, 0)
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

function M.poll_once(host, timeout)
  if not ok_luv then
    return nil, error_mod.new("unsupported", "runtime=luv requires 'luv' module")
  end
  if type(host) ~= "table" then
    return nil, error_mod.new("input", "host table is required")
  end

  local perf = rawget(host, "_debug_perf")
  local phase_started = perf and now_seconds() or nil
  local sync_ok, sync_err = M.sync_watchers(host)
  if phase_started then
    debug_perf_add(host, "poll_sync_watchers", now_seconds() - phase_started)
    phase_started = now_seconds()
  end
  if not sync_ok then
    return nil, sync_err
  end

  local wait_timeout = type(timeout) == "number" and math.max(0, timeout) or 0
  local deadline = now_seconds() + wait_timeout
  repeat
    local ok, err = pcall(function()
      return uv.run("nowait")
    end)
    if not ok then
      if string.find(tostring(err or ""), "loop already running", 1, true) == nil then
        return nil, error_mod.new("io", "luv poll failed", { cause = err })
      end
      break
    end
    if next(host._luv_ready) ~= nil or wait_timeout <= 0 or now_seconds() >= deadline then
      break
    end
    sleep_seconds(math.min(0.001, math.max(0, deadline - now_seconds())))
  until false
  if phase_started then
    debug_perf_add(host, "poll_uv_wait", now_seconds() - phase_started)
    phase_started = now_seconds()
  end

  local ready_map = host._luv_ready
  host._luv_ready = {}
  local ok, err = host:_process_runtime_events(timeout, ready_map)
  if phase_started then
    debug_perf_add(host, "poll_process_events", now_seconds() - phase_started)
  end
  return ok, err
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
      socket = unwrap_socket(listener),
      events = "r",
    }
  end
  for _, entry in ipairs(host._connections) do
    active[entry] = {
      kind = "connection",
      watchable = entry.conn,
      socket = unwrap_socket(entry.conn),
      events = "r",
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
      socket = unwrap_socket(raw_conn),
      events = "r",
    }
  end
  for connection in pairs(host._task_read_waiters or {}) do
    active[connection] = {
      kind = "task_read",
      watchable = connection,
      socket = unwrap_socket(connection),
      events = "r",
    }
  end
  for connection in pairs(host._task_write_waiters or {}) do
    active[connection] = {
      kind = "task_write",
      watchable = connection,
      socket = unwrap_socket(connection),
      events = "w",
    }
  end
  for connection in pairs(host._task_dial_waiters or {}) do
    active[connection] = {
      kind = "task_dial",
      watchable = connection,
      socket = unwrap_socket(connection),
      events = "w",
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
      local watch_method = nil
      if item.events == "w" and item.watchable and type(item.watchable.watch_luv_write) == "function" then
        watch_method = item.watchable.watch_luv_write
      elseif item.watchable and type(item.watchable.watch_luv_readable) == "function" then
        watch_method = item.watchable.watch_luv_readable
      end
      if watch_method then
        local ok, unwatch_or_err = pcall(watch_method, item.watchable, function()
          host._luv_ready[target] = true
          if type(host._wake_task_readers) == "function" then
            host:_wake_task_readers(item.watchable)
            host:_wake_task_readers(target)
          end
          if type(host._wake_task_writers) == "function" then
            host:_wake_task_writers(item.watchable)
            host:_wake_task_writers(target)
          end
        end)
        if not ok then
          return nil,
            error_mod.new("io", "failed to register luv readable watcher", {
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
          return nil,
            error_mod.new("io", "failed to create luv poll handle", {
              kind = item.kind,
              cause = poll_err,
            })
        end

        local ok, start_err = poll_handle:start(item.events or "r", function(err)
          if err then
            host:_set_runtime_error(
              "luv",
              error_mod.new("io", "luv poll callback error", {
                kind = item.kind,
                cause = err,
              })
            )
            return
          end
          host._luv_ready[target] = true
          if type(host._wake_task_readers) == "function" then
            host:_wake_task_readers(item.watchable)
            host:_wake_task_readers(target)
          end
          if type(host._wake_task_writers) == "function" then
            host:_wake_task_writers(item.watchable)
            host:_wake_task_writers(target)
          end
          if type(host._wake_task_dialers) == "function" then
            host:_wake_task_dialers(item.watchable)
            host:_wake_task_dialers(target)
          end
        end)
        if not ok then
          pcall(function()
            poll_handle:close()
          end)
          return nil,
            error_mod.new("io", "failed to start luv poll handle", {
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
