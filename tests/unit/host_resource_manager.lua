local host_mod = require("lua_libp2p.host")
local error_mod = require("lua_libp2p.error")
local runtime_luv_native = require("lua_libp2p.host.runtime_luv_native")

local function run()
  local default_host, default_host_err = host_mod.new({
    listen_addrs = {},
  })
  if not default_host then
    return nil, default_host_err
  end
  local default_stats = default_host:stats()
  if not default_stats.resources or default_stats.resources.limits.streams ~= 2048 then
    return nil, "expected host to install default resource limits"
  end

  local h, host_err = host_mod.new({
    listen_addrs = {},
    resource_manager = {
      streams = 0,
    },
  })
  if not h then
    return nil, host_err
  end

  h.dial = function()
    return {
      new_stream = function()
        return {
          close = function()
            return true
          end,
        }, "/test/1.0.0"
      end,
    }, {
      direction = "outbound",
      remote_peer_id = "peer-a",
    }
  end

  local stream, _, _, stream_err = h:new_stream("peer-a", { "/test/1.0.0" })
  if stream then
    return nil, "expected resource manager to block outbound stream"
  end
  if not stream_err or stream_err.kind ~= "resource" then
    return nil, "expected outbound stream resource error"
  end

  local stats = h:stats()
  if not stats.resources or stats.resources.system.streams ~= 0 then
    return nil, "expected resource stats in host stats"
  end

  local closed = false
  local inbound_limited, inbound_limited_err = host_mod.new({
    listen_addrs = {},
    resource_manager = {
      connections_inbound = 0,
    },
  })
  if not inbound_limited then
    return nil, inbound_limited_err
  end
  inbound_limited._listeners = {
    {
      accept = function()
        return {
          close = function()
            closed = true
            return true
          end,
        }
      end,
    },
  }
  local poll_ok, poll_err = inbound_limited:_process_runtime_events(0)
  if not poll_ok then
    return nil, "inbound resource exhaustion should reject connection without failing host: " .. tostring(poll_err)
  end
  if not closed then
    return nil, "inbound resource exhaustion should close rejected raw connection"
  end

  local released = 0
  local raw_closed = false
  local failed_task_host = {
    spawn_task = function(_, _, fn)
      local task = {
        id = 1,
        status = "failed",
      }
      local ok, result, task_err = pcall(fn, {
        wait_read = function()
          return nil, error_mod.new("closed", "forced inbound upgrade failure")
        end,
      })
      if ok and result == nil then
        task.error = task_err
      elseif not ok then
        task.error = error_mod.new("protocol", "task panicked", { cause = result })
      end
      return task
    end,
    _close_connection_resource = function(_, scope)
      if scope and scope.id == "transient-inbound" then
        released = released + 1
      end
      return true
    end,
    identity = default_host.identity,
    security_transports = default_host.security_transports,
    muxers = default_host.muxers,
  }
  local raw = {
    set_context = function()
      return true
    end,
    read = function()
      return nil, error_mod.new("closed", "forced inbound upgrade failure")
    end,
    close = function()
      raw_closed = true
      return true
    end,
  }
  local status, _, resume_err = runtime_luv_native.resume_inbound_upgrade(failed_task_host, {
    raw_conn = raw,
    resource_scope = { id = "transient-inbound" },
  }, function(err)
    return error_mod.is_error(err) and err.kind == "closed"
  end)
  if status ~= "error" and status ~= "done" then
    return nil, "failed native inbound upgrade should finish or error"
  end
  if not raw_closed then
    return nil, "failed native inbound upgrade should close raw connection"
  end
  if released ~= 1 then
    return nil, "failed native inbound upgrade should release transient connection resource"
  end
  if status == "error" and not resume_err then
    return nil, "failed native inbound upgrade should return error details"
  end

  local pump_closed_host, pump_closed_host_err = host_mod.new({
    listen_addrs = {},
  })
  if not pump_closed_host then
    return nil, pump_closed_host_err
  end
  pump_closed_host._runtime_impl = {
    sync_watchers = function()
      return true
    end,
  }
  local pump_raw_closed = false
  local pump_scope, pump_scope_err = pump_closed_host:_open_connection_resource("inbound", nil, { transient = true })
  if not pump_scope then
    return nil, pump_scope_err
  end
  local pump_entry, pump_register_err = pump_closed_host:_register_connection({
    close = function()
      pump_raw_closed = true
      return true
    end,
  }, {
    direction = "inbound",
    remote_peer_id = "peer-pump-closed",
    resource_scope = pump_scope,
  })
  if not pump_entry then
    return nil, pump_register_err
  end
  pump_entry.scheduler_pump_task = {
    id = 99,
    status = "failed",
    error = error_mod.new("closed", "pump connection closed"),
  }
  local before_pump_stats = pump_closed_host:stats().resources.system
  if before_pump_stats.connections_inbound ~= 1 then
    return nil, "expected native pump test to reserve one inbound connection"
  end
  local process_ok, process_err = runtime_luv_native.process_connection(pump_closed_host, pump_entry, nil, function(err)
    return error_mod.is_error(err) and err.kind == "closed"
  end)
  if not process_ok then
    return nil, process_err
  end
  if not pump_raw_closed then
    return nil, "nonfatal native pump failure should close connection"
  end
  local after_pump_stats = pump_closed_host:stats().resources.system
  if after_pump_stats.connections_inbound ~= 0 then
    return nil, "nonfatal native pump failure should release inbound connection resource"
  end
  if #pump_closed_host._connections ~= 0 then
    return nil, "nonfatal native pump failure should unregister connection"
  end

  local pump_task_host, pump_task_host_err = host_mod.new({
    listen_addrs = {},
  })
  if not pump_task_host then
    return nil, pump_task_host_err
  end
  pump_task_host._runtime_impl = {
    sync_watchers = function()
      return true
    end,
  }
  local pump_task_closed = false
  local pump_task_scope, pump_task_scope_err = pump_task_host:_open_connection_resource("inbound", nil, { transient = true })
  if not pump_task_scope then
    return nil, pump_task_scope_err
  end
  local pump_task_conn = {
    close = function()
      pump_task_closed = true
      return true
    end,
    set_context = function()
      return true
    end,
    pump_once = function()
      return nil, error_mod.new("closed", "native pump closed")
    end,
  }
  local pump_task_entry, pump_task_register_err = pump_task_host:_register_connection(pump_task_conn, {
    direction = "inbound",
    remote_peer_id = "peer-pump-task-closed",
    resource_scope = pump_task_scope,
  })
  if not pump_task_entry then
    return nil, pump_task_register_err
  end
  local pump_task, pump_task_err = runtime_luv_native.start_connection_pump_task(
    pump_task_host,
    pump_task_entry,
    function(err)
      return error_mod.is_error(err) and err.kind == "closed"
    end
  )
  if not pump_task then
    return nil, pump_task_err
  end
  local pump_task_ok, pump_task_run_err = pump_task_host:_run_background_tasks({ max_resumes = 1 })
  if not pump_task_ok then
    return nil, pump_task_run_err
  end
  if not pump_task_closed then
    return nil, "nonfatal native pump task failure should close connection"
  end
  local after_pump_task_stats = pump_task_host:stats().resources.system
  if after_pump_task_stats.connections_inbound ~= 0 then
    return nil, "nonfatal native pump task failure should release inbound connection resource"
  end
  if #pump_task_host._connections ~= 0 then
    return nil, "nonfatal native pump task failure should unregister connection"
  end

  return true
end

return {
  name = "host resource manager integration",
  run = run,
}
