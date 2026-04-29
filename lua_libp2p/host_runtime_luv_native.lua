local error_mod = require("lua_libp2p.error")
local upgrader = require("lua_libp2p.network.upgrader")

local M = {}

function M.is_native_host(host)
  return host._runtime == "luv"
    and host._tcp_transport
    and host._tcp_transport.BACKEND == "luv-native"
end

function M.pending_raw(entry)
  if type(entry) == "table" and entry.raw_conn ~= nil then
    return entry.raw_conn
  end
  return entry
end

function M.resume_inbound_upgrade(host, pending_entry, is_nonfatal_stream_error)
  local raw_conn = M.pending_raw(pending_entry)
  if type(pending_entry) ~= "table" or pending_entry.raw_conn == nil then
    pending_entry = { raw_conn = raw_conn }
  end

  if type(host.spawn_task) == "function" then
    if pending_entry.task == nil then
      local task, task_err = host:spawn_task("host.inbound_upgrade", function(ctx)
        if raw_conn and type(raw_conn.set_context) == "function" then
          raw_conn:set_context(ctx)
        end
        local upgrade_result = { pcall(upgrader.upgrade_inbound, raw_conn, {
          local_keypair = host.identity,
          security_protocols = host.security_transports,
          muxer_protocols = host.muxers,
          ctx = ctx,
        }) }
        if raw_conn and type(raw_conn.set_context) == "function" then
          raw_conn:set_context(nil)
        end
        if not upgrade_result[1] then
          return nil, error_mod.new("protocol", "inbound upgrade task failed", { cause = upgrade_result[2] })
        end
        local conn, state, up_err = upgrade_result[2], upgrade_result[3], upgrade_result[4]
        if not conn then
          return nil, up_err
        end
        state.direction = state.direction or "inbound"
        local entry, register_err = host:_register_connection(conn, state)
        if not entry then
          conn:close()
          return nil, register_err
        end
        return true
      end, { service = "host", priority = "front" })
      if not task then
        raw_conn:close()
        return "error", nil, task_err, pending_entry
      end
      pending_entry.task = task
    end

    local task = pending_entry.task
    if task.status == "completed" then
      return "done", nil, nil, pending_entry
    end
    if task.status == "failed" or task.status == "cancelled" then
      raw_conn:close()
      local task_err = task.error or task.status
      if is_nonfatal_stream_error(task_err) then
        return "done", nil, nil, pending_entry
      end
      return "error", nil, task_err, pending_entry
    end
    return "pending", nil, nil, pending_entry
  end

  if pending_entry.co == nil then
    pending_entry.co = coroutine.create(function()
      return upgrader.upgrade_inbound(raw_conn, {
        local_keypair = host.identity,
        security_protocols = host.security_transports,
        muxer_protocols = host.muxers,
      })
    end)
  end

  local ok, conn, state, up_err = coroutine.resume(pending_entry.co)
  if not ok then
    raw_conn:close()
    return "error", nil, error_mod.new("protocol", "inbound upgrade coroutine failed", { cause = conn }), pending_entry
  end
  if coroutine.status(pending_entry.co) ~= "dead" then
    return "pending", nil, nil, pending_entry
  end
  if conn then
    state.direction = state.direction or "inbound"
    local entry, register_err = host:_register_connection(conn, state)
    if not entry then
      conn:close()
      return "error", nil, register_err, pending_entry
    end
    return "done", entry, nil, pending_entry
  end

  if up_err and error_mod.is_error(up_err) and up_err.kind == "timeout" then
    pending_entry.co = nil
    return "pending", nil, nil, pending_entry
  end

  raw_conn:close()
  if is_nonfatal_stream_error(up_err) then
    return "done", nil, nil, pending_entry
  end
  return "error", nil, up_err, pending_entry
end

function M.process_connection(host, entry, router, is_nonfatal_stream_error)
  if entry.scheduler_pump_task ~= nil then
    local task = entry.scheduler_pump_task
    if task.status == "failed" or task.status == "cancelled" then
      entry.scheduler_pump_task = nil
      local task_err = task.error or task.status
      if is_nonfatal_stream_error(task_err) then
        return true
      end
      return nil, task_err
    end
    return true
  end

  local conn = entry.conn
  if entry.pump_co == nil then
    entry.pump_co = coroutine.create(function()
      if type(conn.pump_once) == "function" then
        return conn:pump_once()
      end
      return conn:process_one()
    end)
  end

  local pump_ok, _, pump_err = coroutine.resume(entry.pump_co)
  if not pump_ok then
    entry.pump_co = nil
    return nil, error_mod.new("protocol", "connection pump coroutine failed", { cause = pump_err })
  end
  if coroutine.status(entry.pump_co) == "dead" then
    entry.pump_co = nil
    if pump_err then
      if not is_nonfatal_stream_error(pump_err) then
        return nil, pump_err
      end
      return true
    end
  else
    return true
  end

  if entry.process_co == nil then
    entry.process_co = coroutine.create(function()
      return conn:accept_stream(router)
    end)
  end

  local ok, stream, protocol_id, handler, handler_options, stream_err = coroutine.resume(entry.process_co)
  if not ok then
    entry.process_co = nil
    return nil, error_mod.new("protocol", "connection processing coroutine failed", { cause = stream })
  end
  if coroutine.status(entry.process_co) ~= "dead" then
    return true
  end
  entry.process_co = nil

  if stream_err then
    if is_nonfatal_stream_error(stream_err) then
      return true
    end
    return nil, stream_err
  end
  if stream and handler then
    if host:_connection_is_limited(entry.state)
      and not host:_protocol_allowed_on_limited_connection(protocol_id, handler_options)
    then
      if type(stream.reset_now) == "function" then
        pcall(function() stream:reset_now() end)
      elseif type(stream.close) == "function" then
        pcall(function() stream:close() end)
      end
      return true
    end
    host:_spawn_handler_task(handler, {
      stream = stream,
      host = host,
      connection = conn,
      state = entry.state,
      protocol = protocol_id,
    })
  end
  return true
end

function M.start_connection_pump_task(host, entry, is_nonfatal_stream_error)
  if entry.scheduler_pump_task ~= nil then
    return entry.scheduler_pump_task
  end
  local conn = entry.conn
  local task, task_err = host:spawn_task("host.connection_pump", function(ctx)
    if type(conn.set_context) == "function" then
      conn:set_context(ctx)
    end
    while host._connections_by_id and host._connections_by_id[entry.id] == entry do
      local processed = 0
      local should_wait = false
      repeat
        local frame, pump_err
        if type(conn.pump_once) == "function" then
          frame, pump_err = conn:pump_once()
        else
          frame, pump_err = conn:process_one()
        end
        if not frame and pump_err and not is_nonfatal_stream_error(pump_err) then
          if type(conn.set_context) == "function" then
            conn:set_context(nil)
          end
          return nil, pump_err
        end
        if not frame then
          should_wait = true
        end

        if frame then
          processed = processed + 1
          while type(conn.accept_stream_raw) == "function" do
            local stream, stream_err = conn:accept_stream_raw()
            if not stream then
              if stream_err and not is_nonfatal_stream_error(stream_err) then
                if type(conn.set_context) == "function" then
                  conn:set_context(nil)
                end
                return nil, stream_err
              end
              break
            end
            local _, neg_task_err = host:_spawn_stream_negotiation_task(stream, conn, entry)
            if neg_task_err then
              if type(conn.set_context) == "function" then
                conn:set_context(nil)
              end
              return nil, neg_task_err
            end
          end
        end
      until should_wait or processed >= 64

      local ok, wait_err = ctx:wait_read(conn)
      if ok == nil and wait_err then
        if type(conn.set_context) == "function" then
          conn:set_context(nil)
        end
        return nil, wait_err
      end
    end
    if type(conn.set_context) == "function" then
      conn:set_context(nil)
    end
    return true
  end, { service = "host" })
  if not task then
    return nil, task_err
  end
  entry.scheduler_pump_task = task
  return task
end

return M
