--- Host helpers for native luv transport integration.
local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log").subsystem("host")
local upgrader = require("lua_libp2p.network.upgrader")

local M = {}

function M.is_native_host(host)
  return host._runtime == "luv" and host._tcp_transport and host._tcp_transport.BACKEND == "luv-native"
end

function M.pending_raw(entry)
  if type(entry) == "table" and entry.raw_conn ~= nil then
    return entry.raw_conn
  end
  return entry
end

local function release_pending_scope(host, pending_entry)
  if type(pending_entry) == "table" and pending_entry.resource_scope ~= nil then
    local scope = pending_entry.resource_scope
    pending_entry.resource_scope = nil
    if host and type(host._close_connection_resource) == "function" then
      host:_close_connection_resource(scope)
    end
  end
end

local function is_terminal_connection_error(err)
  if not error_mod.is_error(err) then
    return false
  end
  if err.context and err.context.stream_id ~= nil then
    return false
  end
  return err.kind == "closed" or err.kind == "decode" or err.kind == "protocol"
end

local function raw_conn_fields(raw_conn)
  local fields = {}
  if raw_conn and type(raw_conn.debug_id) == "function" then
    local ok, raw_id = pcall(raw_conn.debug_id, raw_conn)
    if ok then
      fields.raw_id = raw_id
    end
  elseif raw_conn and raw_conn._debug_id ~= nil then
    fields.raw_id = raw_conn._debug_id
  end
  if raw_conn and type(raw_conn.remote_multiaddr) == "function" then
    local ok, addr = pcall(raw_conn.remote_multiaddr, raw_conn)
    if ok then
      fields.remote_addr = addr
    end
  end
  if raw_conn and type(raw_conn.local_multiaddr) == "function" then
    local ok, addr = pcall(raw_conn.local_multiaddr, raw_conn)
    if ok then
      fields.local_addr = addr
    end
  end
  return fields
end

local function trim_value(value, max_len)
  local text = tostring(value or "")
  local limit = max_len or 160
  if #text <= limit then
    return text
  end
  return text:sub(1, limit) .. "..."
end

local function bump_map(map, key)
  key = tostring(key or "unknown")
  map[key] = (tonumber(map[key]) or 0) + 1
end

local function error_kind(err)
  if error_mod.is_error(err) then
    return err.kind or "error"
  end
  return type(err)
end

local function error_cause_text(err)
  if error_mod.is_error(err) and err.context and err.context.cause ~= nil then
    return trim_value(err.context.cause, 120)
  end
  return trim_value(err, 120)
end

local function is_remote_aborted_upgrade(err)
  if not error_mod.is_error(err) then
    return false
  end
  if err.kind == "closed" then
    return true
  end
  local cause = string.upper(tostring(err.context and err.context.cause or err))
  return cause:find("ECONNRESET", 1, true) ~= nil
    or cause:find("ENOTCONN", 1, true) ~= nil
    or cause:find("CONNECTION CLOSED", 1, true) ~= nil
    or cause:find("YAMUX SESSION CLOSED", 1, true) ~= nil
    or cause:find("YAMUX STREAM IS RESET", 1, true) ~= nil
end

local function record_inbound_upgrade_failure(host, err)
  if type(host) ~= "table" then
    return
  end
  local debug_maps = rawget(host, "_debug_maps")
  if type(debug_maps) ~= "table" then
    debug_maps = {}
    rawset(host, "_debug_maps", debug_maps)
  end
  debug_maps.inbound_upgrade_failed_by_kind = debug_maps.inbound_upgrade_failed_by_kind or {}
  debug_maps.inbound_upgrade_failed_by_cause = debug_maps.inbound_upgrade_failed_by_cause or {}

  bump_map(debug_maps.inbound_upgrade_failed_by_kind, error_kind(err))
  bump_map(debug_maps.inbound_upgrade_failed_by_cause, error_cause_text(err))
end

local function record_inbound_upgrade_result(host, err)
  if is_remote_aborted_upgrade(err) then
    if type(host._bump_debug_counter) == "function" then
      host:_bump_debug_counter("inbound_upgrade_remote_aborted")
    end
  elseif type(host._bump_debug_counter) == "function" then
    host:_bump_debug_counter("inbound_upgrade_failed")
  end
  record_inbound_upgrade_failure(host, err)
end

function M.resume_inbound_upgrade(host, pending_entry, is_nonfatal_stream_error)
  local raw_conn = M.pending_raw(pending_entry)
  if type(pending_entry) ~= "table" or pending_entry.raw_conn == nil then
    pending_entry = { raw_conn = raw_conn }
  end

  if type(host.spawn_task) == "function" then
    if pending_entry.task == nil then
      local task, task_err = host:spawn_task("host.inbound_upgrade", function(ctx)
        if type(host._bump_debug_counter) == "function" then
          host:_bump_debug_counter("inbound_upgrade_started")
        end
        log.debug("host inbound upgrade started", raw_conn_fields(raw_conn))
        if raw_conn and type(raw_conn.set_context) == "function" then
          raw_conn:set_context(ctx)
        end
        local upgrade_result = {
          pcall(upgrader.upgrade_inbound, raw_conn, {
            local_keypair = host.identity,
            security_protocols = host.security_transports,
            muxer_protocols = host.muxers,
            ctx = ctx,
          }),
        }
        if raw_conn and type(raw_conn.set_context) == "function" then
          raw_conn:set_context(nil)
        end
        if not upgrade_result[1] then
          local err = error_mod.new("protocol", "inbound upgrade task failed", { cause = upgrade_result[2] })
          record_inbound_upgrade_result(host, err)
          local fields = raw_conn_fields(raw_conn)
          fields.cause = tostring(err)
          fields.cause_kind = err.kind
          log.debug("host inbound upgrade failed", fields)
          return nil, err
        end
        local conn, state, up_err = upgrade_result[2], upgrade_result[3], upgrade_result[4]
        if not conn then
          record_inbound_upgrade_result(host, up_err)
          local fields = raw_conn_fields(raw_conn)
          fields.cause = tostring(up_err)
          fields.cause_kind = error_mod.is_error(up_err) and up_err.kind or nil
          log.debug("host inbound upgrade failed", fields)
          return nil, up_err
        end
        state.direction = state.direction or "inbound"
        state.resource_scope = pending_entry.resource_scope
        pending_entry.resource_scope = nil
        local entry, register_err = host:_register_connection(conn, state)
        if not entry then
          if type(host._bump_debug_counter) == "function" then
            host:_bump_debug_counter("inbound_registration_failed")
          end
          conn:close()
          log.debug("host inbound upgrade registration failed", {
            remote_addr = state.remote_addr,
            peer_id = state.remote_peer_id,
            cause = tostring(register_err),
            cause_kind = error_mod.is_error(register_err) and register_err.kind or nil,
          })
          return nil, register_err
        end
        log.debug("host inbound upgrade completed", {
          connection_id = entry.id,
          remote_addr = state.remote_addr,
          peer_id = state.remote_peer_id,
          security = state.security,
          muxer = state.muxer,
        })
        if type(host._bump_debug_counter) == "function" then
          host:_bump_debug_counter("inbound_upgrade_completed")
        end
        return true
      end, { service = "host", priority = "front" })
      if not task then
        local fields = raw_conn_fields(raw_conn)
        fields.cause = tostring(task_err)
        log.debug("host inbound raw connection closing before upgrade task", fields)
        raw_conn:close()
        release_pending_scope(host, pending_entry)
        return "error", nil, task_err, pending_entry
      end
      local fields = raw_conn_fields(raw_conn)
      fields.task_id = task.id
      log.debug("host inbound upgrade task spawned", fields)
      pending_entry.task = task
    end

    local task = pending_entry.task
    if task.status == "completed" then
      return "done", nil, nil, pending_entry
    end
    if task.status == "failed" or task.status == "cancelled" then
      if type(host._bump_debug_counter) == "function" then
        host:_bump_debug_counter("inbound_upgrade_task_ended")
      end
      local close_fields = raw_conn_fields(raw_conn)
      close_fields.status = task.status
      close_fields.cause = tostring(task.error or task.status)
      log.debug("host inbound raw connection closing after upgrade task ended", close_fields)
      raw_conn:close()
      release_pending_scope(host, pending_entry)
      local task_err = task.error or task.status
      local fields = raw_conn_fields(raw_conn)
      fields.status = task.status
      fields.cause = tostring(task_err)
      fields.cause_kind = error_mod.is_error(task_err) and task_err.kind or nil
      log.debug("host inbound upgrade task ended", fields)
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
    release_pending_scope(host, pending_entry)
    return "error", nil, error_mod.new("protocol", "inbound upgrade coroutine failed", { cause = conn }), pending_entry
  end
  if coroutine.status(pending_entry.co) ~= "dead" then
    return "pending", nil, nil, pending_entry
  end
  if conn then
    state.direction = state.direction or "inbound"
    state.resource_scope = pending_entry.resource_scope
    pending_entry.resource_scope = nil
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
  release_pending_scope(host, pending_entry)
  record_inbound_upgrade_failure(host, up_err)
  if is_nonfatal_stream_error(up_err) then
    return "done", nil, nil, pending_entry
  end
  return "error", nil, up_err, pending_entry
end

function M.process_connection(host, entry, router, is_nonfatal_stream_error)
  local conn = entry.conn

  local function close_and_unregister(cause)
    if conn and type(conn.close) == "function" then
      conn:close()
    end
    if
      type(host._unregister_connection) == "function"
      and host._connections_by_id
      and host._connections_by_id[entry.id] == entry
    then
      local unregistered, unregister_err = host:_unregister_connection(nil, entry, cause)
      if not unregistered then
        return nil, unregister_err
      end
    end
    if host._runtime_impl and type(host._runtime_impl.sync_watchers) == "function" then
      local sync_ok, sync_err = host._runtime_impl.sync_watchers(host)
      if not sync_ok then
        return nil, sync_err
      end
    end
    return true
  end

  if entry.scheduler_pump_task ~= nil then
    local task = entry.scheduler_pump_task
    if task.status == "failed" or task.status == "cancelled" then
      entry.scheduler_pump_task = nil
      local task_err = task.error or task.status
      log.debug("native connection pump task ended", {
        connection_id = entry.id,
        peer_id = entry.state and entry.state.remote_peer_id or nil,
        status = task.status,
        cause = tostring(task_err),
        kind = error_mod.is_error(task_err) and task_err.kind or nil,
      })
      if is_terminal_connection_error(task_err) then
        return close_and_unregister(task_err)
      end
      return nil, task_err
    end
    return true
  end

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
      log.debug("native connection pump frame error", {
        connection_id = entry.id,
        peer_id = entry.state and entry.state.remote_peer_id or nil,
        cause = tostring(pump_err),
        kind = error_mod.is_error(pump_err) and pump_err.kind or nil,
      })
      if not is_nonfatal_stream_error(pump_err) then
        return nil, pump_err
      end
      if is_terminal_connection_error(pump_err) then
        return close_and_unregister(pump_err)
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
    local stream_scope, resource_err = host:_open_stream_resource(entry, "inbound", protocol_id)
    if resource_err then
      if type(stream.reset_now) == "function" then
        pcall(function()
          stream:reset_now()
        end)
      elseif type(stream.close) == "function" then
        pcall(function()
          stream:close()
        end)
      end
      return nil, resource_err
    end
    stream = host:_wrap_stream_resource(stream, stream_scope)
    if
      host:_connection_is_limited(entry.state)
      and not host:_protocol_allowed_on_limited_connection(protocol_id, handler_options)
    then
      if type(stream.reset_now) == "function" then
        pcall(function()
          stream:reset_now()
        end)
      elseif type(stream.close) == "function" then
        pcall(function()
          stream:close()
        end)
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
        if not frame and pump_err and is_terminal_connection_error(pump_err) then
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
  task.on_finished = function(host_obj, finished_task)
    local finished_err = finished_task.error or finished_task.status
    if
      (finished_task.status == "failed" or finished_task.status == "cancelled")
      and is_terminal_connection_error(finished_err)
    then
      if type(host_obj._bump_debug_counter) == "function" then
        host_obj:_bump_debug_counter("connection_pump_terminal")
      end
      if type(conn.close) == "function" then
        conn:close()
      end
      if
        type(host_obj._unregister_connection) == "function"
        and host_obj._connections_by_id
        and host_obj._connections_by_id[entry.id] == entry
      then
        local unregistered, unregister_err = host_obj:_unregister_connection(nil, entry, finished_err)
        if not unregistered then
          return nil, unregister_err
        end
      end
      if host_obj._runtime_impl and type(host_obj._runtime_impl.sync_watchers) == "function" then
        local sync_ok, sync_err = host_obj._runtime_impl.sync_watchers(host_obj)
        if not sync_ok then
          return nil, sync_err
        end
      end
    end
    return true
  end
  entry.scheduler_pump_task = task
  return task
end

return M
