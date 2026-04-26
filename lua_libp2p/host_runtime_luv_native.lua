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
    local entry, register_err = host:_register_connection(conn, state)
    if not entry then
      conn:close()
      return "error", nil, register_err, pending_entry
    end
    return "done", entry, nil, pending_entry
  end

  raw_conn:close()
  if is_nonfatal_stream_error(up_err) then
    return "done", nil, nil, pending_entry
  end
  return "error", nil, up_err, pending_entry
end

function M.process_connection(host, entry, router, is_nonfatal_stream_error)
  local conn = entry.conn
  if entry.process_co == nil then
    entry.process_co = coroutine.create(function()
      local _, process_err = conn:process_one()
      if process_err then
        return nil, nil, nil, process_err
      end
      return conn:accept_stream(router)
    end)
  end

  local ok, stream, protocol_id, handler, stream_err = coroutine.resume(entry.process_co)
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

return M
