local tcp_luv = require("lua_libp2p.transport_tcp.luv")

local function run()
  if tcp_luv.BACKEND ~= "luv-native" and tcp_luv.BACKEND ~= "luv-proxy" then
    return nil, "unexpected tcp_luv backend marker"
  end

  local has_luv, uv = pcall(require, "luv")
  if not has_luv then
    if tcp_luv.BACKEND ~= "luv-proxy" then
      return nil, "expected luv-proxy marker when luv is unavailable"
    end
    return true
  end

  if tcp_luv.BACKEND ~= "luv-native" and tcp_luv.BACKEND ~= "luv-proxy" then
    return nil, "unexpected backend marker when luv is available"
  end

  if tcp_luv.BACKEND ~= "luv-native" then
    return true
  end

  local real_new_tcp = uv.new_tcp
  uv.new_tcp = function()
    return {
      connect = function() end,
      close = function() return true end,
    }
  end
  local _, forced_timeout_err = tcp_luv.dial({ host = "127.0.0.1", port = 9999 }, { timeout = 0.02 })
  uv.new_tcp = real_new_tcp
  if not forced_timeout_err then
    return nil, "expected forced timeout error from native tcp_luv dial"
  end
  if forced_timeout_err.kind ~= "timeout" then
    return nil, "expected timeout kind for forced native tcp_luv dial timeout"
  end

  uv.new_tcp = function()
    return {
      connect = function(_, _, _, cb)
        cb("ECONNRESET")
      end,
      close = function() return true end,
    }
  end
  local _, forced_closed_err = tcp_luv.dial({ host = "127.0.0.1", port = 9999 }, { timeout = 1 })
  uv.new_tcp = real_new_tcp
  if not forced_closed_err then
    return nil, "expected forced closed error from native tcp_luv dial"
  end
  if forced_closed_err.kind ~= "closed" then
    return nil, "expected closed kind for forced native tcp_luv dial closed error"
  end

  uv.new_tcp = function()
    return {
      connect = function(_, _, _, cb)
        cb("EADDRNOTAVAIL")
      end,
      close = function() return true end,
    }
  end
  local _, forced_io_err = tcp_luv.dial({ host = "127.0.0.1", port = 9999 }, { timeout = 1 })
  uv.new_tcp = real_new_tcp
  if not forced_io_err then
    return nil, "expected forced io error from native tcp_luv dial"
  end
  if forced_io_err.kind ~= "io" then
    return nil, "expected io kind for forced native tcp_luv dial io error"
  end

  uv.new_tcp = function()
    return {
      connect = function(_, _, _, cb)
        cb(nil)
      end,
      getsockname = function()
        return { ip = "127.0.0.1", port = 10001 }
      end,
      getpeername = function()
        return { ip = "127.0.0.1", port = 10002 }
      end,
      write = function(_, _, cb)
        cb("ECONNRESET")
      end,
      shutdown = function(_, cb)
        if cb then
          cb(nil)
        end
      end,
      close = function() return true end,
    }
  end
  local forced_write_closed_conn, forced_write_closed_conn_err = tcp_luv.dial({ host = "127.0.0.1", port = 10002 }, { timeout = 1 })
  uv.new_tcp = real_new_tcp
  if not forced_write_closed_conn then
    return nil, forced_write_closed_conn_err
  end
  local _, forced_write_closed_err = forced_write_closed_conn:write("payload")
  if not forced_write_closed_err then
    return nil, "expected forced closed error from native tcp_luv write"
  end
  if forced_write_closed_err.kind ~= "closed" then
    return nil, "expected closed kind for forced native tcp_luv write closed error"
  end

  local _, forced_write_after_closed_err = forced_write_closed_conn:write("payload")
  forced_write_closed_conn:close()
  if not forced_write_after_closed_err then
    return nil, "expected closed write failure after native tcp_luv close mapping"
  end
  if forced_write_after_closed_err.kind ~= "closed" then
    return nil, "expected closed kind for second forced native tcp_luv write"
  end

  uv.new_tcp = function()
    return {
      connect = function(_, _, _, cb)
        cb(nil)
      end,
      getsockname = function()
        return { ip = "127.0.0.1", port = 10003 }
      end,
      getpeername = function()
        return { ip = "127.0.0.1", port = 10004 }
      end,
      read_start = function(_, cb)
        cb("EOF", nil)
      end,
      read_stop = function() return true end,
      shutdown = function(_, cb)
        if cb then
          cb(nil)
        end
      end,
      close = function() return true end,
    }
  end
  local forced_read_closed_conn, forced_read_closed_conn_err = tcp_luv.dial({ host = "127.0.0.1", port = 10004 }, { timeout = 1 })
  uv.new_tcp = real_new_tcp
  if not forced_read_closed_conn then
    return nil, forced_read_closed_conn_err
  end
  local _, forced_read_closed_err = forced_read_closed_conn:read(1)
  forced_read_closed_conn:close()
  if not forced_read_closed_err then
    return nil, "expected forced closed error from native tcp_luv read"
  end
  if forced_read_closed_err.kind ~= "closed" then
    return nil, "expected closed kind for forced native tcp_luv read closed error"
  end

  uv.new_tcp = function()
    return {
      connect = function(_, _, _, cb)
        cb(nil)
      end,
      getsockname = function()
        return { ip = "127.0.0.1", port = 10005 }
      end,
      getpeername = function()
        return { ip = "127.0.0.1", port = 10006 }
      end,
      read_start = function(_, cb)
        cb("EADDRNOTAVAIL", nil)
      end,
      read_stop = function() return true end,
      shutdown = function(_, cb)
        if cb then
          cb(nil)
        end
      end,
      close = function() return true end,
    }
  end
  local forced_read_io_conn, forced_read_io_conn_err = tcp_luv.dial({ host = "127.0.0.1", port = 10006 }, { timeout = 1 })
  uv.new_tcp = real_new_tcp
  if not forced_read_io_conn then
    return nil, forced_read_io_conn_err
  end
  local _, forced_read_io_err = forced_read_io_conn:read(1)
  forced_read_io_conn:close()
  if not forced_read_io_err then
    return nil, "expected forced io error from native tcp_luv read"
  end
  if forced_read_io_err.kind ~= "io" then
    return nil, "expected io kind for forced native tcp_luv read io error"
  end

  uv.new_tcp = function()
    return {
      connect = function(_, _, _, cb)
        cb(nil)
      end,
      getsockname = function()
        return { ip = "127.0.0.1", port = 10007 }
      end,
      getpeername = function()
        return { ip = "127.0.0.1", port = 10008 }
      end,
      read_start = function() end,
      read_stop = function() return true end,
      shutdown = function(_, cb)
        if cb then
          cb(nil)
        end
      end,
      close = function() return true end,
    }
  end
  local forced_read_timeout_conn, forced_read_timeout_conn_err = tcp_luv.dial({ host = "127.0.0.1", port = 10008 }, { timeout = 1, io_timeout = 0.02 })
  uv.new_tcp = real_new_tcp
  if not forced_read_timeout_conn then
    return nil, forced_read_timeout_conn_err
  end
  local _, forced_read_timeout_err = forced_read_timeout_conn:read(1)
  forced_read_timeout_conn:close()
  if not forced_read_timeout_err then
    return nil, "expected forced timeout error from native tcp_luv read"
  end
  if forced_read_timeout_err.kind ~= "timeout" then
    return nil, "expected timeout kind for forced native tcp_luv read timeout"
  end

  local listener, listen_err = tcp_luv.listen({
    host = "127.0.0.1",
    port = 0,
    accept_timeout = 1,
    io_timeout = 0.02,
  })
  if not listener then
    return nil, listen_err
  end

  local addr, addr_err = listener:multiaddr()
  if not addr then
    listener:close()
    return nil, addr_err
  end

  local client, dial_err = tcp_luv.dial(addr, { timeout = 1 })
  if not client then
    listener:close()
    return nil, dial_err
  end

  local server, accept_err = listener:accept(1)
  if not server then
    client:close()
    listener:close()
    return nil, accept_err
  end

  local readable = 0
  local unwatch, watch_err = server:watch_luv_readable(function()
    readable = readable + 1
  end)
  if type(unwatch) ~= "function" then
    server:close()
    client:close()
    listener:close()
    return nil, watch_err or "expected watch_luv_readable unregister callback"
  end

  local wrote, write_err = client:write("x")
  if not wrote then
    unwatch()
    server:close()
    client:close()
    listener:close()
    return nil, write_err
  end

  for _ = 1, 200 do
    uv.run("nowait")
    if readable > 0 then
      break
    end
  end

  unwatch()
  server:close()
  client:close()
  listener:close()

  if readable <= 0 then
    return nil, "expected luv readable watcher callback"
  end

  local listener_timeout, timeout_listen_err = tcp_luv.listen({
    host = "127.0.0.1",
    port = 0,
    accept_timeout = 0.02,
    io_timeout = 1,
  })
  if not listener_timeout then
    return nil, timeout_listen_err
  end

  local _, accept_timeout_err = listener_timeout:accept()
  listener_timeout:close()
  if not accept_timeout_err or accept_timeout_err.kind ~= "timeout" then
    return nil, "expected listener default accept timeout"
  end

  local listener_io, io_listen_err = tcp_luv.listen({
    host = "127.0.0.1",
    port = 0,
    accept_timeout = 1,
    io_timeout = 0.02,
  })
  if not listener_io then
    return nil, io_listen_err
  end

  local io_addr, io_addr_err = listener_io:multiaddr()
  if not io_addr then
    listener_io:close()
    return nil, io_addr_err
  end

  local io_client, io_dial_err = tcp_luv.dial(io_addr, { timeout = 1, io_timeout = 1 })
  if not io_client then
    listener_io:close()
    return nil, io_dial_err
  end

  local io_server, io_accept_err = listener_io:accept(1)
  if not io_server then
    io_client:close()
    listener_io:close()
    return nil, io_accept_err
  end

  local _, io_read_timeout = io_server:read(1)
  if not io_read_timeout or io_read_timeout.kind ~= "timeout" then
    io_server:close()
    io_client:close()
    listener_io:close()
    return nil, "expected read timeout on native tcp_luv connection"
  end

  local wrote_after_timeout, write_after_timeout_err = io_client:write("z")
  if not wrote_after_timeout then
    io_server:close()
    io_client:close()
    listener_io:close()
    return nil, write_after_timeout_err
  end

  local resumed_data, resumed_read_err = io_server:read(1)
  io_server:close()
  io_client:close()
  listener_io:close()
  if not resumed_data then
    return nil, resumed_read_err
  end
  if resumed_data ~= "z" then
    return nil, "expected read after timeout to succeed"
  end

  return true
end

return {
  name = "tcp luv transport wrapper",
  run = run,
}
