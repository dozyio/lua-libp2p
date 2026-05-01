local error_mod = require("lua_libp2p.error")
local tcp = require("lua_libp2p.transport_tcp.transport")

local function run()
  local listener, listen_err = tcp.listen({
    host = "127.0.0.1",
    port = 0,
    accept_timeout = 1,
    io_timeout = 1,
  })
  if not listener then
    return nil, listen_err
  end

  local addr, addr_err = listener:multiaddr()
  if not addr then
    listener:close()
    return nil, addr_err
  end

  local client, dial_err = tcp.dial(addr, {
    timeout = 1,
  })
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

  local ok, write_err = client:write("ping")
  if not ok then
    server:close()
    client:close()
    listener:close()
    return nil, write_err
  end

  local inbound, read_err = server:read(4)
  if not inbound then
    server:close()
    client:close()
    listener:close()
    return nil, read_err
  end
  if inbound ~= "ping" then
    server:close()
    client:close()
    listener:close()
    return nil, "unexpected inbound payload"
  end

  local srv_ok, srv_write_err = server:write("pong")
  if not srv_ok then
    server:close()
    client:close()
    listener:close()
    return nil, srv_write_err
  end

  local reply, reply_err = client:read(4)
  if not reply then
    server:close()
    client:close()
    listener:close()
    return nil, reply_err
  end
  if reply ~= "pong" then
    server:close()
    client:close()
    listener:close()
    return nil, "unexpected reply payload"
  end

  local _, timeout_err = server:read(1)
  if not timeout_err then
    server:close()
    client:close()
    listener:close()
    return nil, "expected read timeout"
  end
  if not error_mod.is_error(timeout_err) or timeout_err.kind ~= "timeout" then
    server:close()
    client:close()
    listener:close()
    return nil, "expected timeout error kind"
  end

  local close_ok, close_err = client:close()
  if not close_ok then
    server:close()
    listener:close()
    return nil, close_err
  end

  local _, closed_write_err = client:write("x")
  if not closed_write_err then
    server:close()
    listener:close()
    return nil, "expected closed write failure"
  end
  if closed_write_err.kind ~= "closed" then
    server:close()
    listener:close()
    return nil, "expected closed error kind"
  end

  server:close()
  listener:close()
  return true
end

return {
  name = "tcp listen/dial loopback",
  run = run,
}
