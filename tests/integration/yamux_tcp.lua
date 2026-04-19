local tcp = require("lua_libp2p.transport.tcp")
local yamux = require("lua_libp2p.muxer.yamux")

local function expect(ok, err)
  if ok then
    return true
  end
  return nil, err
end

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

  local addr = assert(listener:multiaddr())
  local client_conn = assert(tcp.dial(addr, { timeout = 1 }))
  local server_conn = assert(listener:accept(1))

  local client = yamux.new_session(client_conn, { is_client = true })
  local server = yamux.new_session(server_conn, { is_client = false })

  local c1 = assert(client:open_stream())
  local c2 = assert(client:open_stream())

  assert(server:process_one())
  assert(server:process_one())

  local s1 = server:accept_stream_now()
  local s2 = server:accept_stream_now()
  if not s1 or not s2 then
    return nil, "server did not accept both streams"
  end

  assert(client:process_one())
  assert(client:process_one())
  if not c1.acked or not c2.acked then
    return nil, "client streams were not acknowledged"
  end

  local ok, err = c1:write("alpha")
  if not ok then
    return nil, err
  end
  ok, err = c2:write("beta")
  if not ok then
    return nil, err
  end

  assert(server:process_one())
  assert(server:process_one())

  local a = s1:read_now()
  local b = s2:read_now()
  if a == nil or b == nil then
    -- frames may arrive in either order
    a = a or s2:read_now()
    b = b or s1:read_now()
  end
  if not ((a == "alpha" and b == "beta") or (a == "beta" and b == "alpha")) then
    return nil, "server did not receive both yamux stream payloads"
  end

  assert(client:process_one())
  assert(client:process_one())

  assert(s1:write("one"))
  assert(s2:write("two"))
  assert(client:process_one())
  assert(client:process_one())

  local r1 = c1:read_now()
  local r2 = c2:read_now()
  if r1 == nil or r2 == nil then
    r1 = r1 or c2:read_now()
    r2 = r2 or c1:read_now()
  end
  if not ((r1 == "one" and r2 == "two") or (r1 == "two" and r2 == "one")) then
    return nil, "client did not receive both yamux stream payloads"
  end

  local close_ok, close_err = c1:close_write()
  if not close_ok then
    return nil, close_err
  end
  assert(server:process_one())
  if not s1.remote_closed then
    assert(server:process_one())
  end
  if not s1.remote_closed then
    assert(server:process_one())
  end
  if not s1.remote_closed then
    return nil, "server did not observe stream FIN"
  end

  server_conn:close()
  client_conn:close()
  listener:close()
  return true
end

return {
  name = "yamux multiplexing over tcp",
  run = run,
}
