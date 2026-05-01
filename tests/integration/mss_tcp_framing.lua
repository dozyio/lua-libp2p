local mss = require("lua_libp2p.protocol.mss")
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

  local client, dial_err = tcp.dial(addr, { timeout = 1 })
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

  local ok, write_err = mss.write_frame(client, mss.PROTOCOL_ID)
  if not ok then
    server:close()
    client:close()
    listener:close()
    return nil, write_err
  end

  local inbound, inbound_err = mss.read_frame(server)
  if not inbound then
    server:close()
    client:close()
    listener:close()
    return nil, inbound_err
  end
  if inbound ~= mss.PROTOCOL_ID then
    server:close()
    client:close()
    listener:close()
    return nil, "unexpected multistream frame over tcp"
  end

  local ok2, write_err2 = mss.write_frame(server, mss.NA)
  if not ok2 then
    server:close()
    client:close()
    listener:close()
    return nil, write_err2
  end

  local reply, reply_err = mss.read_frame(client)
  if not reply then
    server:close()
    client:close()
    listener:close()
    return nil, reply_err
  end
  if reply ~= mss.NA then
    server:close()
    client:close()
    listener:close()
    return nil, "unexpected multistream reply over tcp"
  end

  server:close()
  client:close()
  listener:close()
  return true
end

return {
  name = "multistream-select framing over tcp",
  run = run,
}
