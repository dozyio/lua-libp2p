local mss = require("lua_libp2p.protocol.mss")
local ping = require("lua_libp2p.protocol_ping.protocol")
local tcp = require("lua_libp2p.transport.tcp")

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
  local client = assert(tcp.dial(addr, { timeout = 1 }))
  local server = assert(listener:accept(1))

  assert(mss.write_frame(client, mss.PROTOCOL_ID))
  assert(mss.read_frame(server))
  assert(mss.write_frame(server, mss.PROTOCOL_ID))
  assert(mss.read_frame(client))

  assert(mss.write_frame(client, ping.ID))
  local req = assert(mss.read_frame(server))
  if req ~= ping.ID then
    return nil, "server did not receive ping protocol id"
  end
  assert(mss.write_frame(server, ping.ID))
  local ack = assert(mss.read_frame(client))
  if ack ~= ping.ID then
    return nil, "client did not receive ping protocol ack"
  end

  local payload = ping.new_payload()
  assert(client:write(payload))
  assert(ping.handle_once(server))
  local echoed = assert(client:read(ping.PAYLOAD_SIZE))
  if echoed ~= payload then
    return nil, "ping payload mismatch over tcp"
  end

  server:close()
  client:close()
  listener:close()
  return true
end

return {
  name = "ping protocol over tcp+mss",
  run = run,
}
