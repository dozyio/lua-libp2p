package.path = table.concat({
  "./?.lua",
  "./?/init.lua",
  package.path,
}, ";")

local host_mod = require("lua_libp2p.host")
local identify = require("lua_libp2p.protocol.identify")
local multiaddr = require("lua_libp2p.multiaddr")
local ping = require("lua_libp2p.protocol.ping")
local peerid = require("lua_libp2p.peerid")

local target = arg[1]
if not target then
  io.stderr:write("usage: lua examples/identify_ping_client.lua /ip4/127.0.0.1/tcp/12345\n")
  os.exit(2)
end

local host, host_err = host_mod.new({})
if not host then
  io.stderr:write("host init failed: " .. tostring(host_err) .. "\n")
  os.exit(1)
end

local id_stream, selected, _, id_err = host:new_stream(target, { identify.ID })
if not id_stream then
  io.stderr:write("identify stream failed: " .. tostring(id_err) .. "\n")
  os.exit(1)
end

local id_msg, read_err = identify.read(id_stream)
if not id_msg then
  io.stderr:write("identify read failed: " .. tostring(read_err) .. "\n")
  os.exit(1)
end

io.stdout:write("identify protocol: " .. tostring(selected) .. "\n")
io.stdout:write("agentVersion: " .. tostring(id_msg.agentVersion) .. "\n")
io.stdout:write("protocolVersion: " .. tostring(id_msg.protocolVersion) .. "\n")

local remote_peer = nil
if type(id_msg.publicKey) == "string" then
  local pid = peerid.from_public_key_proto(id_msg.publicKey)
  if pid then
    remote_peer = pid.id
    io.stdout:write("peer id: " .. remote_peer .. "\n")
  end
end

io.stdout:write("listen addrs:\n")
for _, addr_bytes in ipairs(id_msg.listenAddrs or {}) do
  local decoded = multiaddr.from_bytes(addr_bytes)
  if decoded and decoded.text then
    io.stdout:write("  " .. decoded.text .. "\n")
    if remote_peer then
      io.stdout:write("  " .. decoded.text .. "/p2p/" .. remote_peer .. "\n")
    end
  else
    io.stdout:write("  <invalid multiaddr bytes>\n")
  end
end

local p_stream, p_selected, _, p_err = host:new_stream(target, { ping.ID })
if not p_stream then
  io.stderr:write("ping stream failed: " .. tostring(p_err) .. "\n")
  os.exit(1)
end

local result, ping_err = ping.ping_once(p_stream)
if not result then
  io.stderr:write("ping failed: " .. tostring(ping_err) .. "\n")
  os.exit(1)
end

io.stdout:write(string.format("ping protocol: %s rtt=%.6fs\n", tostring(p_selected), result.rtt_seconds))

host:close()
