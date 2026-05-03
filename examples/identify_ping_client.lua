package.path = table.concat({
  "./?.lua",
  "./?/init.lua",
  package.path,
}, ";")

local host_mod = require("lua_libp2p.host")
local identify_service = require("lua_libp2p.protocol_identify.service")
local multiaddr = require("lua_libp2p.multiaddr")
local ping_service = require("lua_libp2p.protocol_ping.service")
local peerid = require("lua_libp2p.peerid")

local target = arg[1]
if not target then
  io.stderr:write("usage: lua examples/identify_ping_client.lua /ip4/127.0.0.1/tcp/12345\n")
  os.exit(2)
end

local host, host_err = host_mod.new({
  services = {
    identify = { module = identify_service },
    ping = { module = ping_service },
  },
})
if not host then
  io.stderr:write("host init failed: " .. tostring(host_err) .. "\n")
  os.exit(1)
end

local identify_result, identify_err = host.services.identify:identify(target)
if not identify_result then
  io.stderr:write(tostring(identify_err) .. "\n")
  os.exit(1)
end

local id_msg = identify_result.message
local selected = identify_result.protocol

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

local ping_result, ping_err = host.services.ping:ping(target)
if not ping_result then
  io.stderr:write(tostring(ping_err) .. "\n")
  os.exit(1)
end

io.stdout:write(string.format("ping protocol: %s rtt=%.6fs\n", tostring(ping_result.protocol), ping_result.rtt_seconds))

host:close()
