package.path = table.concat({
  "./?.lua",
  "./?/init.lua",
  package.path,
}, ";")

local host_mod = require("lua_libp2p.host")
local identify = require("lua_libp2p.protocol.identify")
local key_pb = require("lua_libp2p.crypto.key_pb")
local peerid = require("lua_libp2p.peerid")
local ping = require("lua_libp2p.protocol.ping")

local listen_addr = arg[1] or "/ip4/127.0.0.1/tcp/0"

local host, host_err = host_mod.new({
  listen_addrs = { listen_addr },
})
if not host then
  io.stderr:write("host init failed: " .. tostring(host_err) .. "\n")
  os.exit(1)
end

local local_peer, local_peer_err = peerid.from_ed25519_public_key(host.identity.public_key)
if not local_peer then
  io.stderr:write("peer id derivation failed: " .. tostring(local_peer_err) .. "\n")
  os.exit(1)
end

local ok, err = host:handle(identify.ID, function(stream, ctx)
  local local_pub, pub_err = key_pb.encode_public_key(key_pb.KEY_TYPE.Ed25519, host.identity.public_key)
  if not local_pub then
    return nil, pub_err
  end

  local observed = nil
  if ctx and ctx.connection and ctx.connection.raw and ctx.connection:raw().remote_multiaddr then
    observed = ctx.connection:raw():remote_multiaddr()
  end

  local msg = {
    protocolVersion = "/lua-libp2p/examples/0.1.0",
    agentVersion = "lua-libp2p-example/0.1.0",
    publicKey = local_pub,
    listenAddrs = host:get_listen_addrs(),
    observedAddr = observed,
    protocols = { identify.ID, ping.ID },
  }

  local wrote, write_err = identify.write(stream, msg)
  if not wrote then
    return nil, write_err
  end

  if type(stream.close_write) == "function" then
    stream:close_write()
  end

  return true
end)
if not ok then
  io.stderr:write("identify handler setup failed: " .. tostring(err) .. "\n")
  os.exit(1)
end

ok, err = host:handle(ping.ID, function(stream)
  return ping.handle_once(stream)
end)
if not ok then
  io.stderr:write("ping handler setup failed: " .. tostring(err) .. "\n")
  os.exit(1)
end

local addrs, listen_err = host:listen()
if not addrs then
  io.stderr:write("listen failed: " .. tostring(listen_err) .. "\n")
  os.exit(1)
end

io.stdout:write("listening:\n")
for _, addr in ipairs(addrs) do
  io.stdout:write("  " .. addr .. "\n")
  io.stdout:write("  " .. addr .. "/p2p/" .. local_peer.id .. "\n")
end
io.stdout:write("peer id: " .. local_peer.id .. "\n")
io.stdout:write("running; Ctrl-C to stop\n")
io.stdout:flush()

local started, start_err = host:start({
  blocking = true,
  poll_interval = 0.01,
  accept_timeout = 0.05,
})
if not started then
  io.stderr:write("host stopped with error: " .. tostring(start_err) .. "\n")
  os.exit(1)
end
