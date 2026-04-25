package.path = table.concat({
  "./?.lua",
  "./?/init.lua",
  package.path,
}, ";")

local ed25519 = require("lua_libp2p.crypto.ed25519")
local noise = require("lua_libp2p.security.noise")
local peerid = require("lua_libp2p.peerid")
local runtime = os.getenv("LUA_LIBP2P_INTEROP_RUNTIME") or "poll"
local tcp
if runtime == "luv" then
  tcp = require("lua_libp2p.transport.tcp_luv")
elseif runtime == "poll" then
  tcp = require("lua_libp2p.transport.tcp")
else
  io.stderr:write("invalid LUA_LIBP2P_INTEROP_RUNTIME, expected 'luv' or 'poll'\n")
  os.exit(2)
end

local listener, listen_err = tcp.listen({
  host = "127.0.0.1",
  port = 0,
  accept_timeout = 3,
  io_timeout = 3,
})
if not listener then
  io.stderr:write(tostring(listen_err) .. "\n")
  os.exit(1)
end

local ma, ma_err = listener:multiaddr()
if not ma then
  io.stderr:write(tostring(ma_err) .. "\n")
  os.exit(1)
end
local host, port = ma:match("^/ip4/([^/]+)/tcp/(%d+)$")
if not host then
  io.stderr:write("failed to parse listen multiaddr\n")
  os.exit(1)
end

local identity = assert(ed25519.generate_keypair())
local pid = assert(peerid.from_ed25519_public_key(identity.public_key))

io.stdout:write(string.format("%s:%s\n", host, port))
io.stdout:write(string.format("%s\n", pid.id))
io.stdout:flush()

local raw, accept_err = listener:accept(3)
if not raw then
  io.stderr:write(tostring(accept_err) .. "\n")
  os.exit(1)
end

local secure, _, hs_err = noise.handshake_xx_inbound(raw, {
  identity_keypair = identity,
})
if not secure then
  io.stderr:write(tostring(hs_err) .. "\n")
  os.exit(1)
end

local msg, read_err = secure:read(5)
if not msg then
  io.stderr:write(tostring(read_err) .. "\n")
  os.exit(1)
end
if msg ~= "hello" then
  io.stderr:write("unexpected request payload\n")
  os.exit(1)
end

local ok, write_err = secure:write("world")
if not ok then
  io.stderr:write(tostring(write_err) .. "\n")
  os.exit(1)
end

secure:close()
listener:close()
print("ok")
