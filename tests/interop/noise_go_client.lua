package.path = table.concat({
  "./?.lua",
  "./?/init.lua",
  package.path,
}, ";")

local ed25519 = require("lua_libp2p.crypto.ed25519")
local noise = require("lua_libp2p.security.noise")
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

local addr = arg[1]
if not addr or addr == "" then
  io.stderr:write("usage: lua tests/interop/noise_go_client.lua <host:port>\n")
  os.exit(2)
end

local host, port = addr:match("^(.-):(%d+)$")
if not host then
  io.stderr:write("invalid address\n")
  os.exit(2)
end

local raw, dial_err = tcp.dial({ host = host, port = tonumber(port) }, { timeout = 2, io_timeout = 2 })
if not raw then
  io.stderr:write(tostring(dial_err) .. "\n")
  os.exit(1)
end

local identity = assert(ed25519.generate_keypair())
local secure, _, hs_err = noise.handshake_xx_outbound(raw, {
  identity_keypair = identity,
})
if not secure then
  io.stderr:write(tostring(hs_err) .. "\n")
  os.exit(1)
end

local ok, write_err = secure:write("hello")
if not ok then
  io.stderr:write(tostring(write_err) .. "\n")
  os.exit(1)
end

local msg, read_err = secure:read(5)
if not msg then
  io.stderr:write(tostring(read_err) .. "\n")
  os.exit(1)
end

if msg ~= "world" then
  io.stderr:write("unexpected response payload\n")
  os.exit(1)
end

secure:close()
print("ok")
