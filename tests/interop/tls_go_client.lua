package.path = table.concat({
  "./?.lua",
  "./?/init.lua",
  package.path,
}, ";")

local cqueues = require("cqueues")
local cq_socket = require("cqueues.socket")
local ed25519 = require("lua_libp2p.crypto.ed25519")
local tls = require("lua_libp2p.connection_encrypter_tls.protocol")

local addr = arg[1]
if not addr or addr == "" then
  io.stderr:write("usage: lua tests/interop/tls_go_client.lua <host:port>\n")
  os.exit(2)
end

local host, port = addr:match("^(.-):(%d+)$")
if not host then
  io.stderr:write("invalid address\n")
  os.exit(2)
end

local identity = assert(ed25519.generate_keypair())
local cq = cqueues.new()
local ok = false
local err_msg = nil

cq:wrap(function()
  local raw, dial_err = cq_socket.connect({ host = host, port = tonumber(port) })
  if not raw then
    err_msg = tostring(dial_err)
    return
  end
  local secure, _, hs_err = tls.handshake_outbound(raw, {
    identity_keypair = identity,
  })
  if not secure then
    err_msg = tostring(hs_err)
    return
  end

  local write_ok, write_err = secure:write("hello")
  if not write_ok then
    err_msg = tostring(write_err)
    return
  end
  local msg, read_err = secure:read(5)
  if not msg then
    err_msg = tostring(read_err)
    return
  end
  if msg ~= "world" then
    err_msg = "unexpected response payload"
    return
  end
  secure:close()
  ok = true
end)

local loop_ok, loop_err = cq:loop()
if not loop_ok then
  io.stderr:write(tostring(loop_err) .. "\n")
  os.exit(1)
end
if not ok then
  io.stderr:write(tostring(err_msg or "tls interop failed") .. "\n")
  os.exit(1)
end

print("ok")
