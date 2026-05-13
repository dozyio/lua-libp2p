package.path = table.concat({
  "./?.lua",
  "./?/init.lua",
  package.path,
}, ";")

local cqueues = require("cqueues")
local cq_socket = require("cqueues.socket")
local ed25519 = require("lua_libp2p.crypto.ed25519")
local peerid = require("lua_libp2p.peerid")
local tls = require("lua_libp2p.connection_encrypter_tls.protocol")

local listener, listen_err = cq_socket.listen({ host = "127.0.0.1", port = 0 }, { reuseaddr = true })
if not listener then
  io.stderr:write(tostring(listen_err) .. "\n")
  os.exit(1)
end

local _, host, port = listener:localname()
if not host or not port then
  io.stderr:write("failed to inspect cqueues listen socket\n")
  os.exit(1)
end

local identity = assert(ed25519.generate_keypair())
local pid = assert(peerid.from_ed25519_public_key(identity.public_key))

io.stdout:write(string.format("%s:%s\n", host, tostring(port)))
io.stdout:write(string.format("%s\n", pid.id))
io.stdout:flush()

local cq = cqueues.new()
local ok = false
local err_msg = nil

cq:wrap(function()
  local raw, accept_err = listener:accept(5)
  if not raw then
    err_msg = tostring(accept_err)
    return
  end
  local secure, _, hs_err = tls.handshake_inbound(raw, {
    identity_keypair = identity,
  })
  if not secure then
    err_msg = tostring(hs_err)
    return
  end

  local msg, read_err = secure:read(5)
  if not msg then
    err_msg = tostring(read_err)
    return
  end
  if msg ~= "hello" then
    err_msg = "unexpected request payload"
    return
  end
  local write_ok, write_err = secure:write("world")
  if not write_ok then
    err_msg = tostring(write_err)
    return
  end
  secure:close()
  listener:close()
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
