package.path = table.concat({
  "./?.lua",
  "./?/init.lua",
  package.path,
}, ";")

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
local yamux = require("lua_libp2p.muxer.yamux")

local listener, listen_err = tcp.listen({
  host = "127.0.0.1",
  port = 0,
  accept_timeout = 2,
  io_timeout = 2,
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
  io.stderr:write("failed to parse listener multiaddr\n")
  os.exit(1)
end

io.stdout:write(string.format("%s:%s\n", host, port))
io.stdout:flush()

local conn, accept_err = listener:accept(2)
if not conn then
  io.stderr:write(tostring(accept_err) .. "\n")
  os.exit(1)
end

local session = yamux.new_session(conn, { is_client = false })

local stream = nil
for _ = 1, 64 do
  local _, process_err = session:process_one()
  if process_err then
    io.stderr:write(tostring(process_err) .. "\n")
    os.exit(1)
  end
  stream = session:accept_stream_now()
  if stream then
    break
  end
end

if not stream then
  io.stderr:write("no inbound yamux stream accepted\n")
  os.exit(1)
end

local payload = nil
for _ = 1, 64 do
  local got = stream:read_now()
  if got then
    payload = got
    break
  end
  local _, process_err = session:process_one()
  if process_err then
    io.stderr:write(tostring(process_err) .. "\n")
    os.exit(1)
  end
end

if payload ~= "hello" then
  io.stderr:write("unexpected request payload\n")
  os.exit(1)
end

local ok, write_err = stream:write("world")
if not ok then
  io.stderr:write(tostring(write_err) .. "\n")
  os.exit(1)
end

conn:close()
listener:close()
io.stdout:write("ok\n")
