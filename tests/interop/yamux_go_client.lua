package.path = table.concat({
  "./?.lua",
  "./?/init.lua",
  package.path,
}, ";")

local runtime = os.getenv("LUA_LIBP2P_INTEROP_RUNTIME") or "luv"
local tcp
if runtime == "luv" then
  tcp = require("lua_libp2p.transport_tcp.luv")
else
  io.stderr:write("invalid LUA_LIBP2P_INTEROP_RUNTIME, expected 'luv'\n")
  os.exit(2)
end
local yamux = require("lua_libp2p.muxer.yamux")

local addr = arg[1]
if not addr or addr == "" then
  io.stderr:write("missing addr\n")
  os.exit(2)
end

local ma = "/ip4/" .. addr:match("^(.-):") .. "/tcp/" .. addr:match(":(%d+)$")
local conn, err = tcp.dial(ma, { timeout = 2, io_timeout = 2 })
if not conn then
  io.stderr:write(tostring(err) .. "\n")
  os.exit(1)
end

local session = yamux.new_session(conn, { is_client = true })
local stream, open_err = session:open_stream()
if not stream then
  io.stderr:write(tostring(open_err) .. "\n")
  os.exit(1)
end

for _ = 1, 4 do
  local _, p_err = session:process_one()
  if p_err then
    io.stderr:write(tostring(p_err) .. "\n")
    os.exit(1)
  end
  if stream.acked then
    break
  end
end

local ok, write_err = stream:write("hello")
if not ok then
  io.stderr:write(tostring(write_err) .. "\n")
  os.exit(1)
end

local got
for _ = 1, 8 do
  local _, p_err = session:process_one()
  if p_err then
    io.stderr:write(tostring(p_err) .. "\n")
    os.exit(1)
  end
  got = stream:read_now()
  if got then
    break
  end
end

if got ~= "world" then
  io.stderr:write("unexpected response\n")
  os.exit(1)
end

conn:close()
print("ok")
