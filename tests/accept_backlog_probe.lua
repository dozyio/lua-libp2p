package.path = table.concat({
  "./?.lua",
  "./?/init.lua",
  package.path,
}, ";")

local multiaddr = require("lua_libp2p.multiaddr")

local ok_socket, socket = pcall(require, "socket")
if not ok_socket then
  io.stderr:write("lua-socket is required\n")
  os.exit(2)
end

local target = arg[1]
local burst = tonumber(arg[2]) or tonumber(os.getenv("LUA_LIBP2P_ACCEPT_BACKLOG_BURST")) or 256
local hold_seconds = tonumber(arg[3]) or tonumber(os.getenv("LUA_LIBP2P_ACCEPT_BACKLOG_HOLD")) or 10

if not target then
  io.stderr:write("usage: lua tests/accept_backlog_probe.lua /ip4/127.0.0.1/tcp/4001 [burst] [hold_seconds]\n")
  os.exit(2)
end

local parsed, parse_err = multiaddr.parse(target)
if not parsed then
  io.stderr:write("invalid multiaddr: " .. tostring(parse_err) .. "\n")
  os.exit(2)
end

local endpoint = multiaddr.to_tcp_endpoint(parsed)
if not endpoint then
  io.stderr:write("target must be a tcp multiaddr\n")
  os.exit(2)
end

local sockets = {}
local ok_count = 0
local fail_count = 0
local fail_by_error = {}
local started = socket.gettime()

for _ = 1, burst do
  local sock, sock_err = socket.tcp()
  if not sock then
    fail_count = fail_count + 1
    fail_by_error[tostring(sock_err)] = (fail_by_error[tostring(sock_err)] or 0) + 1
    goto continue
  end
  sock:settimeout(2)
  local ok, connect_err = sock:connect(endpoint.host, endpoint.port)
  if ok then
    ok_count = ok_count + 1
    sock:settimeout(0)
    sockets[#sockets + 1] = sock
  else
    fail_count = fail_count + 1
    fail_by_error[tostring(connect_err)] = (fail_by_error[tostring(connect_err)] or 0) + 1
    pcall(function()
      sock:close()
    end)
  end
  ::continue::
end

local elapsed = socket.gettime() - started
io.stdout:write(
  string.format(
    "accept_backlog_probe target=%s burst=%d connected=%d failed=%d connect_seconds=%.3f hold_seconds=%.3f\n",
    target,
    burst,
    ok_count,
    fail_count,
    elapsed,
    hold_seconds
  )
)
for err, count in pairs(fail_by_error) do
  io.stdout:write(string.format("  error %s=%d\n", err, count))
end
io.stdout:flush()

local deadline = socket.gettime() + hold_seconds
while socket.gettime() < deadline do
  socket.sleep(math.min(0.25, deadline - socket.gettime()))
end

for _, sock in ipairs(sockets) do
  pcall(function()
    sock:close()
  end)
end

io.stdout:write("accept_backlog_probe closed=" .. tostring(#sockets) .. "\n")
