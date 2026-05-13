package.path = table.concat({
  "./?.lua",
  "./?/init.lua",
  package.path,
}, ";")

local host_mod = require("lua_libp2p.host")
local identify_service = require("lua_libp2p.protocol_identify.service")
local mdns = require("lua_libp2p.peer_discovery_mdns")
local ping_service = require("lua_libp2p.protocol_ping.service")

local CHAT_PROTOCOL = "/mdns-chat/1.0.0"

local name = arg[1] or os.getenv("USER") or "lua"
local listen_addr = arg[2] or "/ip4/0.0.0.0/tcp/0"
local runtime = os.getenv("LUA_LIBP2P_RUNTIME") or "luv"

local ok_luv, uv = pcall(require, "luv")
if not ok_luv then
  io.stderr:write("mdns chat requires luv\n")
  os.exit(2)
end

local peers = {}
local peer_order = {}
local outbound = {}

local function now_time()
  return os.date("%H:%M:%S")
end

local function push_message(text)
  text = tostring(text or ""):gsub("\r", "")
  if text == "" then
    return
  end
  outbound[#outbound + 1] = text
end

local function encode_message(text)
  local payload = name .. ": " .. text
  if #payload > 65535 then
    payload = payload:sub(1, 65535)
  end
  return string.char((#payload >> 8) & 0xff, #payload & 0xff) .. payload
end

local function read_chat_message(stream)
  local len_bytes, len_err = stream:read(2)
  if not len_bytes then
    return nil, len_err
  end
  if #len_bytes < 2 then
    return nil, "short length prefix"
  end
  local a, b = len_bytes:byte(1, 2)
  local len = (a << 8) | b
  if len == 0 then
    return ""
  end
  return stream:read(len)
end

local function remember_peer(peer)
  if type(peer) ~= "table" or type(peer.peer_id) ~= "string" then
    return
  end
  if peer.peer_id == "" or type(peer.addrs) ~= "table" or #peer.addrs == 0 then
    return
  end
  if not peers[peer.peer_id] then
    peer_order[#peer_order + 1] = peer.peer_id
    io.stdout:write(string.format("%s discovered %s\n", now_time(), peer.peer_id))
    io.stdout:flush()
  end
  peers[peer.peer_id] = peer
end

local function send_to_peer(host, peer_id, text)
  local stream, selected, _, stream_err = host:new_stream(peer_id, { CHAT_PROTOCOL }, { timeout = 4, io_timeout = 4 })
  if not stream then
    local peer = peers[peer_id]
    if peer and peer.addrs and peer.addrs[1] then
      stream, selected, _, stream_err = host:new_stream(peer.addrs[1], { CHAT_PROTOCOL }, { timeout = 4, io_timeout = 4 })
    end
  end
  if not stream then
    io.stderr:write(string.format("send to %s failed: %s\n", peer_id, tostring(stream_err)))
    return
  end
  local ok, write_err = stream:write(encode_message(text))
  if not ok then
    io.stderr:write(string.format("write to %s failed: %s\n", peer_id, tostring(write_err)))
  end
  stream:close()
  if ok then
    io.stdout:write(string.format("%s sent to %s via %s\n", now_time(), peer_id, tostring(selected)))
    io.stdout:flush()
  end
end

local host, host_err = host_mod.new({
  runtime = runtime,
  listen_addrs = { listen_addr },
  services = {
    identify = { module = identify_service },
    ping = { module = ping_service },
    mdns = {
      module = mdns,
      config = {
        interval = 5,
      },
    },
  },
  blocking = false,
  accept_timeout = 0.05,
  on_started = function(h)
    io.stdout:write("mDNS chat started\n")
    io.stdout:write("name: " .. name .. "\n")
    io.stdout:write("peer id: " .. h:peer_id().id .. "\n")
    io.stdout:write("listening:\n")
    for _, addr in ipairs(h:get_multiaddrs()) do
      io.stdout:write("  " .. addr .. "\n")
    end
    io.stdout:write("type a line and press Enter to send to discovered peers; Ctrl-C to stop\n")
    io.stdout:flush()
  end,
})
if not host then
  io.stderr:write("host init failed: " .. tostring(host_err) .. "\n")
  os.exit(1)
end

local handle_ok, handle_err = host:handle(CHAT_PROTOCOL, function(stream)
  local message, read_err = read_chat_message(stream)
  if not message then
    io.stderr:write("chat read failed: " .. tostring(read_err) .. "\n")
    return
  end
  io.stdout:write(string.format("%s %s\n", now_time(), message))
  io.stdout:flush()
end)
if not handle_ok then
  io.stderr:write("chat handler failed: " .. tostring(handle_err) .. "\n")
  os.exit(1)
end

local discover_ok, discover_err = host:on("peer_discovered", function(payload)
  if payload and payload.source == "mdns" then
    remember_peer(payload)
  end
end)
if not discover_ok then
  io.stderr:write("peer discovery watch failed: " .. tostring(discover_err) .. "\n")
  os.exit(1)
end

local started, start_err = host:start()
if not started then
  io.stderr:write("host start failed: " .. tostring(start_err) .. "\n")
  os.exit(1)
end

host:spawn_task("example.mdns_chat.sender", function(ctx)
  while true do
    if #outbound > 0 then
      local text = table.remove(outbound, 1)
      if #peer_order == 0 then
        io.stdout:write("no mDNS peers discovered yet\n")
        io.stdout:flush()
      end
      for _, peer_id in ipairs(peer_order) do
        send_to_peer(host, peer_id, text)
      end
    end
    local slept, sleep_err = ctx:sleep(0.05)
    if slept == nil and sleep_err then
      return nil, sleep_err
    end
  end
end)

local stdin = uv.new_tty(0, true)
if stdin then
  local buffer = ""
  stdin:read_start(function(err, chunk)
    if err then
      io.stderr:write("stdin read failed: " .. tostring(err) .. "\n")
      return
    end
    if not chunk then
      return
    end
    buffer = buffer .. chunk
    while true do
      local line, rest = buffer:match("^(.-)\n(.*)$")
      if not line then
        break
      end
      buffer = rest
      push_message(line)
    end
  end)
end

while true do
  local slept, sleep_err = host:sleep(1)
  if slept == nil and sleep_err then
    io.stderr:write("host sleep failed: " .. tostring(sleep_err) .. "\n")
    os.exit(1)
  end
end
