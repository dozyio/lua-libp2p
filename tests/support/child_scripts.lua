local M = {}

function M.host_stream_error_kind_client()
  return [[
package.path = "./?.lua;./?/init.lua;" .. package.path
local host = require("lua_libp2p.host")

local addr = arg[1]
local out_path = arg[2]
local phase_path = arg[3]
local proto_id = arg[4]

local function write_file(path, text)
  local f = assert(io.open(path, "wb"))
  f:write(text)
  f:close()
end

local function fail(prefix, err)
  write_file(out_path, prefix .. ":" .. tostring(err))
  os.exit(1)
end

local h, h_err = host.new({ blocking = false })
if not h then
  fail("init_error", h_err)
end

local stream, _, _, _, stream_err = h:new_stream(addr, { proto_id }, {
  timeout = 3,
  io_timeout = 0.05,
})
if not stream then
  fail("stream_error", stream_err)
end

local _, first_read_err = stream:read(1)
if not first_read_err or first_read_err.kind ~= "timeout" then
  fail("first_read_kind", first_read_err and first_read_err.kind)
end

write_file(phase_path, "timeout_ok")

for _ = 1, 120 do
  local _, err = stream:read(1)
  if err and err.kind == "closed" then
    if type(stream.close) == "function" then
      stream:close()
    end
    h:close()
    write_file(out_path, "ok")
    os.exit(0)
  end
  if err and err.kind ~= "timeout" then
    fail("closed_wait_kind", err.kind)
  end
end

if type(stream.close) == "function" then
  stream:close()
end
h:close()
write_file(out_path, "closed_timeout")
os.exit(1)
]]
end

function M.host_ping_client()
  return [[
package.path = "./?.lua;./?/init.lua;" .. package.path
local host = require("lua_libp2p.host")
local ping = require("lua_libp2p.protocol.ping")

local addr = arg[1]
local out_path = arg[2]

local function write_out(text)
  local f = assert(io.open(out_path, "wb"))
  f:write(text)
  f:close()
end

local h, h_err = host.new({ blocking = false })
if not h then
  write_out("init_error:" .. tostring(h_err))
  os.exit(1)
end

local stream, _, _, _, stream_err = h:new_stream(addr, { ping.ID }, {
  timeout = 2,
  io_timeout = 2,
})
if not stream then
  write_out("stream_error:" .. tostring(stream_err))
  os.exit(1)
end

local pong, ping_err = ping.ping_once(stream)
if not pong then
  write_out("ping_error:" .. tostring(ping_err))
  os.exit(1)
end

if type(stream.close) == "function" then
  stream:close()
end
h:close()
write_out("ok")
]]
end

function M.host_noise_ping_client()
  return [[
package.path = "./?.lua;./?/init.lua;" .. package.path
local host = require("lua_libp2p.host")
local ping = require("lua_libp2p.protocol.ping")

local addr = arg[1]
local out_path = arg[2]

local function write_out(text)
  local f = assert(io.open(out_path, "wb"))
  f:write(text)
  f:close()
end

local h, h_err = host.new({
  blocking = false,
  services = { "identify" },
})
if not h then
  write_out("init_error:" .. tostring(h_err))
  os.exit(1)
end

local stream, _, _, _, stream_err = h:new_stream(addr, { ping.ID }, {
  timeout = 3,
  io_timeout = 3,
})
if not stream then
  write_out("stream_error:" .. tostring(stream_err))
  os.exit(1)
end

local pong, ping_err = ping.ping_once(stream)
if not pong then
  write_out("ping_error:" .. tostring(ping_err))
  os.exit(1)
end

if type(stream.close) == "function" then
  stream:close()
end
h:close()
write_out("ok")
]]
end

return M
