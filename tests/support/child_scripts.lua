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

local h, h_err = host.new({ runtime = "poll", blocking = false })
if not h then
  fail("init_error", h_err)
end

local task, task_err = h:spawn_task("child.stream_error_kind", function(ctx)
  local stream, _, _, stream_err = h:new_stream(addr, { proto_id }, {
    timeout = 3,
    io_timeout = 0.05,
    ctx = ctx,
  })
  if not stream then
    return nil, "stream_error:" .. tostring(stream_err)
  end

  for _ = 1, 240 do
    local _, err = stream:read(1)
    if err and err.kind == "closed" then
      if type(stream.close) == "function" then
        stream:close()
      end
      return true
    end
    if err and err.kind ~= "timeout" and err.kind ~= "busy" then
      return nil, "closed_wait_kind:" .. tostring(err.kind)
    end
  end

  if type(stream.close) == "function" then
    stream:close()
  end
  return nil, "closed_timeout"
end, { service = "test" })
if not task then
  fail("task_error", task_err)
end

local ok, run_err = h:run_until_task(task, { timeout = 6, poll_interval = 0 })
h:close()
if not ok then
  write_file(out_path, tostring(run_err))
  os.exit(1)
end
write_file(out_path, "ok")
os.exit(0)
]]
end

function M.host_ping_client()
  return [[
package.path = "./?.lua;./?/init.lua;" .. package.path
local host = require("lua_libp2p.host")
local ping = require("lua_libp2p.protocol_ping.protocol")
local identify_service = require("lua_libp2p.protocol_identify.service")

local addr = arg[1]
local out_path = arg[2]

local function write_out(text)
  local f = assert(io.open(out_path, "wb"))
  f:write(text)
  f:close()
end

local h, h_err = host.new({ runtime = "poll", blocking = false })
if not h then
  write_out("init_error:" .. tostring(h_err))
  os.exit(1)
end

local task, task_err = h:spawn_task("child.ping", function(ctx)
  local stream, _, _, stream_err = h:new_stream(addr, { ping.ID }, {
    timeout = 2,
    io_timeout = 2,
    ctx = ctx,
  })
  if not stream then
    return nil, "stream_error:" .. tostring(stream_err)
  end

  local pong, ping_err = ping.ping_once(stream)
  if type(stream.close) == "function" then
    stream:close()
  end
  if not pong then
    return nil, "ping_error:" .. tostring(ping_err)
  end
  return true
end, { service = "test" })
if not task then
  write_out("task_error:" .. tostring(task_err))
  os.exit(1)
end
local ok, run_err = h:run_until_task(task, { timeout = 3, poll_interval = 0 })
if not ok then
  write_out(tostring(run_err))
  os.exit(1)
end

h:close()
write_out("ok")
]]
end

function M.host_noise_ping_client()
  return [[
package.path = "./?.lua;./?/init.lua;" .. package.path
local host = require("lua_libp2p.host")
local ping = require("lua_libp2p.protocol_ping.protocol")

local addr = arg[1]
local out_path = arg[2]

local function write_out(text)
  local f = assert(io.open(out_path, "wb"))
  f:write(text)
  f:close()
end

local h, h_err = host.new({
  runtime = "poll",
  blocking = false,
  services = {
    identify = identify_service,
  },
})
if not h then
  write_out("init_error:" .. tostring(h_err))
  os.exit(1)
end

local task, task_err = h:spawn_task("child.noise_ping", function(ctx)
  local stream, _, _, stream_err = h:new_stream(addr, { ping.ID }, {
    timeout = 3,
    io_timeout = 3,
    ctx = ctx,
  })
  if not stream then
    return nil, "stream_error:" .. tostring(stream_err)
  end

  local pong, ping_err = ping.ping_once(stream)
  if type(stream.close) == "function" then
    stream:close()
  end
  if not pong then
    return nil, "ping_error:" .. tostring(ping_err)
  end
  return true
end, { service = "test" })
if not task then
  write_out("task_error:" .. tostring(task_err))
  os.exit(1)
end
local ok, run_err = h:run_until_task(task, { timeout = 4, poll_interval = 0 })
if not ok then
  write_out(tostring(run_err))
  os.exit(1)
end

h:close()
write_out("ok")
]]
end

function M.host_identify_ping_burst_client()
  return [[
package.path = "./?.lua;./?/init.lua;" .. package.path
local host = require("lua_libp2p.host")
local identify = require("lua_libp2p.protocol_identify.protocol")
local ping = require("lua_libp2p.protocol_ping.protocol")
local socket = require("socket")
local identify_service = require("lua_libp2p.protocol_identify.service")
local ping_service = require("lua_libp2p.protocol_ping.service")
local kad_dht_service = require("lua_libp2p.kad_dht")

local addr = arg[1]
local out_path = arg[2]
local rounds = tonumber(arg[3] or "3") or 3

local function write_out(text)
  local f = assert(io.open(out_path, "wb"))
  f:write(text)
  f:close()
end

local h, h_err = host.new({
  runtime = "poll",
  blocking = false,
  services = {
    identify = identify_service,
    ping = ping_service,
    kad_dht = kad_dht_service,
  },
})
if not h then
  write_out("init_error:" .. tostring(h_err))
  os.exit(1)
end

local function run_stream_task(task_name, fn)
  local task, task_err = h:spawn_task(task_name, fn, { service = "test" })
  if not task then
    write_out("task_spawn_error:" .. tostring(task_err))
    os.exit(1)
  end
  local ok, err = h:run_until_task(task, { timeout = 3, poll_interval = 0 })
  if not ok then
    write_out("task_run_error:" .. tostring(err))
    os.exit(1)
  end
end

for _ = 1, rounds do
  run_stream_task("child.identify", function(ctx)
    local id_stream, _, _, id_err = h:new_stream(addr, { identify.ID }, {
      timeout = 2,
      io_timeout = 2,
      ctx = ctx,
    })
    if not id_stream then
      return nil, "identify_stream_error:" .. tostring(id_err)
    end
    local _, read_err = identify.read(id_stream)
    if type(id_stream.close) == "function" then
      id_stream:close()
    end
    if read_err then
      return nil, "identify_read_error:" .. tostring(read_err)
    end
    return true
  end)

  run_stream_task("child.ping", function(ctx)
    local ping_stream, _, _, ping_stream_err = h:new_stream(addr, { ping.ID }, {
      timeout = 2,
      io_timeout = 2,
      ctx = ctx,
    })
    if not ping_stream then
      return nil, "ping_stream_error:" .. tostring(ping_stream_err)
    end
    local _, ping_err = ping.ping_once(ping_stream)
    if type(ping_stream.close) == "function" then
      ping_stream:close()
    end
    if ping_err then
      return nil, "ping_error:" .. tostring(ping_err)
    end
    return true
  end)

  for _ = 1, 20 do
    local ok, poll_err = h:poll_once(0)
    if not ok then
      write_out("poll_error:" .. tostring(poll_err))
      os.exit(1)
    end
    socket.sleep(0.005)
  end
end

for _ = 1, 40 do
  local ok, poll_err = h:poll_once(0)
  if not ok then
    write_out("poll_error:" .. tostring(poll_err))
    os.exit(1)
  end
  socket.sleep(0.005)
end

h:close()
write_out("ok")
]]
end

return M
