package.path = table.concat({
  "./?.lua",
  "./?/init.lua",
  package.path,
}, ";")

local host_mod = require("lua_libp2p.host")

local ok_luv, uv = pcall(require, "luv")
if not ok_luv then
  io.stderr:write("idle cpu benchmark requires luv\n")
  os.exit(1)
end

local function usage_seconds()
  if type(uv.getrusage) ~= "function" then
    return os.clock(), 0
  end
  local usage = uv.getrusage()
  local function timeval_seconds(value)
    if type(value) ~= "table" then
      return 0
    end
    return (tonumber(value.sec) or 0) + ((tonumber(value.usec) or 0) / 1000000)
  end
  return timeval_seconds(usage.utime), timeval_seconds(usage.stime)
end

local function now_seconds()
  if type(uv.hrtime) == "function" then
    return uv.hrtime() / 1000000000
  end
  return os.clock()
end

local function wait_seconds(seconds)
  local done = false
  local timer = assert(uv.new_timer())
  assert(timer:start(math.max(1, math.floor(seconds * 1000)), 0, function()
    done = true
    timer:stop()
    timer:close()
  end))
  while not done do
    uv.run("once")
  end
end

local function new_host(listen_addrs)
  local h, err = host_mod.new({
    runtime = "luv",
    blocking = false,
    listen_addrs = listen_addrs,
  })
  if not h then
    return nil, err
  end
  local started, start_err = h:start()
  if not started then
    h:close()
    return nil, start_err
  end
  return h
end

local function run_case(name, listen_addrs, seconds)
  collectgarbage("collect")
  local h, err = new_host(listen_addrs)
  if not h then
    return nil, err
  end

  local user_before, sys_before = usage_seconds()
  local wall_before = now_seconds()
  wait_seconds(seconds)
  local wall_after = now_seconds()
  local user_after, sys_after = usage_seconds()

  h:close()

  local wall = wall_after - wall_before
  local user = user_after - user_before
  local sys = sys_after - sys_before
  local cpu = user + sys
  local cpu_percent = wall > 0 and (cpu / wall * 100) or 0
  io.stdout:write(string.format(
    "  %-18s wall_s=%.3f user_s=%.6f sys_s=%.6f cpu_s=%.6f cpu_pct=%.3f\n",
    name,
    wall,
    user,
    sys,
    cpu,
    cpu_percent
  ))
  return true
end

local seconds = tonumber(arg[1]) or tonumber(os.getenv("LUA_LIBP2P_IDLE_SECONDS")) or 30
if seconds <= 0 then
  io.stderr:write("duration must be positive\n")
  os.exit(1)
end

io.stdout:write(string.format("idle cpu benchmark duration_s=%.3f\n", seconds))

local ok, err = run_case("no_listeners", {}, seconds)
if not ok then
  io.stderr:write("no_listeners failed: " .. tostring(err) .. "\n")
  os.exit(1)
end

ok, err = run_case("tcp_listener", { "/ip4/127.0.0.1/tcp/0" }, seconds)
if not ok then
  io.stderr:write("tcp_listener failed: " .. tostring(err) .. "\n")
  os.exit(1)
end
