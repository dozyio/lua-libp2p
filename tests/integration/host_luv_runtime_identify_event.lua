local host_mod = require("lua_libp2p.host")
local tcp_luv = require("lua_libp2p.transport.tcp_luv")

local function shell_quote(text)
  return "'" .. tostring(text):gsub("'", "'\\''") .. "'"
end

local function write_file(path, content)
  local f, err = io.open(path, "wb")
  if not f then
    return nil, err
  end
  local ok, write_err = f:write(content)
  f:close()
  if not ok then
    return nil, write_err
  end
  return true
end

local function read_file(path)
  local f = io.open(path, "rb")
  if not f then
    return nil
  end
  local data = f:read("*a")
  f:close()
  return data
end

local function run()
  local ok_luv, uv = pcall(require, "luv")
  if not ok_luv then
    return true
  end
  if tcp_luv.BACKEND ~= "luv-native" then
    return true
  end

  local host, host_err = host_mod.new({
    runtime = "luv",
    blocking = false,
    listen_addrs = { "/ip4/127.0.0.1/tcp/0" },
    services = { "identify" },
    accept_timeout = 0.05,
  })
  if not host then
    return nil, host_err
  end

  local started, start_err = host:start()
  if not started then
    return nil, start_err
  end

  local addrs = host:get_multiaddrs()
  if #addrs == 0 then
    host:stop()
    return nil, "expected luv host listener address"
  end

  local sub = assert(host:subscribe("peer_identified"))

  local child_script = os.tmpname() .. ".lua"
  local child_out = os.tmpname() .. ".txt"
  local child_log = os.tmpname() .. ".log"

  local child_source = [[
package.path = "./?.lua;./?/init.lua;" .. package.path
local host = require("lua_libp2p.host")

local addr = arg[1]
local out_path = arg[2]

local function write_out(text)
  local f = assert(io.open(out_path, "wb"))
  f:write(text)
  f:close()
end

local h, h_err = host.new({
  services = { "identify" },
  blocking = false,
  accept_timeout = 0.05,
})
if not h then
  write_out("init_error:" .. tostring(h_err))
  os.exit(1)
end

local started, start_err = h:start()
if not started then
  write_out("start_error:" .. tostring(start_err))
  os.exit(1)
end

local _, _, dial_err = h:dial(addr, {
  timeout = 2,
  io_timeout = 2,
})
if dial_err then
  write_out("dial_error:" .. tostring(dial_err))
  os.exit(1)
end

for _ = 1, 200 do
  local ok, poll_err = h:poll_once(0)
  if not ok then
    write_out("poll_error:" .. tostring(poll_err))
    os.exit(1)
  end
end

h:stop()
write_out("ok")
]]

  local wrote, write_err = write_file(child_script, child_source)
  if not wrote then
    host:stop()
    return nil, write_err
  end

  local spawn_cmd = "lua "
    .. shell_quote(child_script)
    .. " "
    .. shell_quote(addrs[1])
    .. " "
    .. shell_quote(child_out)
    .. " >"
    .. shell_quote(child_log)
    .. " 2>&1 &"

  local spawn_ok = os.execute(spawn_cmd)
  if spawn_ok ~= true and spawn_ok ~= 0 then
    host:stop()
    os.remove(child_script)
    os.remove(child_out)
    os.remove(child_log)
    return nil, "failed to spawn child identify client"
  end

  local identified = false
  local child_done = false
  local timeout_hit = false
  local timeout_timer

  local poll_timer = assert(uv.new_timer())
  poll_timer:start(10, 20, function()
    while true do
      local ev = host:next_event(sub)
      if not ev then
        break
      end
      if ev.name == "peer_identified" then
        identified = true
      end
    end

    local content = read_file(child_out)
    if content then
      child_done = true
    end

    if identified and child_done then
      poll_timer:stop()
      poll_timer:close()
      if timeout_timer then
        timeout_timer:stop()
        timeout_timer:close()
      end
      host:stop()
    end
  end)

  timeout_timer = assert(uv.new_timer())
  timeout_timer:start(4000, 0, function()
    timeout_hit = true
    timeout_timer:stop()
    timeout_timer:close()
    pcall(function()
      poll_timer:stop()
      poll_timer:close()
    end)
    host:stop()
  end)

  uv.run("default")

  local child_result = read_file(child_out)
  local child_stderr = read_file(child_log)

  os.remove(child_script)
  os.remove(child_out)
  os.remove(child_log)

  if timeout_hit then
    return nil, "timed out waiting for peer_identified + child completion"
  end
  if not identified then
    return nil, "expected peer_identified event under luv runtime"
  end
  if child_result ~= "ok" then
    return nil, "child identify client failed: " .. tostring(child_result or child_stderr)
  end

  return true
end

return {
  name = "host luv runtime identify event subprocess",
  run = run,
}
