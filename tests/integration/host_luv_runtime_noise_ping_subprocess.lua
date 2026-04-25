local host_mod = require("lua_libp2p.host")
local child_scripts = require("tests.support.child_scripts")
local subprocess = require("tests.support.subprocess")
local tcp_luv = require("lua_libp2p.transport.tcp_luv")

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
    services = { "identify", "ping" },
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

  local child_script = os.tmpname() .. ".lua"
  local child_out = os.tmpname() .. ".txt"
  local child_log = os.tmpname() .. ".log"

  local child_source = child_scripts.host_noise_ping_client()

  local wrote, write_err = subprocess.write_file(child_script, child_source)
  if not wrote then
    host:stop()
    return nil, write_err
  end

  local spawn_ok = subprocess.spawn_lua_background(child_script, {
    addrs[1],
    child_out,
  }, child_log)
  if not spawn_ok then
    host:stop()
    subprocess.remove_files({ child_script, child_out, child_log })
    return nil, "failed to spawn child noise ping client"
  end

  local completed = false
  local timeout_hit = false

  local poll_timer = assert(uv.new_timer())
  poll_timer:start(10, 20, function()
    local content = subprocess.read_file(child_out)
    if content then
      completed = true
      poll_timer:stop()
      poll_timer:close()
      host:stop()
    end
  end)

  local timeout_timer = assert(uv.new_timer())
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

  local result = subprocess.read_file(child_out)
  local log_out = subprocess.read_file(child_log)

  subprocess.remove_files({ child_script, child_out, child_log })

  if timeout_hit then
    return nil, "timed out waiting for child noise ping client"
  end
  if not completed then
    return nil, "child noise ping client did not complete"
  end
  if result ~= "ok" then
    return nil, "child noise ping failed: " .. tostring(result or log_out)
  end

  return true
end

return {
  name = "host luv runtime noise ping subprocess",
  run = run,
}
