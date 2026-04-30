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
    services = { "identify", "ping", "kad_dht", "autorelay" },
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

  local identify_sub = assert(host:subscribe("peer_identified"))
  local child_count = 3
  local script_path = os.tmpname() .. ".lua"
  local files = { script_path }
  local wrote, write_err = subprocess.write_file(script_path, child_scripts.host_identify_ping_burst_client())
  if not wrote then
    host:stop()
    return nil, write_err
  end

  local out_paths = {}
  local log_paths = {}
  for i = 1, child_count do
    local out_path = os.tmpname() .. ".txt"
    local log_path = os.tmpname() .. ".log"
    out_paths[i] = out_path
    log_paths[i] = log_path
    files[#files + 1] = out_path
    files[#files + 1] = log_path
    local spawn_ok = subprocess.spawn_lua_background(script_path, { addrs[1], out_path, "5" }, log_path)
    if not spawn_ok then
      host:stop()
      subprocess.remove_files(files)
      return nil, "failed to spawn identify/ping pressure child"
    end
  end

  local identified_count = 0
  local done = false
  local timeout_hit = false
  local timeout_timer
  local poll_timer = assert(uv.new_timer())
  poll_timer:start(10, 20, function()
    while true do
      local ev = host:next_event(identify_sub)
      if not ev then
        break
      end
      if ev.name == "peer_identified" then
        identified_count = identified_count + 1
      end
    end

    local completed = 0
    for i = 1, child_count do
      if subprocess.read_file(out_paths[i]) then
        completed = completed + 1
      end
    end

    if completed == child_count then
      done = true
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
  timeout_timer:start(7000, 0, function()
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

  local first_error = nil
  for i = 1, child_count do
    local result = subprocess.read_file(out_paths[i])
    local log_out = subprocess.read_file(log_paths[i])
    if result ~= "ok" and first_error == nil then
      first_error = "child pressure client failed: " .. tostring(result or log_out)
    end
  end
  subprocess.remove_files(files)

  if timeout_hit then
    return nil, "timed out waiting for identify/autorelay/dht pressure case"
  end
  if not done then
    return nil, "identify/autorelay/dht pressure case did not complete"
  end
  if first_error then
    return nil, first_error
  end

  return true
end

return {
  name = "host luv scheduler identify autorelay dht pressure",
  run = run,
}
