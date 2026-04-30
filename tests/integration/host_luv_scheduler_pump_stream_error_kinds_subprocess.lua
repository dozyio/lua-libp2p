local host_mod = require("lua_libp2p.host")
local child_scripts = require("tests.support.child_scripts")
local subprocess = require("tests.support.subprocess")
local tcp_luv = require("lua_libp2p.transport.tcp_luv")

local PROTO_ID = "/tests/timeout-close/1.0.0"

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
    accept_timeout = 0.05,
    io_timeout = 0.05,
  })
  if not host then
    return nil, host_err
  end

  local handled_stream = nil
  local ok_handle, handle_err = host:handle(PROTO_ID, function(stream)
    handled_stream = stream
    return true
  end)
  if not ok_handle then
    return nil, handle_err
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
  local child_phase = os.tmpname() .. ".phase"
  local child_log = os.tmpname() .. ".log"

  local wrote, write_err = subprocess.write_file(child_script, child_scripts.host_stream_error_kind_client())
  if not wrote then
    host:stop()
    return nil, write_err
  end

  local spawn_ok = subprocess.spawn_lua_background(child_script, {
    addrs[1],
    child_out,
    child_phase,
    PROTO_ID,
  }, child_log)
  if not spawn_ok then
    host:stop()
    subprocess.remove_files({ child_script, child_out, child_phase, child_log })
    return nil, "failed to spawn child stream error-kind client"
  end

  local completed = false
  local timeout_hit = false
  local stop_sent = false
  local timeout_timer

  local poll_timer = assert(uv.new_timer())
  poll_timer:start(10, 20, function()
    if not stop_sent and handled_stream ~= nil then
      stop_sent = true
      host:stop()
    end

    local content = subprocess.read_file(child_out)
    if content then
      completed = true
      poll_timer:stop()
      poll_timer:close()
      if timeout_timer then
        timeout_timer:stop()
        timeout_timer:close()
      end
    end
  end)

  timeout_timer = assert(uv.new_timer())
  timeout_timer:start(5000, 0, function()
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
  subprocess.remove_files({ child_script, child_out, child_phase, child_log })

  if timeout_hit then
    return nil, "timed out waiting for child stream error-kind client"
  end
  if not completed then
    return nil, "child stream error-kind client did not complete"
  end
  if handled_stream == nil then
    return nil, "expected host to handle inbound test stream"
  end
  if result ~= "ok" then
    return nil, "child stream error-kind client failed: " .. tostring(result or log_out)
  end

  return true
end

return {
  name = "host luv scheduler pump stream error kinds subprocess",
  run = run,
}
