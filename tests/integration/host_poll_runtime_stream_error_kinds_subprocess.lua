local host_mod = require("lua_libp2p.host")
local child_scripts = require("tests.support.child_scripts")
local subprocess = require("tests.support.subprocess")

local PROTO_ID = "/tests/timeout-close/1.0.0"

local function sleep_seconds(seconds)
  local ok_socket, socket = pcall(require, "socket")
  if ok_socket and socket and type(socket.sleep) == "function" then
    socket.sleep(seconds)
    return
  end

  local start = os.clock()
  while os.clock() - start < seconds do
  end
end

local function run()
  local host, host_err = host_mod.new({
    runtime = "poll",
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
    return nil, "expected poll host listener address"
  end

  local child_script = os.tmpname() .. ".lua"
  local child_out = os.tmpname() .. ".txt"
  local child_phase = os.tmpname() .. ".phase"
  local child_log = os.tmpname() .. ".log"

  local child_source = child_scripts.host_stream_error_kind_client()

  local wrote, write_err = subprocess.write_file(child_script, child_source)
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

  local deadline = os.time() + 6
  local completed = false
  local stop_sent = false

  while os.time() <= deadline do
    local _, poll_err = host:poll_once(0.01)
    if poll_err then
      host:stop()
      subprocess.remove_files({ child_script, child_out, child_phase, child_log })
      return nil, poll_err
    end

    if not stop_sent then
      local phase = subprocess.read_file(child_phase)
      if phase == "timeout_ok" then
        stop_sent = true
        host:stop()
      end
    end

    local content = subprocess.read_file(child_out)
    if content then
      completed = true
      break
    end

    sleep_seconds(0.01)
  end

  local result = subprocess.read_file(child_out)
  local log_out = subprocess.read_file(child_log)

  subprocess.remove_files({ child_script, child_out, child_phase, child_log })

  if not completed then
    return nil, "timed out waiting for child stream error-kind client"
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
  name = "host poll runtime stream error kinds subprocess",
  run = run,
}
