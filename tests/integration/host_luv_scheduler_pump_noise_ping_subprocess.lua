local host_mod = require("lua_libp2p.host")
local identify_service = require("lua_libp2p.protocol_identify.service")
local ping_service = require("lua_libp2p.protocol_ping.service")
local child_scripts = require("tests.support.child_scripts")
local subprocess = require("tests.support.subprocess")
local tcp_luv = require("lua_libp2p.transport_tcp.luv")

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
    services = {
      identify = identify_service,
      ping = ping_service,
    },
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

  return subprocess.run_luv_child_file_case({
    uv = uv,
    host = host,
    child_source = child_scripts.host_noise_ping_client(),
    child_args = { addrs[1] },
    timeout_ms = 4000,
    spawn_error = "failed to spawn child noise ping client",
    timeout_error = "timed out waiting for child noise ping client",
    incomplete_error = "child noise ping client did not complete",
    expected_result = "ok",
    result_error_prefix = "child noise ping failed: ",
  })
end

return {
  name = "host luv scheduler pump noise ping subprocess",
  run = run,
}
