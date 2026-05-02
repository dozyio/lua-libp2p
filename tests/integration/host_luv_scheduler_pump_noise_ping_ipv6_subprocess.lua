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
    listen_addrs = { "/ip6/::1/tcp/0" },
    services = {
      identify = identify_service,
      ping = ping_service,
    },
    accept_timeout = 0.05,
  })
  if not host then
    if tostring(host_err):find("unsupported", 1, true) or tostring(host_err):find("ip6", 1, true) then
      return true
    end
    return nil, host_err
  end

  local started, start_err = host:start()
  if not started then
    host:stop()
    if tostring(start_err):find("unsupported", 1, true) or tostring(start_err):find("ip6", 1, true) then
      return true
    end
    return nil, start_err
  end

  local addrs = host:get_multiaddrs()
  if #addrs == 0 then
    host:stop()
    return nil, "expected luv host ipv6 listener address"
  end

  local target
  for _, addr in ipairs(addrs) do
    if tostring(addr):find("/ip6/", 1, true) then
      target = addr
      break
    end
  end
  if not target then
    host:stop()
    return nil, "expected ipv6 listener multiaddr"
  end

  return subprocess.run_luv_child_file_case({
    uv = uv,
    host = host,
    child_source = child_scripts.host_noise_ping_client(),
    child_args = { target },
    timeout_ms = 5000,
    spawn_error = "failed to spawn child noise ping ipv6 client",
    timeout_error = "timed out waiting for child noise ping ipv6 client",
    incomplete_error = "child noise ping ipv6 client did not complete",
    expected_result = "ok",
    result_error_prefix = "child noise ping ipv6 failed: ",
  })
end

return {
  name = "host luv scheduler pump noise ping ipv6 subprocess",
  run = run,
}
