local tcp_luv = require("lua_libp2p.transport.tcp_luv")

local function run()
  if tcp_luv.BACKEND ~= "luv-native" then
    return true
  end

  -- This subprocess path verifies the native luv host can resume inbound
  -- upgrade and stream negotiation coroutines while the uv loop is running.
  local ping_subprocess = require("tests.integration.host_luv_runtime_ping_subprocess")
  return ping_subprocess.run()
end

return {
  name = "host luv native coroutine-resumed ping",
  run = run,
}
