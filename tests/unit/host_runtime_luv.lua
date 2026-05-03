local host_mod = require("lua_libp2p.host")

local function run()
  local has_luv = pcall(require, "luv")

  local h, h_err = host_mod.new({
    runtime = "luv",
    blocking = false,
    listen_addrs = { "/ip4/127.0.0.1/tcp/0" },
  })
  if not h then
    return nil, h_err
  end

  local started, start_err = h:start()
  if has_luv then
    if not started then
      return nil, start_err
    end

    if not h._tcp_transport then
      return nil, "expected runtime=luv host to select luv tcp transport backend"
    end
    if h._tcp_transport.BACKEND ~= "luv-native" then
      return nil, "unexpected runtime=luv tcp transport backend marker"
    end

    if h._luv_tick_timer == nil then
      return nil, "expected luv runtime to create scheduler timer"
    end

    local sub = assert(h:subscribe("host_runtime_error"))
    h:_set_runtime_error("luv", "forced runtime error")
    local ev, ev_err = h:next_event(sub)
    if ev_err then
      return nil, ev_err
    end
    if not ev or ev.name ~= "host_runtime_error" then
      return nil, "expected host_runtime_error event from runtime failure"
    end
    if not ev.payload or ev.payload.runtime ~= "luv" then
      return nil, "expected host_runtime_error payload runtime"
    end
    h._running = true

    local stopped, stop_err = h:stop()
    if not stopped then
      return nil, stop_err
    end
    if h._luv_tick_timer ~= nil then
      return nil, "expected stop to close luv scheduler timer"
    end
    return true
  end

  if started ~= nil then
    return nil, "expected runtime=luv to fail when luv is unavailable"
  end
  if not start_err then
    return nil, "expected start error when luv is unavailable"
  end

  return true
end

return {
  name = "host runtime luv backend",
  run = run,
}
