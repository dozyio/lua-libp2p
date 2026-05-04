local host_mod = require("lua_libp2p.host")

local function run()
  local default_host, default_host_err = host_mod.new({
    listen_addrs = {},
  })
  if not default_host then
    return nil, default_host_err
  end
  local default_stats = default_host:stats()
  if not default_stats.resources or default_stats.resources.limits.streams ~= 2048 then
    return nil, "expected host to install default resource limits"
  end

  local h, host_err = host_mod.new({
    listen_addrs = {},
    resource_manager = {
      streams = 0,
    },
  })
  if not h then
    return nil, host_err
  end

  h.dial = function()
    return {
      new_stream = function()
        return {
          close = function()
            return true
          end,
        }, "/test/1.0.0"
      end,
    }, {
      direction = "outbound",
      remote_peer_id = "peer-a",
    }
  end

  local stream, _, _, stream_err = h:new_stream("peer-a", { "/test/1.0.0" })
  if stream then
    return nil, "expected resource manager to block outbound stream"
  end
  if not stream_err or stream_err.kind ~= "resource" then
    return nil, "expected outbound stream resource error"
  end

  local stats = h:stats()
  if not stats.resources or stats.resources.system.streams ~= 0 then
    return nil, "expected resource stats in host stats"
  end

  return true
end

return {
  name = "host resource manager integration",
  run = run,
}
