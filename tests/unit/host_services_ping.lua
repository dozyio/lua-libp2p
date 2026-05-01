local host_mod = require("lua_libp2p.host")
local ping = require("lua_libp2p.protocol_ping.protocol")
local ping_service = require("lua_libp2p.protocol_ping.service")

local function new_ping_stream()
  local stream = {
    _written = nil,
    _closed = false,
  }

  function stream:write(payload)
    self._written = payload
    return true
  end

  function stream:read(n)
    if type(self._written) ~= "string" then
      return nil, "nothing written"
    end
    if #self._written < n then
      return nil, "short read"
    end
    return self._written:sub(1, n)
  end

  function stream:close()
    self._closed = true
    return true
  end

  return stream
end

local function run()
  local host, host_err = host_mod.new({
    services = {
      ping = ping_service,
    },
  })
  if not host then
    return nil, host_err
  end

  function host:new_stream(peer_or_addr, protocols)
    if peer_or_addr ~= "peer-a" then
      return nil, nil, nil, nil, "unexpected peer target"
    end
    if type(protocols) ~= "table" or protocols[1] ~= ping.ID then
      return nil, nil, nil, nil, "unexpected ping protocol selection"
    end
    return new_ping_stream(), ping.ID, { close = function() return true end }, { remote_peer_id = "peer-a" }
  end

  local result, ping_err = host.services.ping:ping("peer-a")
  if not result then
    return nil, ping_err
  end
  if result.protocol ~= ping.ID then
    return nil, "expected ping protocol in services ping result"
  end
  if type(result.rtt_seconds) ~= "number" then
    return nil, "expected ping result RTT"
  end

  return true
end

return {
  name = "host services ping",
  run = run,
}
