local ed25519 = require("lua_libp2p.crypto.ed25519")
local host_mod = require("lua_libp2p.host")
local identify = require("lua_libp2p.protocol_identify.protocol")
local identify_service = require("lua_libp2p.protocol_identify.service")

local function new_buffer_conn(initial)
  local conn = {
    _in = initial or "",
    _out = "",
    closed = false,
  }

  function conn:read(n)
    if #self._in < n then
      return nil, "unexpected EOF"
    end
    local out = self._in:sub(1, n)
    self._in = self._in:sub(n + 1)
    return out
  end

  function conn:write(payload)
    self._out = self._out .. payload
    return true
  end

  function conn:close()
    self.closed = true
    return true
  end

  function conn:writes()
    return self._out
  end

  return conn
end

local function run()
  local keypair = assert(ed25519.generate_keypair())
  local host, host_err = host_mod.new({
    identity = keypair,
    services = {
      identify = identify_service,
    },
  })
  if not host then
    return nil, host_err
  end

  local writer = new_buffer_conn("")
  local wrote, write_err = identify.write(writer, {
    protocolVersion = "/ipfs/0.1.0",
    agentVersion = "remote/1.0.0",
    protocols = { identify.ID },
    listenAddrs = { "/ip4/127.0.0.1/tcp/4001" },
  })
  if not wrote then
    return nil, write_err
  end
  local payload = writer:writes()

  function host:new_stream(peer_or_addr, protocols)
    if peer_or_addr ~= "peer-a" then
      return nil, nil, nil, nil, "unexpected peer target"
    end
    if type(protocols) ~= "table" or protocols[1] ~= identify.ID then
      return nil, nil, nil, nil, "unexpected identify protocol selection"
    end
    local stream = new_buffer_conn(payload)
    return stream, identify.ID, { close = function() return true end }, { remote_peer_id = "peer-a" }
  end

  local result, req_err = host.services.identify:identify("peer-a")
  if not result then
    return nil, req_err
  end
  if result.protocol ~= identify.ID then
    return nil, "expected identify protocol in services identify result"
  end
  if type(result.message) ~= "table" then
    return nil, "expected identify message in services identify result"
  end
  if result.message.agentVersion ~= "remote/1.0.0" then
    return nil, "expected identify message payload"
  end

  return true
end

return {
  name = "host services identify",
  run = run,
}
