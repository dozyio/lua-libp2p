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
    services = { identify = identify_service },
  })
  if not host then
    return nil, host_err
  end

  local writer = new_buffer_conn("")
  local wrote, write_err = identify.write(writer, {
    protocolVersion = "/ipfs/0.1.0",
    agentVersion = "remote/1.0.0",
    protocols = { "/ipfs/kad/1.0.0" },
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
    return stream,
      identify.ID,
      {
        close = function()
          return true
        end,
      },
      { remote_peer_id = "peer-a" }
  end

  local sub = assert(host:subscribe("peer:identified"))
  local entry, register_err = host:_register_connection({
    close = function()
      return true
    end,
  }, {
    remote_peer_id = "peer-a",
  })
  if not entry then
    return nil, register_err
  end

  local ev, ev_err
  for _ = 1, 8 do
    local bg_ok, bg_err = host:_run_background_tasks()
    if not bg_ok then
      return nil, bg_err
    end
    ev, ev_err = host:next_event(sub)
    if ev or ev_err then
      break
    end
  end
  if ev_err then
    return nil, ev_err
  end
  if not ev or ev.name ~= "peer:identified" then
    return nil, "expected peer:identified event after connection"
  end
  if not ev.payload or ev.payload.peer_id ~= "peer-a" then
    return nil, "expected peer:identified payload to include peer id"
  end
  if type(ev.payload.message) ~= "table" then
    return nil, "expected peer:identified payload to include identify message"
  end

  return true
end

return {
  name = "host identify runs on peer connect",
  run = run,
}
