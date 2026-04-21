local kad_dht = require("lua_libp2p.kad_dht")
local kad_protocol = require("lua_libp2p.kad_dht.protocol")

local function fake_hash(value)
  local map = {
    ["local"] = string.char(0) .. string.rep("\0", 31),
    ["peer-a"] = string.char(128) .. string.rep("\0", 31),
    ["peer-b"] = string.char(129) .. string.rep("\0", 31),
    ["target"] = string.char(128) .. string.rep("\0", 31),
  }
  return map[value] or (string.char(255) .. string.rep("\0", 31))
end

local function run()
  local wire_peer_id = "QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"
  if #kad_dht.DEFAULT_BOOTSTRAPPERS ~= 6 then
    return nil, "expected six default bootstrappers"
  end
  local dialable = kad_dht.default_bootstrappers({ dialable_only = true })
  if #dialable ~= 1 then
    return nil, "expected only tcp bootstrappers to be dialable without dnsaddr resolution"
  end

  local handled_protocol = nil
  local registered_handler = nil
  local host = {
    _peer = { id = "local" },
  }

  function host:peer_id()
    return self._peer
  end

  function host:handle(protocol_id, handler)
    handled_protocol = protocol_id
    registered_handler = handler
    return true
  end

  function host:new_stream()
    local response, response_err = kad_protocol.encode_message({
      type = kad_protocol.MESSAGE_TYPE.FIND_NODE,
      key = "target",
      closer_peers = {
        { id = wire_peer_id },
      },
    })
    if not response then
      return nil, nil, nil, nil, response_err
    end
    local len, len_err = require("lua_libp2p.multiformats.varint").encode_u64(#response)
    if not len then
      return nil, nil, nil, nil, len_err
    end

    local stream = {
      _in = len .. response,
      _out = "",
    }

    function stream:read(n)
      if #self._in < n then
        return nil, "unexpected EOF"
      end
      local out = self._in:sub(1, n)
      self._in = self._in:sub(n + 1)
      return out
    end

    function stream:write(payload)
      self._out = self._out .. payload
      return true
    end

    function stream:close_write()
      return true
    end

    function stream:close()
      return true
    end

    return stream, kad_dht.PROTOCOL_ID, {}, {}
  end

  local dht, dht_err = kad_dht.new(host, {
    hash_function = fake_hash,
    k = 2,
    alpha = 1,
  })
  if not dht then
    return nil, dht_err
  end

  local started, start_err = dht:start()
  if not started then
    return nil, start_err
  end
  if not dht:is_running() then
    return nil, "dht should be running after start"
  end
  if handled_protocol ~= kad_dht.PROTOCOL_ID then
    return nil, "dht start should register protocol handler"
  end
  if type(registered_handler) ~= "function" then
    return nil, "dht start should install a callable handler"
  end

  local added_a, add_a_err = dht:add_peer("peer-a")
  if not added_a then
    return nil, add_a_err
  end
  local added_b, add_b_err = dht:add_peer("peer-b")
  if not added_b then
    return nil, add_b_err
  end

  local found, found_err = dht:find_peer("peer-a")
  if not found then
    return nil, found_err or "expected to find peer-a"
  end
  if found.peer_id ~= "peer-a" then
    return nil, "find_peer returned unexpected peer"
  end

  local nearest, nearest_err = dht:find_closest_peers("target", 1)
  if not nearest then
    return nil, nearest_err
  end
  if #nearest ~= 1 or nearest[1].peer_id ~= "peer-a" then
    return nil, "find_closest_peers returned unexpected ordering"
  end

  local outbound_peers, outbound_err = dht:find_node("/ip4/127.0.0.1/tcp/1", "target")
  if not outbound_peers then
    return nil, outbound_err
  end
  if #outbound_peers ~= 1 or outbound_peers[1].peer_id ~= wire_peer_id then
    return nil, "find_node returned unexpected peer list"
  end

  local req_payload = assert(kad_protocol.encode_message({
    type = kad_protocol.MESSAGE_TYPE.FIND_NODE,
    key = "target",
  }))
  local req_len = assert(require("lua_libp2p.multiformats.varint").encode_u64(#req_payload))
  local scripted = {
    _in = req_len .. req_payload,
    _out = "",
  }
  function scripted:read(n)
    if #self._in < n then
      return nil, "unexpected EOF"
    end
    local out = self._in:sub(1, n)
    self._in = self._in:sub(n + 1)
    return out
  end
  function scripted:write(payload)
    self._out = self._out .. payload
    return true
  end
  function scripted:close_write()
    return true
  end

  local handled, handle_err = dht:_handle_rpc(scripted)
  if not handled then
    return nil, handle_err
  end
  local reply_reader = {
    _in = scripted._out,
  }
  function reply_reader:read(n)
    if #self._in < n then
      return nil, "unexpected EOF"
    end
    local out = self._in:sub(1, n)
    self._in = self._in:sub(n + 1)
    return out
  end
  local reply, reply_err = kad_protocol.read(reply_reader)
  if not reply then
    return nil, reply_err
  end
  if reply.type ~= kad_protocol.MESSAGE_TYPE.FIND_NODE then
    return nil, "unexpected rpc handler response type"
  end
  if #reply.closer_peers < 1 then
    return nil, "expected at least one closer peer in rpc response"
  end
  if reply.closer_peers[1].id ~= "peer-a" then
    return nil, "unexpected first rpc handler closer peer"
  end

  local stopped, stop_err = dht:stop()
  if not stopped then
    return nil, stop_err
  end
  if dht:is_running() then
    return nil, "dht should not be running after stop"
  end

  return true
end

return {
  name = "kad-dht package scaffold",
  run = run,
}
