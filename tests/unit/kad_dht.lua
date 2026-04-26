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
  if dht.max_message_size ~= kad_protocol.MAX_MESSAGE_SIZE then
    return nil, "dht should default to kad protocol max message size"
  end
  if dht.address_filter ~= "public" then
    return nil, "dht should default to public address filter"
  end

  local small_dht, small_dht_err = kad_dht.new(host, {
    hash_function = fake_hash,
    max_message_size = 32,
    address_filter = "all",
  })
  if not small_dht then
    return nil, small_dht_err
  end
  if small_dht.max_message_size ~= 32 then
    return nil, "dht should allow max_message_size override"
  end
  if small_dht.address_filter ~= "all" then
    return nil, "dht should allow built-in address filter override"
  end

  local custom_seen = false
  local custom_dht, custom_dht_err = kad_dht.new(host, {
    hash_function = fake_hash,
    address_filter = function(addr, ctx)
      custom_seen = custom_seen or (ctx and ctx.purpose == "test")
      return addr:match("/tcp/4001") ~= nil
    end,
  })
  if not custom_dht then
    return nil, custom_dht_err
  end
  local custom_addrs = custom_dht:_filter_addrs({ "/ip4/8.8.8.8/tcp/4001", "/ip4/8.8.8.8/tcp/4002" }, { purpose = "test" })
  if #custom_addrs ~= 1 or custom_addrs[1] ~= "/ip4/8.8.8.8/tcp/4001" or not custom_seen then
    return nil, "custom address filter should be applied with context"
  end

  local invalid_dht, invalid_filter_err = kad_dht.new(host, {
    hash_function = fake_hash,
    address_filter = "invalid",
  })
  if invalid_dht ~= nil or not invalid_filter_err then
    return nil, "expected invalid address filter to fail"
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

  dht._rpc = function(_, _, request, expected_type)
    if request.type == kad_protocol.MESSAGE_TYPE.GET_VALUE then
      return {
        type = expected_type,
        key = request.key,
        record = { key = request.key, value = "value-a" },
      }
    end
    if request.type == kad_protocol.MESSAGE_TYPE.GET_PROVIDERS then
      return {
        type = expected_type,
        key = request.key,
        provider_peers = {
          { id = "provider-a", addrs = { "/ip4/8.8.8.8/tcp/4001" } },
        },
      }
    end
    if request.type == kad_protocol.MESSAGE_TYPE.FIND_NODE then
      return {
        type = expected_type,
        key = request.key,
        closer_peers = {
          { id = "closest-a", addrs = { "/ip4/8.8.4.4/tcp/4001" } },
        },
      }
    end
    return nil, "unexpected rpc"
  end

  local value_result, value_err = dht:get_value("peer-a", "key-a")
  if not value_result then
    return nil, value_err
  end
  if not value_result.record or value_result.record.value ~= "value-a" then
    return nil, "get_value should return record value"
  end

  local providers_result, providers_err = dht:get_providers("peer-a", "cid-key")
  if not providers_result then
    return nil, providers_err
  end
  if #providers_result.providers ~= 1 or providers_result.providers[1].peer_id ~= "provider-a" then
    return nil, "get_providers should return provider peers"
  end

  local found_value, find_value_err = dht:find_value("key-a", { peers = { { peer_id = "peer-a", addr = "peer-a" } } })
  if not found_value then
    return nil, find_value_err
  end
  if not found_value.record or found_value.record.value ~= "value-a" then
    return nil, "find_value should return first found record"
  end

  local found_providers, find_providers_err = dht:find_providers("cid-key", { peers = { { peer_id = "peer-a", addr = "peer-a" } } })
  if not found_providers then
    return nil, find_providers_err
  end
  if #found_providers.providers ~= 1 or found_providers.providers[1].peer_id ~= "provider-a" then
    return nil, "find_providers should return discovered providers"
  end

  local closest_peers, closest_lookup = dht:get_closest_peers("target", { peers = { { peer_id = "peer-a", addr = "peer-a" } }, count = 1 })
  if not closest_peers then
    return nil, closest_lookup
  end
  if #closest_peers ~= 1 or closest_peers[1].peer_id ~= "closest-a" then
    return nil, "get_closest_peers should return closest discovered peers"
  end
  if closest_lookup.termination ~= "starvation" and closest_lookup.termination ~= "closest_queried" then
    return nil, "get_closest_peers should include lookup termination"
  end

  dht._rpc = nil

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
