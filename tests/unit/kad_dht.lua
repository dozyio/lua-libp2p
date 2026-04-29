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
    mode = "server",
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

  local client_handled_protocol = nil
  local client_host = {
    _peer = { id = "local" },
  }
  function client_host:peer_id()
    return self._peer
  end
  function client_host:handle(protocol_id)
    client_handled_protocol = protocol_id
    return true
  end
  function client_host:add_service()
    return true
  end
  local client_dht, client_dht_err = kad_dht.new(client_host, {
    hash_function = fake_hash,
  })
  if not client_dht then
    return nil, client_dht_err
  end
  local client_started, client_start_err = client_dht:start()
  if not client_started then
    return nil, client_start_err
  end
  if client_dht.mode ~= "client" then
    return nil, "dht should default to client mode"
  end
  if client_handled_protocol ~= nil then
    return nil, "client-mode dht should not register protocol handler"
  end

  local protocol_callback = nil
  local event_host = {
    _peer = { id = "local" },
    peerstore = {
      get_addrs = function(_, peer_id)
        if peer_id == "peer-a" then
          return { "/ip4/127.0.0.1/tcp/4001" }
        end
        return {}
      end,
    },
  }
  function event_host:peer_id()
    return self._peer
  end
  function event_host:on(event, cb)
    if event == "peer_protocols_updated" then
      protocol_callback = cb
    end
    return true
  end
  function event_host:off(event, cb)
    if event == "peer_protocols_updated" and protocol_callback == cb then
      protocol_callback = nil
    end
    return true
  end
  local event_dht = assert(kad_dht.new(event_host, {
    hash_function = fake_hash,
    address_filter = "all",
  }))
  if not protocol_callback then
    return nil, "dht should subscribe to peer protocol updates"
  end
  local event_ok, event_err = protocol_callback({
    peer_id = "peer-a",
    protocols = { kad_dht.PROTOCOL_ID },
  })
  if not event_ok then
    return nil, event_err
  end
  if not event_dht:find_peer("peer-a") then
    return nil, "dht should add KAD-capable peers from protocol update events"
  end
  event_dht:stop()
  if protocol_callback ~= nil then
    return nil, "dht stop should unsubscribe from protocol update events"
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

  local outbound_peers, outbound_err = dht:_find_node("/ip4/127.0.0.1/tcp/1", "target")
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

  local value_result, value_err = dht:_get_value("peer-a", "key-a")
  if not value_result then
    return nil, value_err
  end
  if not value_result.record or value_result.record.value ~= "value-a" then
    return nil, "get_value should return record value"
  end

  local providers_result, providers_err = dht:_get_providers("peer-a", "cid-key")
  if not providers_result then
    return nil, providers_err
  end
  if #providers_result.providers ~= 1 or providers_result.providers[1].peer_id ~= "provider-a" then
    return nil, "get_providers should return provider peers"
  end

  local found_value, find_value_err = dht:_find_value("key-a", { peers = { { peer_id = "peer-a", addr = "peer-a" } } })
  if not found_value then
    return nil, find_value_err
  end
  if not found_value.record or found_value.record.value ~= "value-a" then
    return nil, "find_value should return first found record"
  end

  local found_providers, find_providers_err = dht:_find_providers("cid-key", { peers = { { peer_id = "peer-a", addr = "peer-a" } } })
  if not found_providers then
    return nil, find_providers_err
  end
  if #found_providers.providers ~= 1 or found_providers.providers[1].peer_id ~= "provider-a" then
    return nil, "find_providers should return discovered providers"
  end

  local closest_peers, closest_lookup = dht:_get_closest_peers("target", { peers = { { peer_id = "peer-a", addr = "peer-a" } }, count = 1 })
  if not closest_peers then
    return nil, closest_lookup
  end
  if #closest_peers ~= 1 or closest_peers[1].peer_id ~= "closest-a" then
    return nil, "get_closest_peers should return closest discovered peers"
  end
  if closest_lookup.termination ~= "starvation" and closest_lookup.termination ~= "closest_queried" then
    return nil, "get_closest_peers should include lookup termination"
  end

  local op_host = {
    _peer = { id = "local" },
  }
  function op_host:peer_id()
    return self._peer
  end
  function op_host:spawn_task(_, fn)
    local task = { id = 1 }
    local function pack(...)
      return { n = select("#", ...), ... }
    end
    local ctx = {
      checkpoint = function() return true end,
      await_task = function(_, child)
        return table.unpack(child.results, 1, child.results.n)
      end,
    }
    local results = pack(fn(ctx))
    if results[1] == nil and results[2] ~= nil then
      task.status = "failed"
      task.error = results[2]
    else
      task.status = "completed"
      task.result = results[1]
      task.results = results
    end
    return task
  end
  function op_host:run_until_task(task)
    if task.status ~= "completed" then
      return nil, task.error
    end
    return table.unpack(task.results, 1, task.results.n)
  end
  local op_dht = assert(kad_dht.new(op_host, {
    local_peer_id = "local",
    hash_function = fake_hash,
    address_filter = "all",
  }))
  op_dht._rpc = dht._rpc
  local closest_op, closest_op_err = op_dht:get_closest_peers("target", {
    peers = { { peer_id = "peer-a", addr = "peer-a" } },
    count = 1,
  })
  if not closest_op then
    return nil, closest_op_err
  end
  local op_peers, op_report, op_err = closest_op:result({ timeout = 1 })
  if not op_peers then
    return nil, op_err
  end
  if not op_report or #op_peers ~= 1 or op_peers[1].peer_id ~= "closest-a" then
    return nil, "public DHT operation should return result values and a separate error slot"
  end

  local spawned = 0
  local cancelled = 0
  local scheduler_host = {
    _peer = { id = "local" },
  }
  function scheduler_host:peer_id()
    return self._peer
  end
  function scheduler_host:spawn_task(_, fn)
    spawned = spawned + 1
    local task = { id = spawned, status = "waiting", result = { closer_peers = {} } }
    if spawned == 1 then
      local result, err = fn({})
      if result then
        task.status = "completed"
        task.result = result
      else
        task.status = "failed"
        task.error = err
      end
    end
    return task
  end
  function scheduler_host:cancel_task()
    cancelled = cancelled + 1
    return true
  end
  local scheduler_dht = assert(kad_dht.new(scheduler_host, {
    hash_function = fake_hash,
    k = 1,
    alpha = 2,
    disjoint_paths = 1,
    address_filter = "all",
  }))
  local checkpointed = 0
  local scheduler_lookup = scheduler_dht:_run_client_lookup("target", {
    { peer_id = "peer-a", addrs = { "/ip4/127.0.0.1/tcp/1" } },
    { peer_id = "peer-b", addrs = { "/ip4/127.0.0.1/tcp/2" } },
  }, function()
    return { closer_peers = {} }
  end, {
    scheduler_task = true,
    lookup_k = 1,
    ctx = {
      checkpoint = function()
        checkpointed = checkpointed + 1
        return true
      end,
    },
  })
  if not scheduler_lookup then
    return nil, "scheduler lookup should complete"
  end
  if scheduler_lookup.termination ~= "closest_queried" then
    return nil, "scheduler lookup should terminate as soon as closest peer is queried"
  end
  if spawned ~= 2 or cancelled ~= 1 then
    return nil, "scheduler lookup should cancel outstanding query tasks after strict completion"
  end
  if scheduler_lookup.queried ~= 2 or scheduler_lookup.responses ~= 1 or scheduler_lookup.failed ~= 0 or scheduler_lookup.cancelled ~= 1 then
    return nil, "scheduler lookup accounting should include cancelled active queries"
  end
  if checkpointed ~= 0 then
    return nil, "scheduler lookup should not checkpoint-spin after strict completion"
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
