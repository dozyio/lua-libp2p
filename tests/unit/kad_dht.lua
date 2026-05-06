local kad_dht = require("lua_libp2p.kad_dht")
local kad_protocol = require("lua_libp2p.kad_dht.protocol")
local multihash = require("lua_libp2p.multiformats.multihash")

local CONTENT_KEY = assert(multihash.sha2_256("cid-key"))

local function fake_hash(value)
  local map = {
    ["local"] = string.char(0) .. string.rep("\0", 31),
    ["peer-a"] = string.char(128) .. string.rep("\0", 31),
    ["peer-b"] = string.char(129) .. string.rep("\0", 31),
    ["closest-a"] = string.char(128) .. string.rep("\0", 31),
    ["closest-b"] = string.char(200) .. string.rep("\0", 31),
    ["target"] = string.char(128) .. string.rep("\0", 31),
    [CONTENT_KEY] = string.char(128) .. string.rep("\0", 31),
  }
  return map[value] or (string.char(255) .. string.rep("\0", 31))
end

local function attach_dial_policy(host_obj)
  host_obj.listen_addrs = host_obj.listen_addrs or { "/ip4/0.0.0.0/tcp/4001", "/ip6/::/tcp/4001" }
  function host_obj:_dialable_ip_families()
    local allow_ip4 = false
    local allow_ip6 = false
    for _, addr in ipairs(self.listen_addrs or {}) do
      if addr:sub(1, 5) == "/ip4/" then
        allow_ip4 = true
      elseif addr:sub(1, 5) == "/ip6/" then
        allow_ip6 = true
      end
    end
    if not allow_ip4 and not allow_ip6 then
      return true, true
    end
    return allow_ip4, allow_ip6
  end
  function host_obj:is_dialable_addr(addr)
    local allow_ip4, allow_ip6 = self:_dialable_ip_families()
    if addr:sub(1, 5) == "/ip4/" then
      return allow_ip4
    elseif addr:sub(1, 5) == "/ip6/" then
      return allow_ip6
    elseif addr:sub(1, 6) == "/dns4/" then
      return allow_ip4
    elseif addr:sub(1, 6) == "/dns6/" then
      return allow_ip6
    elseif addr:sub(1, 5) == "/dns/" then
      return allow_ip4 or allow_ip6
    end
    return false
  end
  function host_obj:filter_dialable_addrs(addrs)
    local out = {}
    for _, addr in ipairs(addrs or {}) do
      if self:is_dialable_addr(addr) then
        out[#out + 1] = addr
      end
    end
    return out
  end
end

local function run()
  local wire_peer_id = "QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"
  local handled_protocol = nil
  local registered_handler = nil
  local host = {
    _peer = { id = "local" },
    listen_addrs = { "/ip4/0.0.0.0/tcp/4001", "/ip6/::/tcp/4001" },
    peerstore = {
      addrs = {
        ["peer-a"] = { "/ip4/8.8.8.8/tcp/4001" },
        ["peer-b"] = { "/ip4/8.8.4.4/tcp/4001" },
      },
    },
  }
  attach_dial_policy(host)

  function host:peer_id()
    return self._peer
  end

  function host.peerstore:get_addrs(peer_id)
    return self.addrs[peer_id] or {}
  end

  function host.peerstore:merge(peer_id, info)
    if info and type(info.addrs) == "table" then
      self.addrs[peer_id] = info.addrs
    end
    return true
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

  host.listen_addrs = { "/ip4/0.0.0.0/tcp/4001" }
  local ip4_only = dht:_dialable_tcp_addrs({
    "/ip4/8.8.8.8/tcp/4001",
    "/ip6/2001:db8::1/tcp/4001",
    "/dns4/bootstrap.libp2p.io/tcp/4001",
    "/dns6/bootstrap.libp2p.io/tcp/4001",
  })
  if #ip4_only ~= 2 or ip4_only[1] ~= "/ip4/8.8.8.8/tcp/4001" or ip4_only[2] ~= "/dns4/bootstrap.libp2p.io/tcp/4001" then
    return nil, "dialable address filter should keep only ipv4-compatible addrs for ipv4-only listeners"
  end

  host.listen_addrs = { "/ip6/::/tcp/4001" }
  local ip6_only = dht:_dialable_tcp_addrs({
    "/ip4/8.8.8.8/tcp/4001",
    "/ip6/2001:db8::1/tcp/4001",
    "/dns4/bootstrap.libp2p.io/tcp/4001",
    "/dns6/bootstrap.libp2p.io/tcp/4001",
  })
  if #ip6_only ~= 2 or ip6_only[1] ~= "/ip6/2001:db8::1/tcp/4001" or ip6_only[2] ~= "/dns6/bootstrap.libp2p.io/tcp/4001" then
    return nil, "dialable address filter should keep only ipv6-compatible addrs for ipv6-only listeners"
  end

  host.listen_addrs = { "/ip4/0.0.0.0/tcp/4001", "/ip6/::/tcp/4001" }
  local dual_stack = dht:_dialable_tcp_addrs({
    "/ip4/8.8.8.8/tcp/4001",
    "/ip6/2001:db8::1/tcp/4001",
    "/dns/bootstrap.libp2p.io/tcp/4001",
  })
  if #dual_stack ~= 3 then
    return nil, "dialable address filter should keep both families for dual-stack listeners"
  end

  local host_without_policy = {
    _peer = { id = "local" },
    peerstore = host.peerstore,
  }
  function host_without_policy:peer_id()
    return self._peer
  end
  local strict_dht = assert(kad_dht.new(host_without_policy, {
    hash_function = fake_hash,
    address_filter = "all",
  }))
  local strict_filtered, strict_err = strict_dht:_dialable_tcp_addrs({ "/ip4/8.8.8.8/tcp/4001" })
  if strict_filtered ~= nil or not strict_err or strict_err.kind ~= "state" then
    return nil, "kad dht should require host filter_dialable_addrs policy helper"
  end

  local client_handled_protocol = nil
  local client_host = {
    _peer = { id = "local" },
  }
  attach_dial_policy(client_host)
  function client_host:peer_id()
    return self._peer
  end
  function client_host:handle(protocol_id)
    client_handled_protocol = protocol_id
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
  if client_dht:get_mode() ~= "client" then
    return nil, "get_mode should return current client mode"
  end
  if client_handled_protocol ~= nil then
    return nil, "client-mode dht should not register protocol handler"
  end

  local auto_handled_protocol = nil
  local auto_unhandled_protocol = nil
  local auto_self_callback = nil
  local auto_mode_events = {}
  local auto_host = { _peer = { id = "local" } }
  attach_dial_policy(auto_host)
  function auto_host:peer_id()
    return self._peer
  end
  function auto_host:handle(protocol_id)
    auto_handled_protocol = protocol_id
    return true
  end
  function auto_host:unhandle(protocol_id)
    auto_unhandled_protocol = protocol_id
    return true
  end
  function auto_host:on(event, cb)
    if event == "self_peer_update" then
      auto_self_callback = cb
    end
    return true
  end
  function auto_host:emit(event, payload)
    if event == "kad_dht:mode_changed" then
      auto_mode_events[#auto_mode_events + 1] = payload
    end
    return true
  end
  function auto_host:off(event, cb)
    if event == "self_peer_update" and auto_self_callback == cb then
      auto_self_callback = nil
    end
    return true
  end
  local auto_dht = assert(kad_dht.new(auto_host, {
    mode = "auto",
    hash_function = fake_hash,
  }))
  if auto_dht.mode ~= "client" then
    return nil, "auto-mode dht should start in client mode"
  end
  if auto_dht:get_mode() ~= "client" then
    return nil, "get_mode should return current auto client mode"
  end
  assert(auto_dht:start())
  if auto_handled_protocol ~= nil then
    return nil, "auto-mode dht should not serve before public self address"
  end
  local auto_ok, auto_err = auto_self_callback({ addrs = { "/ip4/203.0.113.10/tcp/4001" } })
  if not auto_ok then
    return nil, auto_err
  end
  if auto_dht.mode ~= "server" or auto_handled_protocol ~= kad_dht.PROTOCOL_ID then
    return nil, "auto-mode dht should switch to server for public direct address"
  end
  if auto_dht:get_mode() ~= "server" then
    return nil, "get_mode should return current auto server mode"
  end
  if #auto_mode_events ~= 1
    or auto_mode_events[1].old_mode ~= "client"
    or auto_mode_events[1].mode ~= "server"
    or auto_mode_events[1].reason ~= "public_self_address"
    or auto_mode_events[1].auto ~= true
  then
    return nil, "auto-mode dht should emit server mode change event"
  end
  auto_ok, auto_err = auto_self_callback({ addrs = { "/ip4/192.168.1.5/tcp/4001" } })
  if not auto_ok then
    return nil, auto_err
  end
  if auto_dht.mode ~= "client" or auto_unhandled_protocol ~= kad_dht.PROTOCOL_ID then
    return nil, "auto-mode dht should switch back to client without public direct address"
  end
  if #auto_mode_events ~= 2
    or auto_mode_events[2].old_mode ~= "server"
    or auto_mode_events[2].mode ~= "client"
    or auto_mode_events[2].reason ~= "no_public_self_address"
  then
    return nil, "auto-mode dht should emit client mode change event"
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
  if not event_dht:get_local_peer("peer-a") then
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

  local found, found_err = dht:get_local_peer("peer-a")
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
          { id = "closest-b", addrs = { "/ip4/8.8.4.5/tcp/4001" } },
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

  local providers_result, providers_err = dht:_get_providers("peer-a", CONTENT_KEY)
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

  local found_providers, find_providers_err = dht:_find_providers(CONTENT_KEY, { peers = { { peer_id = "peer-a", addr = "peer-a" } } })
  if not found_providers then
    return nil, find_providers_err
  end
  if #found_providers.providers ~= 1 or found_providers.providers[1].peer_id ~= "provider-a" then
    return nil, "find_providers should return discovered providers"
  end

  local closest_peers, closest_lookup = dht:_get_closest_peers("target", { peers = { { peer_id = "peer-a", addr = "peer-a" } }, count = 2 })
  if not closest_peers then
    return nil, closest_lookup
  end
  if #closest_peers ~= 2 or closest_peers[1].peer_id ~= "closest-a" or closest_peers[2].peer_id ~= "closest-b" then
    return nil, "get_closest_peers should sort discovered peers by KAD distance"
  end
  if closest_lookup.termination ~= "starvation" and closest_lookup.termination ~= "closest_queried" then
    return nil, "get_closest_peers should include lookup termination"
  end

  local op_host = {
    _peer = { id = "local" },
  }
  attach_dial_policy(op_host)
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

  local cached_peer_report = assert(dht:_find_peer_network("peer-a"))
  if not cached_peer_report.peer or cached_peer_report.peer.peer_id ~= "peer-a" or cached_peer_report.lookup.termination ~= "local_peer" then
    return nil, "find_peer_network should use local routing cache by default"
  end
  local network_peer_called = false
  op_dht._get_closest_peers = function(_, key, opts)
    network_peer_called = true
    if key ~= "peer-a" or opts.use_cache ~= false then
      return nil, "expected peer network lookup options"
    end
    return {
      { peer_id = "peer-a", addrs = { "/ip4/8.8.8.8/tcp/4001" } },
    }, { termination = "network_peer" }
  end
  local cached_peer_op = assert(op_dht:find_peer("peer-a"))
  local cached_peer_op_report = assert(cached_peer_op:result({ timeout = 1 }))
  if not cached_peer_op_report.peer or cached_peer_op_report.lookup.termination ~= "local_peer" then
    return nil, "public find_peer should use local cache by default"
  end
  local network_peer_op = assert(op_dht:find_peer("peer-a", { use_cache = false }))
  local network_peer_report = assert(network_peer_op:result({ timeout = 1 }))
  if not network_peer_called
    or not network_peer_report.peer
    or network_peer_report.peer.peer_id ~= "peer-a"
    or network_peer_report.lookup.termination ~= "network_peer"
  then
    return nil, "find_peer_network use_cache=false should bypass local peer cache"
  end
  network_peer_called = false
  local no_network_op = assert(op_dht:find_peer("missing-peer", { use_network = false }))
  local no_network_report = assert(no_network_op:result({ timeout = 1 }))
  if network_peer_called
    or no_network_report.peer ~= nil
    or no_network_report.lookup.termination ~= "network_disabled"
  then
    return nil, "find_peer use_network=false should not query network after cache miss"
  end

  local spawned = 0
  local cancelled = 0
  local scheduler_host = {
    _peer = { id = "local" },
  }
  attach_dial_policy(scheduler_host)
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

  spawned = 0
  local capped_dht = assert(kad_dht.new(scheduler_host, {
    hash_function = fake_hash,
    k = 20,
    alpha = 10,
    disjoint_paths = 10,
    max_concurrent_queries = 3,
    address_filter = "all",
  }))
  local seed_peers = {}
  for i = 1, 10 do
    seed_peers[#seed_peers + 1] = { peer_id = "peer-cap-" .. i, addrs = { "/ip4/127.0.0.1/tcp/" .. (1000 + i) } }
  end
  local capped_lookup = capped_dht:_run_client_lookup("target", seed_peers, function()
    return { closer_peers = {} }
  end, {})
  if not capped_lookup then
    return nil, "capped scheduler lookup should complete"
  end
  if capped_lookup.active_peak > 3 then
    return nil, "max_concurrent_queries should cap active lookup tasks"
  end
  if capped_lookup.requested_concurrency ~= 100 or capped_lookup.effective_concurrency ~= 3 or capped_lookup.max_concurrent_queries ~= 3 then
    return nil, "lookup report should include requested and effective concurrency"
  end

  local filtered_seen = {}
  local filter_dht = assert(kad_dht.new(scheduler_host, {
    hash_function = fake_hash,
    k = 1,
    alpha = 1,
    address_filter = "all",
    query_filter = function(peer, ctx)
      filtered_seen[#filtered_seen + 1] = { peer = peer.peer_id, target = ctx.target }
      return peer.peer_id ~= "peer-a"
    end,
  }))
  local filter_lookup = filter_dht:_run_client_lookup("target", {
    { peer_id = "peer-a", addrs = { "/ip4/127.0.0.1/tcp/1" } },
    { peer_id = "peer-b", addrs = { "/ip4/127.0.0.1/tcp/2" } },
  }, function(_peer)
    return { closer_peers = { { peer_id = "peer-a", addrs = { "/ip4/127.0.0.1/tcp/1" } } } }
  end, {
    scheduler_task = true,
  })
  if not filter_lookup then
    return nil, "query filter lookup should complete"
  end
  if filter_lookup.queried ~= 1 or not filter_lookup.queried_peers[1] or filter_lookup.queried_peers[1].peer_id ~= "peer-b" then
    return nil, "query_filter should exclude filtered seed peers"
  end
  if #filter_lookup.closest_peers ~= 1 or filter_lookup.closest_peers[1].peer_id ~= "peer-a" then
    return nil, "query_filter should still report discovered peers before enqueue filtering"
  end
  if #filtered_seen < 3 or filtered_seen[1].target ~= "target" then
    return nil, "query_filter should receive peer and target context"
  end
  local option_filter_lookup = filter_dht:_run_client_lookup("target", {
    { peer_id = "peer-b", addrs = { "/ip4/127.0.0.1/tcp/2" } },
  }, function()
    return { closer_peers = {} }
  end, {
    scheduler_task = true,
    query_filter = function()
      return false
    end,
  })
  if not option_filter_lookup or option_filter_lookup.queried ~= 0 or option_filter_lookup.termination ~= "starvation" then
    return nil, "per-query query_filter should override configured filter"
  end

  local routing_filter_seen = {}
  local routing_filter_dht = assert(kad_dht.new(scheduler_host, {
    hash_function = fake_hash,
    address_filter = "all",
    routing_table_filter = function(peer, ctx)
      routing_filter_seen[#routing_filter_seen + 1] = { peer = peer.peer_id, allow_replace = ctx.opts.allow_replace }
      return peer.peer_id ~= "peer-a"
    end,
  }))
  local rejected_peer, rejected_peer_err = routing_filter_dht:add_peer("peer-a")
  if rejected_peer ~= nil or not rejected_peer_err or rejected_peer_err.kind ~= "filtered" then
    return nil, "routing_table_filter should reject filtered peers"
  end
  if routing_filter_dht:get_local_peer("peer-a") ~= nil then
    return nil, "routing_table_filter should prevent routing table insertion"
  end
  local accepted_peer, accepted_peer_err = routing_filter_dht:add_peer("peer-b", { allow_replace = true })
  if not accepted_peer then
    return nil, accepted_peer_err
  end
  if not routing_filter_dht:get_local_peer("peer-b") then
    return nil, "routing_table_filter should allow accepted peers"
  end
  if #routing_filter_seen ~= 2 or routing_filter_seen[2].allow_replace ~= true then
    return nil, "routing_table_filter should receive peer and insertion options"
  end
  local override_peer, override_peer_err = routing_filter_dht:add_peer("peer-a", {
    routing_table_filter = function()
      return true
    end,
  })
  if not override_peer then
    return nil, override_peer_err
  end

  local kad_filter_host = {
    _peer = { id = "local" },
    peerstore = {
      protocols = {
        ["peer-a"] = { "/ipfs/ping/1.0.0" },
        ["peer-b"] = { kad_dht.PROTOCOL_ID },
        ["peer-c"] = {},
      },
    },
  }
  function kad_filter_host:peer_id()
    return self._peer
  end
  function kad_filter_host.peerstore:get_protocols(peer_id)
    return self.protocols[peer_id] or {}
  end
  function kad_filter_host.peerstore:supports_protocol(peer_id, protocol)
    for _, known in ipairs(self:get_protocols(peer_id)) do
      if known == protocol then
        return true
      end
    end
    return false
  end
  local kad_filter_dht = assert(kad_dht.new(kad_filter_host, {
    hash_function = fake_hash,
    address_filter = "all",
  }))
  local non_kad_added, non_kad_err = kad_filter_dht:add_peer("peer-a")
  if non_kad_added ~= nil or not non_kad_err or non_kad_err.kind ~= "filtered" then
    return nil, "add_peer should reject peers known not to support kad-dht"
  end
  local kad_added, kad_add_err = kad_filter_dht:add_peer("peer-b")
  if not kad_added then
    return nil, kad_add_err
  end
  local unknown_added, unknown_add_err = kad_filter_dht:add_peer("peer-c")
  if not unknown_added then
    return nil, unknown_add_err
  end
  local bypass_added, bypass_add_err = kad_filter_dht:add_peer("peer-a", { skip_kad_protocol_filter = true })
  if not bypass_added then
    return nil, bypass_add_err
  end

  local diversity_seen = {}
  local diversity_dht = assert(kad_dht.new(host, {
    hash_function = fake_hash,
    address_filter = "all",
    peer_diversity_filter = function(peer, ctx)
      diversity_seen[#diversity_seen + 1] = {
        peer = peer.peer_id,
        existing = #ctx.peers,
        k = ctx.k,
        allow_replace = ctx.opts.allow_replace,
      }
      return peer.peer_id ~= "peer-b"
    end,
  }))
  local diversity_a, diversity_a_err = diversity_dht:add_peer("peer-a", { allow_replace = true })
  if not diversity_a then
    return nil, diversity_a_err
  end
  local diversity_b, diversity_b_err = diversity_dht:add_peer("peer-b")
  if diversity_b ~= nil or not diversity_b_err or diversity_b_err.kind ~= "filtered" then
    return nil, "peer_diversity_filter should reject filtered peers"
  end
  if diversity_dht:get_local_peer("peer-b") ~= nil then
    return nil, "peer_diversity_filter should prevent routing table insertion"
  end
  if #diversity_seen ~= 2 or diversity_seen[1].existing ~= 0 or diversity_seen[2].existing ~= 1 or diversity_seen[1].allow_replace ~= true then
    return nil, "peer_diversity_filter should receive routing table snapshot and insertion options"
  end
  local diversity_override, diversity_override_err = diversity_dht:add_peer("peer-b", {
    peer_diversity_filter = function()
      return true
    end,
  })
  if not diversity_override then
    return nil, diversity_override_err
  end

  local ip_group_host = {
    _peer = { id = "local" },
    peerstore = {
      addrs = {
        ["peer-a"] = { "/ip4/203.0.113.1/tcp/4001" },
        ["peer-b"] = { "/ip4/203.0.42.2/tcp/4001" },
        ["peer-c"] = { "/ip4/198.51.100.3/tcp/4001" },
        ["peer-d"] = { "/ip6/2001:db8::1/tcp/4001" },
        ["peer-e"] = { "/ip6/2001:db8:1::1/tcp/4001" },
        ["peer-f"] = { "/ip4/12.1.1.1/tcp/4001" },
        ["peer-g"] = { "/ip4/12.2.1.1/tcp/4001" },
      },
    },
  }
  function ip_group_host:peer_id()
    return self._peer
  end
  function ip_group_host.peerstore:get_addrs(peer_id)
    return self.addrs[peer_id] or {}
  end
  local ip_group_dht = assert(kad_dht.new(ip_group_host, {
    hash_function = fake_hash,
    address_filter = "all",
    peer_diversity_max_peers_per_ip_group = 1,
  }))
  local ip_group_a, ip_group_a_err = ip_group_dht:add_peer("peer-a")
  if not ip_group_a then
    return nil, ip_group_a_err
  end
  local ip_group_b, ip_group_b_err = ip_group_dht:add_peer("peer-b")
  if ip_group_b ~= nil or not ip_group_b_err or ip_group_b_err.kind ~= "filtered" then
    return nil, "built-in ip group diversity should reject overrepresented ipv4 /16 peers"
  end
  if ip_group_dht:get_local_peer("peer-b") ~= nil then
    return nil, "built-in ip group diversity should prevent routing table insertion"
  end
  local ip_group_c, ip_group_c_err = ip_group_dht:add_peer("peer-c")
  if not ip_group_c then
    return nil, ip_group_c_err
  end
  local ip_group_d, ip_group_d_err = ip_group_dht:add_peer("peer-d")
  if not ip_group_d then
    return nil, ip_group_d_err
  end
  local ip_group_e, ip_group_e_err = ip_group_dht:add_peer("peer-e")
  if ip_group_e ~= nil or not ip_group_e_err or ip_group_e_err.kind ~= "filtered" then
    return nil, "built-in ip group diversity should reject overrepresented ipv6 /32 peers"
  end
  local legacy_dht = assert(kad_dht.new(ip_group_host, {
    hash_function = fake_hash,
    address_filter = "all",
    peer_diversity_max_peers_per_ip_group = 1,
  }))
  assert(legacy_dht:add_peer("peer-f"))
  local legacy_added, legacy_err = legacy_dht:add_peer("peer-g")
  if legacy_added ~= nil or not legacy_err or legacy_err.kind ~= "filtered" then
    return nil, "built-in ip group diversity should group legacy class A ipv4 blocks by /8"
  end
  local per_bucket_dht = assert(kad_dht.new(ip_group_host, {
    hash_function = fake_hash,
    address_filter = "all",
    peer_diversity_max_peers_per_ip_group = 3,
    peer_diversity_max_peers_per_ip_group_per_bucket = 1,
  }))
  assert(per_bucket_dht:add_peer("peer-a"))
  local per_bucket_added, per_bucket_err = per_bucket_dht:add_peer("peer-b")
  if per_bucket_added ~= nil or not per_bucket_err or per_bucket_err.kind ~= "filtered" then
    return nil, "built-in ip group diversity should enforce per-bucket limits"
  end

  local find_node_target_peer = wire_peer_id
  local find_node_target_bytes = assert(kad_protocol.peer_bytes(find_node_target_peer))
  local find_node_host = {
    _peer = { id = "local" },
    peerstore = {
      addrs = {
        [find_node_target_peer] = { "/ip4/10.0.0.1/tcp/4001" },
        ["peer-a"] = { "/ip4/8.8.8.8/tcp/4001", "/ip4/127.0.0.1/tcp/4001" },
        ["peer-b"] = { "/ip4/8.8.4.4/tcp/4001" },
        ["local"] = { "/ip4/8.8.4.5/tcp/4001" },
      },
    },
  }
  function find_node_host:peer_id()
    return self._peer
  end
  function find_node_host.peerstore:get_addrs(peer_id)
    return self.addrs[peer_id] or {}
  end
  local find_node_dht = assert(kad_dht.new(find_node_host, {
    hash_function = fake_hash,
  }))
  assert(find_node_dht:add_peer("peer-a", { skip_kad_protocol_filter = true }))
  assert(find_node_dht:add_peer("peer-b", { skip_kad_protocol_filter = true }))
  local find_node_response = assert(find_node_dht:_handle_find_node({
    type = kad_protocol.MESSAGE_TYPE.FIND_NODE,
    key = find_node_target_bytes,
  }, { peer_id = "peer-a" }))
  local response_ids = {}
  local target_record
  for _, peer in ipairs(find_node_response.closer_peers or {}) do
    local id = peer.id == find_node_target_bytes and find_node_target_peer or peer.id
    response_ids[id] = peer
    if peer.id == find_node_target_bytes then
      target_record = peer
    end
  end
  if not target_record then
    return nil, "FIND_NODE should include target peer from peerstore even when it is not a DHT server"
  end
  if response_ids["peer-a"] or response_ids["local"] then
    return nil, "FIND_NODE should exclude requester and self from closer peers"
  end
  if not response_ids["peer-b"] then
    return nil, "FIND_NODE should keep returning other closest peers after exclusions"
  end
  if target_record.addrs[1] ~= nil then
    return nil, "public FIND_NODE responses should filter private target addresses by default"
  end
  if #response_ids["peer-b"].addrs ~= 1 or response_ids["peer-b"].addrs[1] ~= "/ip4/8.8.4.4/tcp/4001" then
    return nil, "public FIND_NODE responses should include only public addresses"
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
