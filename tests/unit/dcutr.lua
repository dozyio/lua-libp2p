local dcutr = require("lua_libp2p.protocol_dcutr.protocol")
local dcutr_service = require("lua_libp2p.protocol_dcutr.service")
local multiaddr = require("lua_libp2p.multiformats.multiaddr")

local function loopback_stream_pair()
  local a_in = ""
  local b_in = ""
  local a = {}
  local b = {}
  function a:write(data)
    b_in = b_in .. data
    return true
  end
  function b:write(data)
    a_in = a_in .. data
    return true
  end
  function a:read(n)
    if #a_in < n then
      return nil, "eof"
    end
    local out = a_in:sub(1, n)
    a_in = a_in:sub(n + 1)
    return out
  end
  function b:read(n)
    if #b_in < n then
      return nil, "eof"
    end
    local out = b_in:sub(1, n)
    b_in = b_in:sub(n + 1)
    return out
  end
  function a:close()
    return true
  end
  function b:close()
    return true
  end
  return a, b
end

local function run()
  local msg = {
    type = dcutr.TYPE.CONNECT,
    obs_addrs = {
      "/ip4/1.2.3.4/tcp/4001",
      "/ip6/::1/tcp/4001",
    },
  }
  local payload, enc_err = dcutr.encode_message(msg)
  if not payload then
    return nil, enc_err
  end
  local decoded, dec_err = dcutr.decode_message(payload)
  if not decoded then
    return nil, dec_err
  end
  if decoded.type ~= dcutr.TYPE.CONNECT or #decoded.obs_addrs ~= 2 then
    return nil, "dcutr encode/decode mismatch"
  end

  local a, b = loopback_stream_pair()
  local ok, write_err = dcutr.write_message(a, msg)
  if not ok then
    return nil, write_err
  end
  local wire_decoded, wire_err = dcutr.read_message(b)
  if not wire_decoded then
    return nil, wire_err
  end
  if wire_decoded.type ~= dcutr.TYPE.CONNECT then
    return nil, "dcutr wire decode type mismatch"
  end

  local handler_protocol = nil
  local handler_fn = nil
  local event_handlers = {}
  local emitted_events = {}
  local dial_calls = 0
  local dialed_addrs = {}
  local tagged_peers = {}
  local spawned_task_names = {}
  local closed_conn_count = 0
  local host = {
    address_manager = {
      get_public_address_mappings = function()
        return { "/ip4/8.8.8.8/tcp/4001" }
      end,
    },
    get_multiaddrs = function()
      return { "/ip4/127.0.0.1/tcp/4001" }
    end,
    connection_manager = {
      tag_peer = function(_, peer_id, tag, value)
        tagged_peers[#tagged_peers + 1] = { peer_id = peer_id, tag = tag, value = value }
        return true
      end,
    },
  }
  function host:handle(protocol, fn)
    handler_protocol = protocol
    handler_fn = fn
    return true
  end
  host.peerstore = {
    get_addrs = function(_, peer_id)
      if peer_id == "peer-auto-inbound" then
        return { "/ip6/2001:db8::10/tcp/4001" }
      end
      return {}
    end,
  }
  function host:on(name, fn)
    event_handlers[name] = fn
    return name
  end
  function host:dial(target)
    dial_calls = dial_calls + 1
    if type(target) == "table" and type(target.addrs) == "table" and #target.addrs > 0 then
      dialed_addrs[#dialed_addrs + 1] = target.addrs[1]
      return {}, { remote_peer_id = target.peer_id }
    end
    return nil, nil, "dial failed"
  end
  function host:_find_connection_by_id(connection_id)
    if connection_id ~= 42 then
      return nil
    end
    return {
      state = {
        remote_peer_id = "peer-1",
        relay = { limit_kind = "limited" },
      },
      conn = {
        close = function()
          closed_conn_count = closed_conn_count + 1
          return true
        end,
      },
    }
  end
  function host:spawn_task(name, fn)
    spawned_task_names[#spawned_task_names + 1] = name
    return {
      id = #spawned_task_names,
      status = "completed",
      result = fn({
        sleep = function()
          return true
        end,
      }),
    }
  end
  function host:wait_task(task)
    return task and task.result
  end
  function host:new_stream()
    return nil, nil, nil, "forced stream open failure"
  end
  function host:emit(name, event_payload)
    emitted_events[#emitted_events + 1] = { name, event_payload }
    return true
  end

  local svc, svc_err = dcutr_service.new(host, { relay_grace_seconds = 0 })
  if not svc then
    return nil, svc_err
  end
  local started, start_err = svc:start()
  if not started then
    return nil, start_err
  end
  if handler_protocol ~= dcutr.ID or type(handler_fn) ~= "function" then
    return nil, "dcutr handler registration failed"
  end
  if type(event_handlers["peer_connected"]) ~= "function" then
    return nil, "expected dcutr auto hook on peer_connected"
  end

  local before_auto = #spawned_task_names
  event_handlers["peer_connected"]({
    peer_id = "peer-auto-inbound",
    state = {
      direction = "inbound",
      relay = { limit_kind = "limited" },
    },
  })
  if
    #spawned_task_names ~= before_auto + 1 or spawned_task_names[#spawned_task_names] ~= "services.dcutr.auto_start"
  then
    return nil, "expected unilateral upgrade to run from async auto task"
  end
  if #dialed_addrs == 0 then
    return nil, "expected async unilateral upgrade to dial public candidate"
  end

  local before_outbound = #spawned_task_names
  event_handlers["peer_connected"]({
    peer_id = "peer-auto-outbound",
    state = {
      direction = "outbound",
      relay = { limit_kind = "limited" },
    },
  })
  if #spawned_task_names ~= before_outbound then
    return nil, "outbound limited relay connection should not trigger dcutr"
  end

  host.peerstore.get_addrs = function()
    return {}
  end
  local before_fallback = #spawned_task_names
  event_handlers["peer_connected"]({
    peer_id = "peer-auto-fallback",
    state = {
      direction = "inbound",
      relay = { limit_kind = "limited" },
    },
  })
  if #spawned_task_names == before_fallback then
    return nil, "expected inbound fallback to trigger dcutr task when unilateral upgrade has no candidates"
  end

  local inbound_a, inbound_b = loopback_stream_pair()
  local connect_ok, connect_write_err = dcutr.write_message(inbound_b, {
    type = dcutr.TYPE.CONNECT,
    obs_addrs = {
      multiaddr.to_bytes("/ip4/9.9.9.9/tcp/4001"),
      multiaddr.to_bytes("/ip6/2001:db8::1/tcp/4001"),
      multiaddr.to_bytes("/ip6/fd00::1/tcp/4001"),
      multiaddr.to_bytes("/ip6/2001:db8::1/tcp/4001/p2p-circuit"),
    },
  })
  if not connect_ok then
    return nil, connect_write_err
  end
  local sync_ok, sync_write_err = dcutr.write_message(inbound_b, { type = dcutr.TYPE.SYNC })
  if not sync_ok then
    return nil, sync_write_err
  end
  local handled, handled_err = handler_fn(inbound_a, {
    connection = { remote_peer_id = "peer-1" },
    state = {
      connection_id = 42,
      relay = { limit_kind = "limited" },
      remote_addr = "/ip6/2001:db8::9/tcp/4001",
    },
  })
  if not handled then
    return nil, handled_err
  end
  if dial_calls == 0 then
    return nil, "expected dcutr inbound handler to attempt direct dial"
  end
  if closed_conn_count == 0 then
    return nil, "expected dcutr to close relay connection after successful upgrade"
  end
  local selected_addr = dialed_addrs[#dialed_addrs]
  if
    type(selected_addr) ~= "string"
    or selected_addr:sub(1, 5) ~= "/ip6/"
    or string.find(selected_addr, "/p2p%-circuit", 1, false)
    or not multiaddr.is_public_addr(selected_addr)
  then
    return nil, "expected dcutr to prefer public same-family non-relay candidate"
  end

  local failed_new_stream_calls = 0
  host.new_stream = function()
    failed_new_stream_calls = failed_new_stream_calls + 1
    return nil, nil, nil, "forced stream open failure"
  end
  local retry_task = assert(svc:start_hole_punch("peer-retry", {
    max_attempts = 3,
    retry_delay_seconds = 0,
  }))
  local retry_result = retry_task.result
  if retry_result ~= nil then
    return nil, "expected dcutr retry task to fail after max attempts"
  end
  if failed_new_stream_calls ~= 3 then
    return nil, "expected dcutr to retry stream setup up to max attempts"
  end

  local saw_retry_reason = false
  for _, ev in ipairs(emitted_events) do
    if
      ev[1] == "dcutr:attempt:failed"
      and type(ev[2]) == "table"
      and ev[2].reason == dcutr_service.FAILURE_REASON.STREAM_OPEN_FAILED
    then
      saw_retry_reason = true
      break
    end
  end
  if not saw_retry_reason then
    return nil, "expected dcutr failure event to include reason code"
  end

  local saw_direct_tag = false
  for _, tagged in ipairs(tagged_peers) do
    if tagged.peer_id == "peer-1" and tagged.tag == "dcutr-direct" then
      saw_direct_tag = true
      break
    end
  end
  if not saw_direct_tag then
    return nil, "expected dcutr migration to tag peer as direct-preferred"
  end

  return true
end

return {
  name = "dcutr protocol and service",
  run = run,
}
