local kad_dht = require("lua_libp2p.kad_dht")
local keys = require("lua_libp2p.crypto.keys")
local kad_protocol = require("lua_libp2p.kad_dht.protocol")
local memory_datastore = require("lua_libp2p.datastore.memory")
local records = require("lua_libp2p.kad_dht.records")
local sqlite = require("lua_libp2p.datastore.sqlite")
local varint = require("lua_libp2p.multiformats.varint")

local function fake_hash(value)
  local map = {
    local_peer = string.char(0) .. string.rep("\0", 31),
    ["record-key"] = string.char(128) .. string.rep("\0", 31),
  }
  return map[value] or (string.char(255) .. string.rep("\0", 31))
end

local function hex_encode(value)
  return (value:gsub(".", function(c)
    return string.format("%02x", c:byte())
  end))
end

local function new_host()
  local host = {
    _peer = { id = "local_peer" },
    peerstore = { _peers = {} },
  }
  function host:peer_id()
    return self._peer
  end
  function host.peerstore:get_addrs()
    return {}
  end
  function host.peerstore:merge(peer_id, value)
    self._peers[peer_id] = self._peers[peer_id] or { peer_id = peer_id }
    for k, v in pairs(value or {}) do
      self._peers[peer_id][k] = v
    end
    return true
  end
  function host.peerstore:get(peer_id)
    return self._peers[peer_id]
  end
  return host
end

local function new_task_host(identity)
  local host = new_host()
  host.identity = identity
  if identity then
    local peer = assert(keys.peer_id(identity))
    host._peer = { id = peer.id }
  end
  function host:spawn_task(_, fn)
    local task = { id = 1 }
    local function pack(...)
      return { n = select("#", ...), ... }
    end
    local ctx = {
      checkpoint = function()
        return true
      end,
    }
    local results = pack(fn(ctx))
    task.results = results
    if results[1] == nil and results[2] ~= nil then
      task.status = "failed"
      task.error = results[2]
    else
      task.status = "completed"
      task.result = results[1]
    end
    return task
  end
  function host:run_until_task(task)
    if task.status ~= "completed" then
      return nil, task.error
    end
    return table.unpack(task.results, 1, task.results.n)
  end
  return host
end

local function new_scripted_stream(message)
  local payload = assert(kad_protocol.encode_message(message))
  local frame = assert(varint.encode_u64(#payload)) .. payload
  local stream = {
    _in = frame,
    _out = "",
    _reset = false,
  }
  function stream:read(n)
    if #self._in < n then
      return nil, "unexpected EOF"
    end
    local out = self._in:sub(1, n)
    self._in = self._in:sub(n + 1)
    return out
  end
  function stream:write(payload_bytes)
    self._out = self._out .. payload_bytes
    return true
  end
  function stream:close_write()
    return true
  end
  function stream:reset()
    self._reset = true
    return true
  end
  return stream
end

local function read_response(stream)
  local reader = { _in = stream._out }
  function reader:read(n)
    if #self._in < n then
      return nil, "unexpected EOF"
    end
    local out = self._in:sub(1, n)
    self._in = self._in:sub(n + 1)
    return out
  end
  return kad_protocol.read(reader)
end

local function run()
  local now = 1000
  local store = assert(records.new({
    default_ttl_seconds = 10,
    now = function()
      return now
    end,
  }))
  local put, put_err = store:put("record-key", {
    key = "record-key",
    value = "record-value",
  })
  if not put then
    return nil, put_err
  end
  local entry, entry_err = store:get("record-key")
  if not entry then
    return nil, entry_err or "expected stored record"
  end
  if entry.value ~= "record-value" or entry.time_received ~= "1970-01-01T00:16:40Z" or entry.expires_at ~= 1010 then
    return nil, "record store should preserve value and set timestamps"
  end
  now = 1011
  if store:get("record-key") ~= nil then
    return nil, "record store should expire records"
  end

  local raw_record_store = memory_datastore.new()
  local corrupt_record_key = "kad_dht/records/" .. hex_encode("record-key")
  assert(raw_record_store:put(corrupt_record_key, {
    key = "record-key",
    value = 123,
  }))
  local corrupt_record_store = assert(records.new({ datastore = raw_record_store }))
  if corrupt_record_store:get("record-key") ~= nil or raw_record_store:get(corrupt_record_key) ~= nil then
    return nil, "record store should delete and ignore malformed persisted entries"
  end

  local dht = assert(kad_dht.new(new_host(), {
    mode = "server",
    hash_function = fake_hash,
    address_filter = "all",
  }))
  local put_stream = new_scripted_stream({
    type = kad_protocol.MESSAGE_TYPE.PUT_VALUE,
    key = "record-key",
    record = {
      key = "record-key",
      value = "record-value",
      time_received = "2026-05-04T00:00:00Z",
    },
  })
  local handled, handle_err = dht:_handle_rpc(put_stream)
  if not handled then
    return nil, handle_err
  end
  local put_response, put_response_err = read_response(put_stream)
  if not put_response then
    return nil, put_response_err
  end
  if put_response.type ~= kad_protocol.MESSAGE_TYPE.PUT_VALUE or not put_response.record then
    return nil, "PUT_VALUE should return stored record"
  end
  local local_record = assert(dht:get_local_record("record-key"))
  if local_record.value ~= "record-value" then
    return nil, "PUT_VALUE should store value record locally"
  end

  local get_stream = new_scripted_stream({
    type = kad_protocol.MESSAGE_TYPE.GET_VALUE,
    key = "record-key",
  })
  handled, handle_err = dht:_handle_rpc(get_stream)
  if not handled then
    return nil, handle_err
  end
  local get_response, get_response_err = read_response(get_stream)
  if not get_response then
    return nil, get_response_err
  end
  if get_response.type ~= kad_protocol.MESSAGE_TYPE.GET_VALUE or not get_response.record or get_response.record.value ~= "record-value" then
    return nil, "GET_VALUE should return local value record"
  end

  local invalid_get_stream = new_scripted_stream({
    type = kad_protocol.MESSAGE_TYPE.GET_VALUE,
  })
  local invalid_handled, invalid_handle_err = dht:_handle_rpc(invalid_get_stream)
  if invalid_handled ~= nil or not invalid_handle_err then
    return nil, "invalid GET_VALUE should fail"
  end
  if invalid_get_stream._out ~= "" or invalid_get_stream._reset ~= true then
    return nil, "invalid GET_VALUE should reset without writing a response"
  end

  local invalid_put_stream = new_scripted_stream({
    type = kad_protocol.MESSAGE_TYPE.PUT_VALUE,
    key = "record-key",
  })
  invalid_handled, invalid_handle_err = dht:_handle_rpc(invalid_put_stream)
  if invalid_handled ~= nil or not invalid_handle_err then
    return nil, "invalid PUT_VALUE should fail"
  end
  if invalid_put_stream._out ~= "" or invalid_put_stream._reset ~= true then
    return nil, "invalid PUT_VALUE should reset without writing a response"
  end

  local rejecting_dht = assert(kad_dht.new(new_host(), {
    mode = "server",
    hash_function = fake_hash,
    address_filter = "all",
    record_validator = function()
      return false
    end,
  }))
  local rejected, rejected_err = rejecting_dht:_handle_put_value({
    type = kad_protocol.MESSAGE_TYPE.PUT_VALUE,
    key = "record-key",
    record = { key = "record-key", value = "bad" },
  })
  if rejected ~= nil or not rejected_err or rejected_err.kind ~= "protocol" then
    return nil, "PUT_VALUE should honor record validator rejection"
  end

  local rpc_request
  dht._rpc = function(_, peer_or_addr, request, expected_type)
    if peer_or_addr ~= "peer-a" then
      return nil, "unexpected put value target"
    end
    rpc_request = request
    return {
      type = expected_type,
      key = request.key,
      record = request.record,
      closer_peers = {},
    }
  end
  local put_result, put_result_err = dht:_put_value("peer-a", "record-key", {
    key = "record-key",
    value = "record-value",
  })
  if not put_result then
    return nil, put_result_err
  end
  if not rpc_request
    or rpc_request.type ~= kad_protocol.MESSAGE_TYPE.PUT_VALUE
    or rpc_request.key ~= "record-key"
    or not rpc_request.record
    or rpc_request.record.value ~= "record-value"
  then
    return nil, "_put_value should send PUT_VALUE request with record"
  end

  local put_host = new_host()
  function put_host:spawn_task(_, fn)
    local task = { id = 1 }
    local function pack(...)
      return { n = select("#", ...), ... }
    end
    local ctx = {
      checkpoint = function()
        return true
      end,
    }
    local results = pack(fn(ctx))
    task.results = results
    if results[1] == nil and results[2] ~= nil then
      task.status = "failed"
      task.error = results[2]
    else
      task.status = "completed"
      task.result = results[1]
    end
    return task
  end
  function put_host:run_until_task(task)
    if task.status ~= "completed" then
      return nil, task.error
    end
    return table.unpack(task.results, 1, task.results.n)
  end
  local put_dht = assert(kad_dht.new(put_host, {
    mode = "client",
    hash_function = fake_hash,
    address_filter = "all",
  }))
  put_dht._get_closest_peers = function(_, key, opts)
    if key ~= "record-key" or not opts.scheduler_task then
      return nil, "put_value should run closest-peer lookup as scheduler task"
    end
    return {
      { peer_id = "server-a", addrs = { "/ip4/1.1.1.1/tcp/4001" } },
      { peer_id = "server-b", addrs = { "/ip4/1.1.1.2/tcp/4001" } },
    }, { termination = "test" }
  end
  local published = {}
  put_dht._put_value = function(_, target, key, record)
    published[#published + 1] = {
      target = target,
      key = key,
      record = record,
    }
    return { key = key, record = record, closer_peers = {} }
  end
  local op, op_err = put_dht:put_value("record-key", "record-value", { count = 2 })
  if not op then
    return nil, op_err
  end
  local report, report_err = op:result({ timeout = 1 })
  if not report then
    return nil, report_err
  end
  if report.attempted ~= 2 or report.succeeded ~= 2 or report.failed ~= 0 or #published ~= 2 then
    return nil, "put_value should publish to closest peers and report successes"
  end
  if published[1].key ~= "record-key" or published[1].record.value ~= "record-value" then
    return nil, "put_value should publish value record"
  end
  local stored_local = assert(put_dht:get_local_record("record-key"))
  if stored_local.value ~= "record-value" then
    return nil, "put_value should store local record"
  end
  local alias_put_op, alias_put_err = put_dht:put("record-key", "record-value", { count = 1 })
  if not alias_put_op then
    return nil, alias_put_err
  end
  local alias_put_report, alias_put_report_err = alias_put_op:result({ timeout = 1 })
  if not alias_put_report then
    return nil, alias_put_report_err
  end
  if alias_put_report.attempted ~= 1 or alias_put_report.succeeded ~= 1 then
    return nil, "put alias should delegate to put_value behavior"
  end
  local alias_get_op, alias_get_err = put_dht:get("record-key")
  if not alias_get_op then
    return nil, alias_get_err
  end
  local alias_get_result, alias_get_result_err = alias_get_op:result({ timeout = 1 })
  if not alias_get_result then
    return nil, alias_get_result_err
  end
  if not alias_get_result.record or alias_get_result.record.value ~= "record-value" then
    return nil, "get alias should delegate to find_value behavior"
  end
  local cache_only_op = assert(put_dht:get("record-key", { use_network = false }))
  local cache_only_result = assert(cache_only_op:result({ timeout = 1 }))
  if not cache_only_result.record or cache_only_result.record.value ~= "record-value" or cache_only_result.lookup.termination ~= "local_record" then
    return nil, "value lookup use_network=false should return local cache hit"
  end
  local value_network_called = false
  put_dht._run_client_lookup = function()
    value_network_called = true
    return { closest_peers = {}, errors = {}, termination = "network_called" }
  end
  local cache_miss_no_network_op = assert(put_dht:get("missing-key", { use_network = false }))
  local cache_miss_no_network = assert(cache_miss_no_network_op:result({ timeout = 1 }))
  if value_network_called
    or cache_miss_no_network.record ~= nil
    or cache_miss_no_network.lookup.termination ~= "network_disabled"
  then
    return nil, "value lookup use_network=false should not query network after cache miss"
  end
  local no_cache_no_network_op = assert(put_dht:get("record-key", { use_cache = false, use_network = false }))
  local no_cache_no_network = assert(no_cache_no_network_op:result({ timeout = 1 }))
  if value_network_called
    or no_cache_no_network.record ~= nil
    or no_cache_no_network.lookup.termination ~= "network_disabled"
  then
    return nil, "value lookup with cache and network disabled should not return local cache"
  end
  local capabilities = {}
  for _, capability in ipairs(kad_dht.provides or {}) do
    capabilities[capability] = true
  end
  if not capabilities.peer_routing or not capabilities.content_routing or not capabilities.value_routing or not capabilities.kad_dht then
    return nil, "kad_dht should advertise peer/content/value routing capabilities"
  end

  local policy_dht = assert(kad_dht.new(new_host(), {
    mode = "server",
    hash_function = fake_hash,
    address_filter = "all",
  }))
  local validator_seen = false
  local validator_ok, validator_set_err = policy_dht:set_record_validator("testns", function(key, record)
    validator_seen = key == "/testns/name" and record.value == "allowed"
    return record.value == "allowed"
  end)
  if not validator_ok then
    return nil, validator_set_err
  end
  local accepted, accepted_err = policy_dht:put_local_record("/testns/name", {
    key = "/testns/name",
    value = "allowed",
  })
  if not accepted then
    return nil, accepted_err
  end
  if not validator_seen then
    return nil, "namespaced record validator should run"
  end
  local blocked, blocked_err = policy_dht:put_local_record("/testns/name", {
    key = "/testns/name",
    value = "blocked",
  })
  if blocked ~= nil or not blocked_err or blocked_err.kind ~= "protocol" then
    return nil, "namespaced record validator should reject invalid records"
  end

  local selector_ok, selector_set_err = policy_dht:set_record_selector("testns", function(_, existing, incoming)
    if incoming.value == "newer" then
      return "incoming"
    end
    if existing.value == "allowed" then
      return "existing"
    end
    return "incoming"
  end)
  if not selector_ok then
    return nil, selector_set_err
  end
  policy_dht:set_record_validator("testns", function()
    return true
  end)
  local kept, kept_err = policy_dht:put_local_record("/testns/name", {
    key = "/testns/name",
    value = "older",
  })
  if not kept then
    return nil, kept_err
  end
  local selected_record = assert(policy_dht:get_local_record("/testns/name"))
  if selected_record.value ~= "allowed" then
    return nil, "record selector should keep existing record when it wins"
  end
  local replaced, replaced_err = policy_dht:put_local_record("/testns/name", {
    key = "/testns/name",
    value = "newer",
  })
  if not replaced then
    return nil, replaced_err
  end
  selected_record = assert(policy_dht:get_local_record("/testns/name"))
  if selected_record.value ~= "newer" then
    return nil, "record selector should replace existing record when incoming wins"
  end

  local bad_selector_ok, bad_selector_err = policy_dht:set_record_selector("testns", "nope")
  if bad_selector_ok ~= nil or not bad_selector_err or bad_selector_err.kind ~= "input" then
    return nil, "set_record_selector should reject non-functions"
  end

  local function run_multi_record_selection(order, opts)
    local selection_dht = assert(kad_dht.new(new_task_host(), {
      mode = "client",
      hash_function = fake_hash,
      address_filter = "all",
    }))
    assert(selection_dht:set_record_validator("testns", function()
      return true
    end))
    assert(selection_dht:set_record_selector("testns", function(_, existing, incoming)
      return tonumber(incoming.value) > tonumber(existing.value) and "incoming" or "existing"
    end))
    local records_by_peer = {
      ["peer-old"] = { key = "/testns/select", value = "1" },
      ["peer-new"] = { key = "/testns/select", value = "2" },
    }
    local corrections = {}
    function selection_dht:_get_value(target)
      return { key = "/testns/select", record = records_by_peer[target], closer_peers = {} }
    end
    function selection_dht:_put_value(target, key, record)
      corrections[#corrections + 1] = { target = target, key = key, record = record }
      return { key = key, record = record, closer_peers = {} }
    end
    function selection_dht:_run_client_lookup(_, seed_peers, query_func)
      local lookup = {
        closest_peers = {},
        errors = {},
        termination = "closest_queried",
        queried = 0,
        responses = 0,
      }
      for _, peer in ipairs(seed_peers) do
        lookup.queried = lookup.queried + 1
        local response, err = query_func(peer)
        if response then
          lookup.responses = lookup.responses + 1
        else
          lookup.errors[#lookup.errors + 1] = err
        end
      end
      return lookup
    end
    local find_opts = opts or {}
    find_opts.use_cache = false
    find_opts.peers = order
    local selection_op = assert(selection_dht:find_value("/testns/select", find_opts))
    local result = assert(selection_op:result({ timeout = 1 }))
    return result, corrections
  end
  local newer_after_older, corrections = run_multi_record_selection({
    { peer_id = "peer-old", addr = "peer-old" },
    { peer_id = "peer-new", addr = "peer-new" },
  })
  if newer_after_older.record.value ~= "2" or newer_after_older.found_records ~= 2 then
    return nil, "find_value should select the best remote record after seeing multiple records"
  end
  if #corrections ~= 1 or corrections[1].target ~= "peer-old" or corrections[1].record.value ~= "2" then
    return nil, "find_value should correct peers that returned stale records"
  end
  local _, disabled_corrections = run_multi_record_selection({
    { peer_id = "peer-old", addr = "peer-old" },
    { peer_id = "peer-new", addr = "peer-new" },
  }, { correct_stale_records = false })
  if #disabled_corrections ~= 0 then
    return nil, "correct_stale_records=false should disable stale record correction"
  end
  local older_after_newer = assert(run_multi_record_selection({
    { peer_id = "peer-new", addr = "peer-new" },
    { peer_id = "peer-old", addr = "peer-old" },
  }))
  if older_after_newer.record.value ~= "2" or older_after_newer.found_records ~= 2 then
    return nil, "find_value should keep the best remote record regardless of response order"
  end

  local pk_identity = assert(keys.generate_keypair("ed25519"))
  local pk_peer = assert(keys.peer_id(pk_identity))
  local pk_key = "/pk/" .. pk_peer.id
  local pk_dht = assert(kad_dht.new(new_host(), {
    mode = "server",
    hash_function = fake_hash,
    address_filter = "all",
  }))
  local pk_stored, pk_store_err = pk_dht:put_local_record(pk_key, {
    key = pk_key,
    value = pk_identity.public_key_proto,
  })
  if not pk_stored then
    return nil, pk_store_err
  end
  local pk_record = assert(pk_dht:get_local_record(pk_key))
  if pk_record.value ~= pk_identity.public_key_proto then
    return nil, "valid /pk record should store public key proto"
  end
  local raw_pk_key = "/pk/" .. pk_peer.bytes
  local raw_pk_stored, raw_pk_store_err = pk_dht:put_local_record(raw_pk_key, {
    key = raw_pk_key,
    value = pk_identity.public_key_proto,
  })
  if not raw_pk_stored then
    return nil, raw_pk_store_err
  end
  local raw_pk_record = assert(pk_dht:get_local_record(raw_pk_key))
  if raw_pk_record.value ~= pk_identity.public_key_proto then
    return nil, "raw-byte /pk record key should validate against public key proto"
  end

  local bad_pk_key_stored, bad_pk_key_err = pk_dht:put_local_record("/pk/", {
    key = "/pk/",
    value = pk_identity.public_key_proto,
  })
  if bad_pk_key_stored ~= nil or not bad_pk_key_err then
    return nil, "invalid /pk key should reject"
  end

  local other_identity = assert(keys.generate_keypair("ed25519"))
  local mismatched, mismatch_err = pk_dht:put_local_record(pk_key, {
    key = pk_key,
    value = other_identity.public_key_proto,
  })
  if mismatched ~= nil or not mismatch_err or mismatch_err.kind ~= "protocol" then
    return nil, "mismatched /pk public key should reject"
  end

  local original_pk_record = assert(pk_dht:get_local_record(pk_key))
  local duplicate_stored, duplicate_err = pk_dht:put_local_record(pk_key, {
    key = pk_key,
    value = pk_identity.public_key_proto,
  })
  if not duplicate_stored then
    return nil, duplicate_err
  end
  local duplicate_pk_record = assert(pk_dht:get_local_record(pk_key))
  if duplicate_pk_record.value ~= original_pk_record.value then
    return nil, "/pk selector should keep immutable public key value"
  end

  local synth_identity = assert(keys.generate_keypair("ed25519"))
  local synth_peer = assert(keys.peer_id(synth_identity))
  local synth_host = new_task_host(synth_identity)
  local synth_dht = assert(kad_dht.new(synth_host, {
    mode = "server",
    hash_function = fake_hash,
    address_filter = "all",
  }))
  local synth_key = "/pk/" .. synth_peer.id
  local synthesized = assert(synth_dht:_synthesize_pk_record(synth_key))
  if synthesized.value ~= synth_identity.public_key_proto then
    return nil, "self /pk synthesis should use host identity public key proto"
  end
  local pk_get = assert(synth_dht:_handle_get_value({
    type = kad_protocol.MESSAGE_TYPE.GET_VALUE,
    key = synth_key,
  }))
  if not pk_get.record or pk_get.record.value ~= synth_identity.public_key_proto then
    return nil, "GET_VALUE should synthesize self /pk record"
  end
  local find_op, find_op_err = synth_dht:find_value(synth_key, {
    peers = {
      { peer_id = "unused", addrs = { "/ip4/1.1.1.1/tcp/4001" } },
    },
  })
  if not find_op then
    return nil, find_op_err
  end
  local found_pk, found_pk_err = find_op:result({ timeout = 1 })
  if not found_pk then
    return nil, found_pk_err
  end
  if not found_pk.record or found_pk.record.value ~= synth_identity.public_key_proto or found_pk.lookup.termination ~= "local_record" then
    return nil, "find_value should short-circuit self /pk from local identity"
  end
  local network_lookup_called = false
  synth_dht._run_client_lookup = function()
    network_lookup_called = true
    return {
      queried = 0,
      closest_peers = {},
      errors = {},
      termination = "test_network",
    }
  end
  local no_cache_op, no_cache_op_err = synth_dht:find_value(synth_key, {
    use_cache = false,
    peers = {
      { peer_id = "unused", addrs = { "/ip4/1.1.1.1/tcp/4001" } },
    },
  })
  if not no_cache_op then
    return nil, no_cache_op_err
  end
  local no_cache_result, no_cache_err = no_cache_op:result({ timeout = 1 })
  if not no_cache_result then
    return nil, no_cache_err
  end
  if not network_lookup_called or no_cache_result.lookup.termination ~= "test_network" then
    return nil, "find_value use_cache=false should bypass local /pk cache"
  end

  local peerstore_identity = assert(keys.generate_keypair("ed25519"))
  local peerstore_peer = assert(keys.peer_id(peerstore_identity))
  local peerstore_key = "/pk/" .. peerstore_peer.id
  synth_host.peerstore:merge(peerstore_peer.id, {
    public_key = peerstore_identity.public_key_proto,
  })
  local peerstore_record = assert(synth_dht:_synthesize_pk_record(peerstore_key))
  if peerstore_record.value ~= peerstore_identity.public_key_proto then
    return nil, "/pk synthesis should use peerstore public key"
  end

  local missing_pk, missing_pk_err = synth_dht:_synthesize_pk_record("/pk/not-a-peer-id")
  if missing_pk ~= nil or not missing_pk_err then
    return nil, "malformed /pk synthesis should return parse error"
  end

  local remote_identity = assert(keys.generate_keypair("ed25519"))
  local remote_peer = assert(keys.peer_id(remote_identity))
  local remote_key = "/pk/" .. remote_peer.id
  synth_dht._rpc = function(_, _, request, expected_type)
    if request.type ~= kad_protocol.MESSAGE_TYPE.GET_VALUE or request.key ~= remote_key then
      return nil, "unexpected remote /pk request"
    end
    return {
      type = expected_type,
      key = remote_key,
      record = {
        key = remote_key,
        value = remote_identity.public_key_proto,
      },
      closer_peers = {},
    }
  end
  local remote_result, remote_result_err = synth_dht:_get_value("peer-a", remote_key)
  if not remote_result then
    return nil, remote_result_err
  end
  local remote_peerstore = synth_host.peerstore:get(remote_peer.id)
  if not remote_peerstore or remote_peerstore.public_key ~= remote_identity.public_key_proto then
    return nil, "remote /pk GET_VALUE should merge public key into peerstore"
  end

  local bad_remote_identity = assert(keys.generate_keypair("ed25519"))
  synth_dht._rpc = function(_, _, _, expected_type)
    return {
      type = expected_type,
      key = remote_key,
      record = {
        key = remote_key,
        value = bad_remote_identity.public_key_proto,
      },
      closer_peers = {},
    }
  end
  local bad_remote, bad_remote_err = synth_dht:_get_value("peer-a", remote_key)
  if bad_remote ~= nil or not bad_remote_err or bad_remote_err.kind ~= "protocol" then
    return nil, "remote mismatched /pk record should reject before peerstore merge"
  end

  local has_luasql = pcall(require, "luasql.sqlite3")
  if has_luasql then
    local path = os.tmpname()
    os.remove(path)
    local sqlite_store, sqlite_store_err = sqlite.new({ path = path })
    if not sqlite_store then
      os.remove(path)
      return nil, sqlite_store_err
    end
    local persisted_dht = assert(kad_dht.new(new_task_host(), {
      mode = "server",
      hash_function = fake_hash,
      address_filter = "all",
      datastore = sqlite_store,
    }))
    local persisted, persisted_err = persisted_dht:put_local_record("record-key", {
      key = "record-key",
      value = "record-value",
    })
    if not persisted then
      sqlite_store:close()
      os.remove(path)
      return nil, persisted_err
    end
    sqlite_store:close()

    local reopened_store, reopened_store_err = sqlite.new({ path = path })
    if not reopened_store then
      os.remove(path)
      return nil, reopened_store_err
    end
    local reopened_dht = assert(kad_dht.new(new_task_host(), {
      mode = "server",
      hash_function = fake_hash,
      address_filter = "all",
      datastore = reopened_store,
    }))
    local persisted_lookup_op, persisted_lookup_op_err = reopened_dht:find_value("record-key", { use_network = false })
    if not persisted_lookup_op then
      reopened_store:close()
      os.remove(path)
      return nil, persisted_lookup_op_err
    end
    local cache_only, cache_only_err = persisted_lookup_op:result({ timeout = 1 })
    if not cache_only then
      reopened_store:close()
      os.remove(path)
      return nil, cache_only_err
    end
    if not cache_only.record or cache_only.record.value ~= "record-value" or cache_only.lookup.termination ~= "local_record" then
      reopened_store:close()
      os.remove(path)
      return nil, "sqlite-backed record store should reload values in a new dht"
    end
    reopened_store:close()
    os.remove(path)

    local expired_path = os.tmpname()
    os.remove(expired_path)
    local expired_now = 1000
    local expired_store, expired_store_err = sqlite.new({ path = expired_path })
    if not expired_store then
      os.remove(expired_path)
      return nil, expired_store_err
    end
    local expiring_dht = assert(kad_dht.new(new_task_host(), {
      mode = "server",
      hash_function = fake_hash,
      address_filter = "all",
      datastore = expired_store,
      record_ttl_seconds = 10,
      now = function()
        return expired_now
      end,
    }))
    local expiring, expiring_err = expiring_dht:put_local_record("record-key", {
      key = "record-key",
      value = "record-value",
    })
    if not expiring then
      expired_store:close()
      os.remove(expired_path)
      return nil, expiring_err
    end
    expired_store:close()

    expired_now = 1011
    local expired_reopened_store, expired_reopened_store_err = sqlite.new({ path = expired_path })
    if not expired_reopened_store then
      os.remove(expired_path)
      return nil, expired_reopened_store_err
    end
    local expired_reopened_dht = assert(kad_dht.new(new_task_host(), {
      mode = "server",
      hash_function = fake_hash,
      address_filter = "all",
      datastore = expired_reopened_store,
      record_ttl_seconds = 10,
      now = function()
        return expired_now
      end,
    }))
    local expired_lookup_op, expired_lookup_op_err = expired_reopened_dht:find_value("record-key", { use_network = false })
    if not expired_lookup_op then
      expired_reopened_store:close()
      os.remove(expired_path)
      return nil, expired_lookup_op_err
    end
    local expired_lookup, expired_lookup_err = expired_lookup_op:result({ timeout = 1 })
    if not expired_lookup then
      expired_reopened_store:close()
      os.remove(expired_path)
      return nil, expired_lookup_err
    end
    if expired_lookup.record ~= nil or expired_lookup.lookup.termination ~= "network_disabled" then
      expired_reopened_store:close()
      os.remove(expired_path)
      return nil, "sqlite-backed record store should not reload expired records"
    end
    expired_reopened_store:close()
    os.remove(expired_path)
  end

  return true
end

return {
  name = "kad-dht record store and value RPCs",
  run = run,
}
