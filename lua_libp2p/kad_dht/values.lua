--- KAD-DHT value record workflows.
-- @module lua_libp2p.kad_dht.values
local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log").subsystem("kad_dht")
local protocol = require("lua_libp2p.kad_dht.protocol")
local record_validators = require("lua_libp2p.kad_dht.record_validators")

local M = {}

function M.record_namespace(key)
  if type(key) ~= "string" or key == "" then
    return nil
  end
  if key:sub(1, 1) == "/" then
    local next_slash = key:find("/", 2, true)
    if next_slash then
      return key:sub(2, next_slash - 1)
    end
    return key:sub(2)
  end
  local next_slash = key:find("/", 1, true)
  if next_slash then
    return key:sub(1, next_slash - 1)
  end
  return nil
end

function M.merge_policy_maps(defaults, overrides)
  local out = {}
  for k, v in pairs(defaults or {}) do
    out[k] = v
  end
  for k, v in pairs(overrides or {}) do
    out[k] = v
  end
  return out
end

function M.set_record_policy(map, namespace, fn, policy_name)
  if type(map) ~= "table" then
    return nil, error_mod.new("state", policy_name .. " registry is not initialized")
  end
  if type(namespace) ~= "string" or namespace == "" then
    return nil, error_mod.new("input", policy_name .. " namespace must be non-empty")
  end
  if fn ~= nil and type(fn) ~= "function" then
    return nil, error_mod.new("input", policy_name .. " must be a function or nil")
  end
  map[namespace] = fn
  return true
end

local function host_identity_public_key_proto(host)
  if host and type(host.identity) == "table" and type(host.identity.public_key_proto) == "string" then
    return host.identity.public_key_proto
  end
  return nil
end

local function peerstore_public_key(peerstore, peer_id_text)
  if not peerstore or type(peerstore.get) ~= "function" then
    return nil
  end
  local peer = peerstore:get(peer_id_text)
  if type(peer) == "table" and type(peer.public_key) == "string" then
    return peer.public_key
  end
  return nil
end

function M.validate_record(dht, key, record)
  local namespace = M.record_namespace(key)
  local validator = namespace and dht.record_validators[namespace] or nil
  validator = validator or dht.record_validator
  if validator == nil then
    return true
  end
  if type(validator) ~= "function" then
    return nil, error_mod.new("input", "kad-dht record validator must be a function")
  end
  local ok, valid_or_err, validator_err = pcall(validator, key, record, dht)
  if not ok then
    return nil, error_mod.new("protocol", "kad-dht record validator failed", { cause = valid_or_err })
  end
  if valid_or_err == nil and validator_err ~= nil then
    return nil, validator_err
  end
  if valid_or_err == false then
    return nil, error_mod.new("protocol", "kad-dht record rejected by validator")
  end
  return true
end

function M.select_record(dht, key, incoming)
  local existing, existing_err = dht:get_local_record(key)
  if existing_err then
    return nil, existing_err
  end
  if not existing then
    return true
  end

  local namespace = M.record_namespace(key)
  local selector = namespace and dht.record_selectors[namespace] or nil
  selector = selector or dht.record_selector
  if selector == nil then
    return true
  end
  if type(selector) ~= "function" then
    return nil, error_mod.new("input", "kad-dht record selector must be a function")
  end

  local ok, choice_or_err = pcall(selector, key, existing, incoming, dht)
  if not ok then
    return nil, error_mod.new("protocol", "kad-dht record selector failed", { cause = choice_or_err })
  end
  if choice_or_err == "incoming" or choice_or_err == true or choice_or_err == 2 then
    return true
  end
  if choice_or_err == "existing" or choice_or_err == false or choice_or_err == 1 then
    return false
  end
  return nil,
    error_mod.new("protocol", "kad-dht record selector returned unsupported choice", { choice = choice_or_err })
end

function M.select_best_record(dht, key, existing, incoming)
  if not existing then
    return incoming
  end

  local namespace = M.record_namespace(key)
  local selector = namespace and dht.record_selectors[namespace] or nil
  selector = selector or dht.record_selector
  if selector == nil then
    return existing
  end
  if type(selector) ~= "function" then
    return nil, error_mod.new("input", "kad-dht record selector must be a function")
  end

  local ok, choice_or_err = pcall(selector, key, existing, incoming, dht)
  if not ok then
    return nil, error_mod.new("protocol", "kad-dht record selector failed", { cause = choice_or_err })
  end
  if choice_or_err == "incoming" or choice_or_err == true or choice_or_err == 2 then
    return incoming
  end
  if choice_or_err == "existing" or choice_or_err == false or choice_or_err == 1 then
    return existing
  end
  return nil,
    error_mod.new("protocol", "kad-dht record selector returned unsupported choice", { choice = choice_or_err })
end

function M.synthesize_pk_record(dht, key)
  local pk_peer_id, key_err = record_validators.pk_peer_id_text(key)
  if not pk_peer_id then
    return nil, key_err
  end

  local value
  if pk_peer_id == dht.local_peer_id then
    value = host_identity_public_key_proto(dht.host)
  end
  value = value or peerstore_public_key(dht.host and dht.host.peerstore, pk_peer_id)
  if type(value) ~= "string" or value == "" then
    return nil
  end

  local record = { key = key, value = value }
  local valid, validate_err = M.validate_record(dht, key, record)
  if not valid then
    return nil, validate_err
  end
  return record
end

function M.merge_pk_record(dht, key, record)
  if M.record_namespace(key) ~= "pk" or type(record) ~= "table" or type(record.value) ~= "string" then
    return true
  end
  local pk_peer_id, key_err = record_validators.pk_peer_id_text(key)
  if not pk_peer_id then
    return nil, key_err
  end
  local valid, validate_err = M.validate_record(dht, key, record)
  if not valid then
    return nil, validate_err
  end
  if dht.host and dht.host.peerstore and type(dht.host.peerstore.merge) == "function" then
    local merged, merge_err = dht.host.peerstore:merge(pk_peer_id, { public_key = record.value })
    if not merged then
      return nil, merge_err
    end
  end
  return true
end

function M.local_value_record(dht, key)
  local record, record_err = dht:get_local_record(key)
  if record_err then
    return nil, record_err
  end
  if record then
    return record
  end
  if M.record_namespace(key) == "pk" then
    local synthesized, synth_err = M.synthesize_pk_record(dht, key)
    if synth_err then
      return nil, synth_err
    end
    return synthesized
  end
  return nil
end

function M.handle_put_value(dht, req)
  local key = req.key
  local record = req.record
  if type(key) ~= "string" or key == "" then
    return nil, error_mod.new("input", "PUT_VALUE request missing key")
  end
  if type(record) ~= "table" then
    return nil, error_mod.new("input", "PUT_VALUE request missing record")
  end
  local ok, put_err = dht:put_local_record(key, record)
  if not ok then
    return nil, put_err
  end
  local response_record = dht:get_local_record(key) or record
  local closer, closer_err = dht:_closest_peer_records(key, dht.k)
  if not closer then
    return nil, closer_err
  end
  return { type = protocol.MESSAGE_TYPE.PUT_VALUE, key = key, record = response_record, closer_peers = closer }
end

function M.handle_get_value(dht, req)
  local key = req.key
  if type(key) ~= "string" or key == "" then
    return nil, error_mod.new("input", "GET_VALUE request missing key")
  end
  local record, record_err = M.local_value_record(dht, key)
  if record_err then
    return nil, record_err
  end
  local closer, closer_err = dht:_closest_peer_records(key, dht.k)
  if not closer then
    return nil, closer_err
  end
  return { type = protocol.MESSAGE_TYPE.GET_VALUE, key = key, record = record, closer_peers = closer }
end

function M.get_value(dht, peer_or_addr, key, opts)
  if type(key) ~= "string" or key == "" then
    return nil, error_mod.new("input", "GET_VALUE key must be non-empty bytes")
  end
  local response, err =
    dht:_rpc(peer_or_addr, { type = protocol.MESSAGE_TYPE.GET_VALUE, key = key }, protocol.MESSAGE_TYPE.GET_VALUE, opts)
  if not response then
    return nil, err
  end
  if response.record then
    local valid, validate_err = M.validate_record(dht, response.key or key, response.record)
    if not valid then
      return nil, validate_err
    end
    local merged, merge_err = M.merge_pk_record(dht, response.key or key, response.record)
    if not merged then
      return nil, merge_err
    end
  end
  local closer = dht:_decode_response_peers(response, "get_value_result")
  return { record = response.record, closer_peers = closer, key = response.key }
end

function M.put_value(dht, peer_or_addr, key, record, opts)
  if type(key) ~= "string" or key == "" then
    return nil, error_mod.new("input", "PUT_VALUE key must be non-empty bytes")
  end
  if type(record) ~= "table" then
    return nil, error_mod.new("input", "PUT_VALUE record must be a table")
  end
  local response, err = dht:_rpc(peer_or_addr, {
    type = protocol.MESSAGE_TYPE.PUT_VALUE,
    key = key,
    record = record,
  }, protocol.MESSAGE_TYPE.PUT_VALUE, opts)
  if not response then
    return nil, err
  end
  local closer = dht:_decode_response_peers(response, "put_value_result")
  return { record = response.record, closer_peers = closer, key = response.key }
end

local function seed_candidates_from_routing_table(dht, key, count)
  local nearest = dht:find_closest_peers(key, count or dht.k) or {}
  local out = {}
  for _, entry in ipairs(nearest) do
    local addrs = {}
    if dht.host and dht.host.peerstore then
      local filtered, filtered_err =
        dht:_dialable_tcp_addrs(dht:_filter_addrs(dht.host.peerstore:get_addrs(entry.peer_id), {
          peer_id = entry.peer_id,
          purpose = "client_query_seed",
        }))
      if not filtered then
        return nil, filtered_err
      end
      addrs = filtered
    end
    if #addrs > 0 then
      out[#out + 1] = { peer_id = entry.peer_id, addrs = addrs }
    end
  end
  return out
end

function M.find_value(dht, key, opts)
  local options = opts or {}
  log.debug("kad dht find value started", {
    key_size = type(key) == "string" and #key or nil,
    use_cache = options.use_cache ~= false,
    use_network = options.use_network ~= false,
  })
  if options.use_cache ~= false then
    local local_record, local_record_err = M.local_value_record(dht, key)
    if local_record_err then
      return nil, local_record_err
    end
    if local_record and local_record.value ~= nil then
      log.debug("kad dht find value cache hit", {
        key_size = #key,
      })
      return {
        key = key,
        record = local_record,
        closer_peers = {},
        lookup = { target = key, queried = {}, closest_peers = {}, errors = {}, termination = "local_record" },
      }
    end
  end
  if options.use_network == false then
    log.debug("kad dht find value network disabled", {
      key_size = #key,
    })
    return {
      key = key,
      record = nil,
      closer_peers = {},
      lookup = { target = key, queried = {}, closest_peers = {}, errors = {}, termination = "network_disabled" },
    }
  end

  local best_record
  local best_key = key
  local found_count = 0
  local seen_records = {}
  local seed_peers = options.peers
  if not seed_peers then
    local lookup_err
    seed_peers, lookup_err = seed_candidates_from_routing_table(dht, key, dht.k)
    if not seed_peers then
      return nil, lookup_err
    end
  end
  local lookup, lookup_err = dht:_run_client_lookup(key, seed_peers, function(peer, ctx)
    local query_options = options
    if ctx then
      query_options = {}
      for k, v in pairs(options) do
        query_options[k] = v
      end
      query_options.ctx = ctx
    end
    local result, err = dht:_get_value(
      peer.addr or (peer.addrs and peer.addrs[1]) or { peer_id = peer.peer_id, addrs = peer.addrs },
      key,
      query_options
    )
    if not result then
      return nil, err
    end
    if result.record and result.record.value ~= nil then
      local candidate_key = result.key or key
      local selected, select_err = M.select_best_record(dht, candidate_key, best_record, result.record)
      if not selected then
        return nil, select_err
      end
      best_record = selected
      best_key = candidate_key
      found_count = found_count + 1
      log.debug("kad dht value record found", {
        key_size = #candidate_key,
        via_peer_id = peer.peer_id,
        found_records = found_count,
      })
      seen_records[#seen_records + 1] = { peer = peer, key = candidate_key, record = result.record }
    end
    return { closer_peers = result.closer_peers }
  end, options)
  if not lookup then
    return nil, lookup_err
  end
  if best_record then
    if options.correct_stale_records ~= false then
      for _, item in ipairs(seen_records) do
        if item.key == best_key and item.record.value ~= best_record.value then
          log.debug("kad dht correcting stale value record", {
            key_size = #best_key,
            peer_id = item.peer.peer_id,
          })
          dht:_put_value(
            item.peer.addr
              or (item.peer.addrs and item.peer.addrs[1])
              or { peer_id = item.peer.peer_id, addrs = item.peer.addrs },
            best_key,
            best_record,
            options
          )
        end
      end
    end
    log.debug("kad dht find value completed", {
      key_size = #best_key,
      found_records = found_count,
      queried = lookup.queried,
      termination = lookup.termination,
    })
    return {
      key = best_key,
      record = best_record,
      closer_peers = lookup.closest_peers,
      lookup = lookup,
      found_records = found_count,
    }
  end
  log.debug("kad dht find value completed", {
    key_size = type(key) == "string" and #key or nil,
    found_records = 0,
    queried = lookup.queried,
    termination = lookup.termination,
  })
  return { record = nil, closer_peers = lookup.closest_peers, lookup = lookup, errors = lookup.errors }
end

local function normalize_value_record(key, value_or_record)
  if type(key) ~= "string" or key == "" then
    return nil, error_mod.new("input", "put_value key must be non-empty bytes")
  end
  if type(value_or_record) == "table" then
    local record_key = value_or_record.key or key
    if record_key ~= key then
      return nil, error_mod.new("input", "put_value record key does not match key")
    end
    if type(value_or_record.value) ~= "string" then
      return nil, error_mod.new("input", "put_value record value must be bytes")
    end
    return {
      key = key,
      value = value_or_record.value,
      time_received = value_or_record.time_received or value_or_record.timeReceived,
    }
  end
  if type(value_or_record) ~= "string" then
    return nil, error_mod.new("input", "put_value value must be bytes or a record table")
  end
  return { key = key, value = value_or_record }
end

function M.put_value_workflow(dht, key, value_or_record, opts)
  local options = opts or {}
  local record, record_err = normalize_value_record(key, value_or_record)
  if not record then
    return nil, record_err
  end

  local stored, store_err = dht:put_local_record(key, record, {
    ttl = options.record_ttl or options.ttl,
    ttl_seconds = options.record_ttl_seconds or options.ttl_seconds,
  })
  if not stored then
    return nil, store_err
  end
  record = dht:get_local_record(key) or record
  log.debug("kad dht put value local record stored", {
    key_size = #key,
    value_size = type(record.value) == "string" and #record.value or nil,
  })

  local peers = options.peers
  local lookup
  if not peers then
    local closest, closest_lookup_or_err = dht:_get_closest_peers(key, {
      peers = options.seed_peers,
      count = options.count or dht.k,
      alpha = options.alpha,
      k = options.k,
      disjoint_paths = options.disjoint_paths,
      scheduler_task = options.scheduler_task,
      ctx = options.ctx,
      yield = options.yield,
      query_timeout_seconds = options.query_timeout_seconds,
      max_query_rounds = options.max_query_rounds,
    })
    if not closest then
      return nil, closest_lookup_or_err
    end
    peers = closest
    lookup = closest_lookup_or_err
  end

  local report =
    { key = key, record = record, lookup = lookup, attempted = 0, succeeded = 0, failed = 0, peers = {}, errors = {} }
  local limit = options.count or dht.k
  log.debug("kad dht put value announcing", {
    key_size = #key,
    peers = #(peers or {}),
    limit = limit,
  })
  for _, peer in ipairs(peers or {}) do
    if limit and report.attempted >= limit then
      break
    end
    local target = peer.addr or (peer.addrs and peer.addrs[1]) or { peer_id = peer.peer_id, addrs = peer.addrs }
    report.attempted = report.attempted + 1
    local result, err = dht:_put_value(target, key, record, options.put_value_opts or options)
    if result then
      report.succeeded = report.succeeded + 1
      report.peers[#report.peers + 1] = peer.peer_id or target
      log.debug("kad dht put value peer succeeded", {
        key_size = #key,
        peer_id = peer.peer_id or target,
      })
    else
      report.failed = report.failed + 1
      report.errors[#report.errors + 1] = err
      log.debug("kad dht put value peer failed", {
        key_size = #key,
        peer_id = peer.peer_id or target,
        cause = tostring(err),
      })
      if options.fail_fast then
        return nil, err
      end
    end
  end
  log.debug("kad dht put value completed", {
    key_size = #key,
    attempted = report.attempted,
    succeeded = report.succeeded,
    failed = report.failed,
  })
  return report
end

return M
