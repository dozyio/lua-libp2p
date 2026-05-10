--- KAD-DHT provider routing workflows.
-- @module lua_libp2p.kad_dht.provider_routing
local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log").subsystem("kad_dht")
local multiaddr = require("lua_libp2p.multiaddr")
local multihash = require("lua_libp2p.multiformats.multihash")
local peerid = require("lua_libp2p.peerid")
local protocol = require("lua_libp2p.kad_dht.protocol")

local M = {}

-- Provider keys are CID multihashes on the IPFS KAD-DHT wire protocol. The
-- routing spec caps ADD_PROVIDER keys at 80 bytes to reject oversized records
-- before they reach provider storage or routing-table lookups.
M.MAX_PROVIDER_KEY_BYTES = 80

function M.validate_provider_key(key, context)
  if type(key) ~= "string" or key == "" then
    return nil, error_mod.new("input", (context or "provider") .. " key must be non-empty multihash bytes")
  end
  if #key > M.MAX_PROVIDER_KEY_BYTES then
    return nil,
      error_mod.new("input", (context or "provider") .. " key exceeds 80 byte limit", {
        limit = M.MAX_PROVIDER_KEY_BYTES,
        actual = #key,
      })
  end
  local decoded, decode_err = multihash.decode(key)
  if not decoded then
    return nil,
      error_mod.new("input", (context or "provider") .. " key must be a valid multihash", { cause = decode_err })
  end
  return true
end

local function decode_peer_id_text(id_bytes)
  local parsed = peerid and peerid.parse and peerid.parse(id_bytes)
  if parsed and parsed.id then
    return parsed.id
  end
  local has_non_printable = false
  for i = 1, #id_bytes do
    local b = id_bytes:byte(i)
    if b < 32 or b > 126 then
      has_non_printable = true
      break
    end
  end
  if has_non_printable then
    return peerid.to_base58(id_bytes)
  end
  return id_bytes
end

local function decode_kad_multiaddrs(addrs)
  local out = {}
  for _, addr in ipairs(addrs or {}) do
    if type(addr) == "string" and addr:sub(1, 1) == "/" then
      out[#out + 1] = addr
    elseif type(addr) == "string" and addr ~= "" then
      local parsed = multiaddr.from_bytes(addr)
      if parsed and parsed.text then
        out[#out + 1] = parsed.text
      end
    end
  end
  return out
end

local function provider_record_to_wire(entry)
  return { id = protocol.peer_bytes(entry.peer_id), addrs = entry.addrs or {} }
end

function M.local_provider_info(dht, opts)
  local options = opts or {}
  local addrs = options.addrs
  if addrs == nil and dht.host and type(dht.host.get_multiaddrs_raw) == "function" then
    addrs = dht.host:get_multiaddrs_raw()
  end
  if addrs == nil and dht.host and type(dht.host.get_listen_addrs) == "function" then
    addrs = dht.host:get_listen_addrs()
  end
  if addrs == nil and dht.host then
    addrs = dht.host.listen_addrs
  end
  if addrs ~= nil and type(addrs) ~= "table" then
    return nil, error_mod.new("input", "provider addrs must be a list")
  end
  local filtered = dht:_filter_addrs(addrs or {}, {
    peer_id = dht.local_peer_id,
    purpose = "provide",
  })
  return { peer_id = dht.local_peer_id, addrs = filtered }
end

function M.handle_add_provider(dht, req)
  local key = req.key
  local valid_key, key_err = M.validate_provider_key(key, "ADD_PROVIDER")
  if not valid_key then
    return nil, key_err
  end
  if dht.mode ~= "server" then
    return nil, error_mod.new("unsupported", "client-mode dht does not accept provider records")
  end

  for _, provider_peer in ipairs(req.provider_peers or {}) do
    local peer_id_text = decode_peer_id_text(provider_peer.id)
    local addrs = dht:_filter_addrs(decode_kad_multiaddrs(provider_peer.addrs), {
      peer_id = peer_id_text,
      purpose = "provider_record",
    })
    local ok, add_err = dht:add_provider(key, { peer_id = peer_id_text, addrs = addrs })
    if not ok then
      return nil, add_err
    end
    if dht.host and dht.host.peerstore and peer_id_text then
      dht.host.peerstore:merge(peer_id_text, { addrs = addrs }, { ttl = dht.provider_addr_ttl_seconds })
    end
  end

  -- ADD_PROVIDER has no response payload; avoid building unused closer peers.
  return nil
end

function M.handle_get_providers(dht, req)
  local key = req.key
  local valid_key, key_err = M.validate_provider_key(key, "GET_PROVIDERS")
  if not valid_key then
    return nil, key_err
  end
  local local_providers, providers_err = dht:get_local_providers(key, { limit = dht.k })
  if not local_providers then
    return nil, providers_err
  end
  local provider_peers = {}
  for _, entry in ipairs(local_providers) do
    provider_peers[#provider_peers + 1] = provider_record_to_wire(entry)
  end
  local closer, closer_err = dht:_closest_peer_records(key, dht.k)
  if not closer then
    return nil, closer_err
  end
  return {
    type = protocol.MESSAGE_TYPE.GET_PROVIDERS,
    key = key,
    provider_peers = provider_peers,
    closer_peers = closer,
  }
end

function M.add_provider(dht, peer_or_addr, key, provider_info, opts)
  local valid_key, key_err = M.validate_provider_key(key, "ADD_PROVIDER")
  if not valid_key then
    return nil, key_err
  end
  local rpc_opts = {}
  for k, v in pairs(opts or {}) do
    rpc_opts[k] = v
  end
  -- ADD_PROVIDER has no response payload in the spec; go-libp2p/js-libp2p store and close.
  rpc_opts.add_provider = true
  local response, err = dht:_rpc(peer_or_addr, {
    type = protocol.MESSAGE_TYPE.ADD_PROVIDER,
    key = key,
    provider_peers = { provider_record_to_wire(provider_info or {}) },
  }, protocol.MESSAGE_TYPE.ADD_PROVIDER, rpc_opts)
  if not response then
    return nil, err
  end
  return { key = response.key, closer_peers = dht:_decode_response_peers(response, "add_provider_result") }
end

function M.get_providers(dht, peer_or_addr, key, opts)
  local valid_key, key_err = M.validate_provider_key(key, "GET_PROVIDERS")
  if not valid_key then
    return nil, key_err
  end
  local response, err = dht:_rpc(peer_or_addr, {
    type = protocol.MESSAGE_TYPE.GET_PROVIDERS,
    key = key,
  }, protocol.MESSAGE_TYPE.GET_PROVIDERS, opts)
  if not response then
    return nil, err
  end
  local closer, providers = dht:_decode_response_peers(response, "get_providers_result")
  return { providers = providers, provider_peers = providers, closer_peers = closer, key = response.key }
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

function M.find_providers(dht, key, opts)
  local options = opts or {}
  local valid_key, key_err = M.validate_provider_key(key, "find_providers")
  if not valid_key then
    return nil, key_err
  end
  log.debug("kad dht find providers started", {
    key_size = #key,
    limit = options.limit or 1,
  })
  local providers = {}
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
    local result, err = dht:_get_providers(
      peer.addr or (peer.addrs and peer.addrs[1]) or { peer_id = peer.peer_id, addrs = peer.addrs },
      key,
      query_options
    )
    if not result then
      return nil, err
    end
    for _, provider in ipairs(result.providers or {}) do
      providers[#providers + 1] = provider
      log.debug("kad dht provider found", {
        key_size = #key,
        provider_peer_id = provider.peer_id,
        via_peer_id = peer.peer_id,
      })
      if #providers >= (options.limit or 1) then
        return { closer_peers = result.closer_peers, stop = true }
      end
    end
    return { closer_peers = result.closer_peers }
  end, options)
  if not lookup then
    return nil, lookup_err
  end
  log.debug("kad dht find providers completed", {
    key_size = #key,
    providers = #providers,
    queried = lookup.queried,
    termination = lookup.termination,
  })
  return {
    providers = providers,
    provider_peers = providers,
    closer_peers = lookup.closest_peers,
    lookup = lookup,
    errors = lookup.errors,
  }
end

function M.provide(dht, key, opts)
  local options = opts or {}
  local valid_key, key_err = M.validate_provider_key(key, "provide")
  if not valid_key then
    return nil, key_err
  end
  local provider_info, provider_err = dht:_local_provider_info(options.provider or options)
  if not provider_info then
    return nil, provider_err
  end
  local stored, store_err = dht:add_provider(key, provider_info, {
    ttl = options.provider_ttl or options.ttl,
    ttl_seconds = options.provider_ttl_seconds or options.ttl_seconds,
  })
  if not stored then
    return nil, store_err
  end
  log.debug("kad dht provide local record stored", {
    key_size = #key,
    provider_peer_id = provider_info.peer_id,
    addrs = #(provider_info.addrs or {}),
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

  local report = {
    key = key,
    provider = provider_info,
    lookup = lookup,
    attempted = 0,
    succeeded = 0,
    failed = 0,
    peers = {},
    errors = {},
  }
  local limit = options.count or dht.k
  log.debug("kad dht provide announcing", {
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
    local result, err = dht:_add_provider(target, key, provider_info, options.add_provider_opts or options)
    if result then
      report.succeeded = report.succeeded + 1
      report.peers[#report.peers + 1] = peer.peer_id or target
      log.debug("kad dht provide peer succeeded", {
        key_size = #key,
        peer_id = peer.peer_id or target,
      })
    else
      report.failed = report.failed + 1
      report.errors[#report.errors + 1] = err
      log.debug("kad dht provide peer failed", {
        key_size = #key,
        peer_id = peer.peer_id or target,
        cause = tostring(err),
      })
      if options.fail_fast then
        return nil, err
      end
    end
  end
  log.debug("kad dht provide completed", {
    key_size = #key,
    attempted = report.attempted,
    succeeded = report.succeeded,
    failed = report.failed,
  })
  return report
end

return M
