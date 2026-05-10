local kad_dht = require("lua_libp2p.kad_dht")
local protocol = require("lua_libp2p.kad_dht.protocol")
local query = require("lua_libp2p.kad_dht.query")

local M = {
  name = "kad-dht hot paths",
}

local function hash32(n)
  local bytes = {}
  for i = 1, 32 do
    bytes[i] = string.char((math.floor(n / (i + 3)) + i * 17) % 256)
  end
  return table.concat(bytes)
end

local function fake_hash(value)
  if value == "local" then
    return string.rep("\0", 32)
  end
  if value == "target" then
    return string.char(128) .. string.rep("\0", 31)
  end
  local index = tostring(value):match("peer%-(%d+)")
  if index then
    return hash32(tonumber(index) or 0)
  end
  return hash32(#tostring(value))
end

local function elapsed_seconds(fn)
  collectgarbage("collect")
  local started = os.clock()
  fn()
  return os.clock() - started
end

local function print_result(name, iterations, elapsed)
  local per_iter_ms = iterations > 0 and (elapsed * 1000 / iterations) or 0
  io.stdout:write(
    string.format(
      "  %-34s iterations=%d total_ms=%.2f per_iter_ms=%.4f\n",
      name,
      iterations,
      elapsed * 1000,
      per_iter_ms
    )
  )
end

local function new_host(peer_count, addrs_per_peer)
  local peerstore = { addrs = {} }
  for i = 1, peer_count do
    local peer_id = string.format("peer-%04d", i)
    local addrs = {}
    for j = 1, addrs_per_peer do
      addrs[j] = string.format("/ip4/8.%d.%d.%d/tcp/4001", i % 250, j % 250, (i + j) % 250)
    end
    peerstore.addrs[peer_id] = addrs
  end

  function peerstore:get_addrs(peer_id)
    return self.addrs[peer_id] or {}
  end

  local host = {
    _peer = { id = "local" },
    listen_addrs = { "/ip4/0.0.0.0/tcp/4001" },
    peerstore = peerstore,
  }

  function host:peer_id()
    return self._peer
  end

  function host:is_dialable_addr(addr)
    return type(addr) == "string" and addr:sub(1, 5) == "/ip4/"
  end

  function host:filter_dialable_addrs(addrs)
    local out = {}
    for _, addr in ipairs(addrs or {}) do
      if self:is_dialable_addr(addr) then
        out[#out + 1] = addr
      end
    end
    return out
  end

  return host
end

local function copy_list(values)
  local out = {}
  for i, value in ipairs(values or {}) do
    out[i] = value
  end
  return out
end

local function new_dht(peer_count, addrs_per_peer, opts)
  local options = opts or {}
  local host = new_host(peer_count, addrs_per_peer)
  local dht = assert(kad_dht.new(host, {
    mode = "server",
    k = peer_count,
    bucket_size = peer_count,
    hash_function = fake_hash,
    address_filter = options.address_filter or "all",
    filtered_addr_cache_ttl_seconds = options.filtered_addr_cache_ttl_seconds,
    filtered_addr_cache_size = options.filtered_addr_cache_size,
  }))
  for i = 1, peer_count do
    assert(dht:add_peer(string.format("peer-%04d", i), {
      allow_replace = true,
      skip_kad_protocol_filter = true,
    }))
  end
  return dht
end

local function clear_distances(values)
  for _, value in pairs(values) do
    if type(value) == "table" then
      value._distance = nil
      value._distance_target_hash = nil
    end
  end
end

local function bench_closest_peer_records()
  local dht = new_dht(256, 2)
  local iterations = 400
  local elapsed = elapsed_seconds(function()
    for _ = 1, iterations do
      local records = assert(dht:_closest_peer_records("target", 20))
      if #records == 0 then
        error("expected closest peer records")
      end
    end
  end)
  print_result("closest_peer_records", iterations, elapsed)
end

local function bench_peer_record_filtering()
  local dht = new_dht(256, 4, { address_filter = "public", filtered_addr_cache_ttl_seconds = 0 })
  local peer_ids = {}
  for i = 1, 256 do
    peer_ids[i] = string.format("peer-%04d", i)
  end

  local iterations = 20000
  local current_elapsed = elapsed_seconds(function()
    for i = 1, iterations do
      local peer_id = peer_ids[((i - 1) % #peer_ids) + 1]
      local record = assert(dht:_peer_record(peer_id, "response"))
      if #record.addrs == 0 then
        error("expected filtered addrs")
      end
    end
  end)
  print_result("peer_record_current_public", iterations, current_elapsed)

  local production_cache_dht = new_dht(256, 4, { address_filter = "public" })
  local production_cache_elapsed = elapsed_seconds(function()
    for i = 1, iterations do
      local peer_id = peer_ids[((i - 1) % #peer_ids) + 1]
      local record = assert(production_cache_dht:_peer_record(peer_id, "response"))
      if #record.addrs == 0 then
        error("expected production cached filtered addrs")
      end
    end
  end)
  print_result("peer_record_prod_addr_cache", iterations, production_cache_elapsed)

  local filtered_addr_cache = {}
  local function peer_record_with_addr_cache(peer_id, purpose)
    local id_bytes = assert(protocol.peer_bytes(peer_id))
    local cache_key = tostring(purpose or "response") .. "\0" .. peer_id
    local addrs = filtered_addr_cache[cache_key]
    if not addrs then
      addrs = dht:_filter_addrs(dht.host.peerstore:get_addrs(peer_id), {
        peer_id = peer_id,
        purpose = purpose or "response",
      })
      filtered_addr_cache[cache_key] = addrs
    end
    return { id = id_bytes, addrs = addrs }
  end

  local addr_cache_elapsed = elapsed_seconds(function()
    for i = 1, iterations do
      local peer_id = peer_ids[((i - 1) % #peer_ids) + 1]
      local record = peer_record_with_addr_cache(peer_id, "response")
      if #record.addrs == 0 then
        error("expected cached filtered addrs")
      end
    end
  end)
  print_result("peer_record_filtered_addr_cache", iterations, addr_cache_elapsed)

  local record_cache = {}
  local function cached_peer_record(peer_id, purpose)
    local cache_key = tostring(purpose or "response") .. "\0" .. peer_id
    local record = record_cache[cache_key]
    if not record then
      record = assert(dht:_peer_record(peer_id, purpose))
      record_cache[cache_key] = record
    end
    return record
  end

  local record_cache_elapsed = elapsed_seconds(function()
    for i = 1, iterations do
      local peer_id = peer_ids[((i - 1) % #peer_ids) + 1]
      local record = cached_peer_record(peer_id, "response")
      if #record.addrs == 0 then
        error("expected cached record addrs")
      end
    end
  end)
  print_result("peer_record_full_record_cache", iterations, record_cache_elapsed)

  local closest_iterations = 400
  local closest_current_elapsed = elapsed_seconds(function()
    for _ = 1, closest_iterations do
      local records = assert(dht:_closest_peer_records("target", 20))
      if #records == 0 then
        error("expected public closest records")
      end
    end
  end)
  print_result("closest_peer_records_public", closest_iterations, closest_current_elapsed)

  local closest_prod_cache_dht = new_dht(256, 4, { address_filter = "public" })
  local closest_prod_cache_elapsed = elapsed_seconds(function()
    for _ = 1, closest_iterations do
      local records = assert(closest_prod_cache_dht:_closest_peer_records("target", 20))
      if #records == 0 then
        error("expected production cached closest records")
      end
    end
  end)
  print_result("closest_prod_addr_cache", closest_iterations, closest_prod_cache_elapsed)

  local function closest_with_record_cache()
    local nearest = assert(dht:find_closest_peers("target", 22))
    local peers = {}
    for _, entry in ipairs(nearest) do
      if #peers >= 20 then
        break
      end
      local record = cached_peer_record(entry.peer_id, "response")
      if #record.addrs > 0 then
        peers[#peers + 1] = {
          id = record.id,
          addrs = copy_list(record.addrs),
        }
      end
    end
    return peers
  end
  local closest_cache_elapsed = elapsed_seconds(function()
    for _ = 1, closest_iterations do
      local records = closest_with_record_cache()
      if #records == 0 then
        error("expected cached closest records")
      end
    end
  end)
  print_result("closest_with_record_cache", closest_iterations, closest_cache_elapsed)
end

local function bench_query_sort_and_complete()
  local dht = new_dht(512, 1)
  local target_hash = assert(dht.routing_table:_hash("target"))
  local candidates = {}
  local states = {}
  for i = 1, 512 do
    local peer = {
      peer_id = string.format("peer-%04d", i),
      state = i <= 20 and "queried" or "heard",
    }
    candidates[#candidates + 1] = peer
    states[peer.peer_id] = peer
  end

  local cold_iterations = 80
  local cold_elapsed = elapsed_seconds(function()
    for _ = 1, cold_iterations do
      clear_distances(candidates)
      query.sort_candidates(dht, target_hash, candidates)
    end
  end)
  print_result("query_sort_cold", cold_iterations, cold_elapsed)

  local warm_iterations = 800
  query.sort_candidates(dht, target_hash, candidates)
  local warm_elapsed = elapsed_seconds(function()
    for _ = 1, warm_iterations do
      query.sort_candidates(dht, target_hash, candidates)
    end
  end)
  print_result("query_sort_warm", warm_iterations, warm_elapsed)

  clear_distances(states)
  local complete_iterations = 800
  local complete_elapsed = elapsed_seconds(function()
    for _ = 1, complete_iterations do
      query.strict_complete(dht, target_hash, states, 20)
    end
  end)
  print_result("query_strict_complete", complete_iterations, complete_elapsed)
end

function M.run()
  bench_closest_peer_records()
  bench_peer_record_filtering()
  bench_query_sort_and_complete()
  return true
end

return M
