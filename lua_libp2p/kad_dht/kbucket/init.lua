--- Kademlia k-bucket routing table primitives.
local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log").subsystem("ktable")
local peerid = require("lua_libp2p.peerid")

local ok_sodium, sodium = pcall(require, "luasodium")
if not ok_sodium then
  error("luasodium is required for kbucket support")
end

local M = {}

M.DEFAULT_BUCKET_SIZE = 20
M.KEYSPACE_BITS = 256

local RoutingTable = {}
RoutingTable.__index = RoutingTable

local function count_leading_zero_bits(byte)
  local n = 0
  for i = 7, 0, -1 do
    if ((byte >> i) & 0x01) == 0 then
      n = n + 1
    else
      break
    end
  end
  return n
end

local function cpl_from_hashes(left, right)
  local limit = #left
  if #right < limit then
    limit = #right
  end

  local matched_bits = 0
  for i = 1, limit do
    local xor_byte = (left:byte(i) ~ right:byte(i))
    if xor_byte == 0 then
      matched_bits = matched_bits + 8
    else
      matched_bits = matched_bits + count_leading_zero_bits(xor_byte)
      break
    end
  end
  return matched_bits
end

local function xor_distance(left, right)
  local out = {}
  for i = 1, #left do
    out[i] = string.char(left:byte(i) ~ right:byte(i))
  end
  return table.concat(out)
end

local function compare_distance(left, right)
  local size = #left
  if #right < size then
    size = #right
  end
  for i = 1, size do
    local a = left:byte(i)
    local b = right:byte(i)
    if a < b then
      return -1
    end
    if a > b then
      return 1
    end
  end
  if #left < #right then
    return -1
  end
  if #left > #right then
    return 1
  end
  return 0
end

local function remove_from_list(list, value)
  for i = 1, #list do
    if list[i] == value then
      table.remove(list, i)
      return true
    end
  end
  return false
end

local function normalize_key_bytes(raw)
  local value = raw
  if type(value) == "table" then
    if type(value.bytes) == "string" then
      return value.bytes
    end
    if type(value.id) == "string" then
      value = value.id
    else
      return nil, error_mod.new("input", "key table must include id or bytes")
    end
  end

  if type(value) ~= "string" or value == "" then
    return nil, error_mod.new("input", "key must be a non-empty string")
  end

  local parsed = peerid.parse(value)
  if parsed then
    return parsed.bytes
  end

  return value
end

local function normalize_peer_id(raw)
  local value = raw
  if type(value) == "table" then
    if type(value.id) == "string" then
      value = value.id
    elseif type(value.bytes) == "string" then
      local parsed, parse_err = peerid.parse(peerid.to_base58(value.bytes))
      if not parsed then
        return nil, nil, parse_err
      end
      return parsed.id, parsed.bytes
    else
      return nil, nil, error_mod.new("input", "peer must include id or bytes")
    end
  end

  if type(value) ~= "string" or value == "" then
    return nil, nil, error_mod.new("input", "peer id must be a non-empty string")
  end

  local parsed = peerid.parse(value)
  if parsed then
    return parsed.id, parsed.bytes
  end
  return value, value
end

local function copy_entry(entry)
  return {
    peer_id = entry.peer_id,
    last_seen = entry.last_seen,
    bucket = entry.bucket,
  }
end

local function default_hash(value)
  return sodium.crypto_hash_sha256(value)
end

function M.common_prefix_length(left_key, right_key)
  local left_bytes, left_err = normalize_key_bytes(left_key)
  if not left_bytes then
    return nil, left_err
  end
  local right_bytes, right_err = normalize_key_bytes(right_key)
  if not right_bytes then
    return nil, right_err
  end

  local left_hash = default_hash(left_bytes)
  local right_hash = default_hash(right_bytes)
  return cpl_from_hashes(left_hash, right_hash)
end

function RoutingTable:_hash(value)
  local hash = self._hash_function(value)
  if type(hash) ~= "string" or #hash ~= 32 then
    return nil, error_mod.new("input", "hash function must return 32-byte digest")
  end
  return hash
end

function RoutingTable:_bucket_for_hash(hash)
  local cpl = cpl_from_hashes(self._local_hash, hash)
  if cpl >= M.KEYSPACE_BITS then
    return nil
  end
  return cpl + 1
end

--- Attempt to add peer to routing table.
-- `opts.allow_replace=true` permits evicting the oldest bucket entry.
-- @param peer Peer id or peer descriptor.
--- opts? table
--- true|false|nil added_or_refreshed
--- string|table|nil evicted_or_err
function RoutingTable:try_add_peer(peer, opts)
  local options = opts or {}
  local peer_id, key_bytes, peer_err = normalize_peer_id(peer)
  if not peer_id then
    return nil, peer_err
  end

  local hashed, hash_err = self:_hash(key_bytes)
  if not hashed then
    return nil, hash_err
  end

  local bucket_index = self:_bucket_for_hash(hashed)
  if not bucket_index then
    return nil, error_mod.new("input", "cannot add local peer id to routing table")
  end

  local existing = self._entries[peer_id]
  if existing then
    existing.last_seen = os.time()
    remove_from_list(self._buckets[existing.bucket], existing)
    table.insert(self._buckets[existing.bucket], 1, existing)
    log.debug("routing table peer refreshed", {
      peer_id = peer_id,
      bucket = existing.bucket,
      size = self:size(),
    })
    return false
  end

  local bucket = self._buckets[bucket_index]
  local evicted = nil
  if #bucket >= self.bucket_size then
    if not options.allow_replace then
      log.debug("routing table peer rejected capacity", {
        peer_id = peer_id,
        bucket = bucket_index,
        bucket_size = #bucket,
        max_bucket_size = self.bucket_size,
      })
      return nil, error_mod.new("capacity", "kbucket is full", { bucket = bucket_index })
    end
    local old = table.remove(bucket)
    if old then
      self._entries[old.peer_id] = nil
      evicted = old.peer_id
      log.debug("routing table peer evicted", {
        peer_id = old.peer_id,
        bucket = bucket_index,
        replacement_peer_id = peer_id,
      })
    end
  end

  local entry = {
    peer_id = peer_id,
    key_bytes = key_bytes,
    hash = hashed,
    bucket = bucket_index,
    last_seen = os.time(),
  }
  table.insert(bucket, 1, entry)
  self._entries[peer_id] = entry

  log.debug("routing table peer added", {
    peer_id = peer_id,
    bucket = bucket_index,
    size = self:size(),
    evicted_peer_id = evicted,
  })

  return true, evicted
end

function RoutingTable:remove_peer(peer)
  local peer_id, _, peer_err = normalize_peer_id(peer)
  if not peer_id then
    return nil, peer_err
  end

  local existing = self._entries[peer_id]
  if not existing then
    return false
  end

  remove_from_list(self._buckets[existing.bucket], existing)
  self._entries[peer_id] = nil
  log.debug("routing table peer removed", {
    peer_id = peer_id,
    bucket = existing.bucket,
    size = self:size(),
  })
  return true
end

function RoutingTable:find_peer(peer)
  local peer_id, _, peer_err = normalize_peer_id(peer)
  if not peer_id then
    return nil, peer_err
  end

  local entry = self._entries[peer_id]
  if not entry then
    return nil
  end
  return copy_entry(entry)
end

function RoutingTable:bucket_for_peer(peer)
  local peer_id, _, peer_err = normalize_peer_id(peer)
  if not peer_id then
    return nil, peer_err
  end
  local entry = self._entries[peer_id]
  if entry then
    return entry.bucket
  end
  local _, key_bytes, key_err = normalize_peer_id(peer)
  if not key_bytes then
    return nil, key_err
  end
  local hashed, hash_err = self:_hash(key_bytes)
  if not hashed then
    return nil, hash_err
  end
  return self:_bucket_for_hash(hashed)
end

function RoutingTable:size()
  local total = 0
  for _ in pairs(self._entries) do
    total = total + 1
  end
  return total
end

function RoutingTable:npeers_for_cpl(cpl)
  if type(cpl) ~= "number" or cpl < 0 or cpl >= M.KEYSPACE_BITS then
    return nil, error_mod.new("input", "cpl must be in [0,255]")
  end
  local bucket = self._buckets[cpl + 1]
  return #bucket
end

function RoutingTable:all_peers()
  local out = {}
  for _, entry in pairs(self._entries) do
    out[#out + 1] = copy_entry(entry)
  end
  table.sort(out, function(a, b)
    return a.peer_id < b.peer_id
  end)
  return out
end

function RoutingTable:nearest_peers(target_key, count)
  local want = tonumber(count) or self.bucket_size
  if want <= 0 then
    return {}
  end

  local target_bytes, target_err = normalize_key_bytes(target_key)
  if not target_bytes then
    return nil, target_err
  end

  local target_hash, hash_err = self:_hash(target_bytes)
  if not target_hash then
    return nil, hash_err
  end

  local ranked = {}
  for _, entry in pairs(self._entries) do
    ranked[#ranked + 1] = {
      entry = entry,
      distance = xor_distance(target_hash, entry.hash),
    }
  end

  table.sort(ranked, function(a, b)
    local cmp = compare_distance(a.distance, b.distance)
    if cmp == 0 then
      return a.entry.peer_id < b.entry.peer_id
    end
    return cmp < 0
  end)

  local out = {}
  for i = 1, math.min(want, #ranked) do
    out[i] = copy_entry(ranked[i].entry)
  end
  return out
end

--- Construct a routing table.
-- `opts.local_peer_id` is required.
-- Optional: `opts.bucket_size` and `opts.hash_function(value)->32-byte digest`.
--- opts table
--- table|nil table
--- table|nil err
function M.new(opts)
  local options = opts or {}

  local local_peer_id, local_key_bytes, peer_err = normalize_peer_id(options.local_peer_id)
  if not local_peer_id then
    return nil, peer_err
  end

  local hash_fn = options.hash_function or default_hash
  if type(hash_fn) ~= "function" then
    return nil, error_mod.new("input", "hash_function must be a function")
  end

  local self_obj = setmetatable({
    local_peer_id = local_peer_id,
    bucket_size = options.bucket_size or M.DEFAULT_BUCKET_SIZE,
    _hash_function = hash_fn,
    _entries = {},
    _buckets = {},
  }, RoutingTable)

  if type(self_obj.bucket_size) ~= "number" or self_obj.bucket_size <= 0 then
    return nil, error_mod.new("input", "bucket_size must be > 0")
  end

  local local_hash, hash_err = self_obj:_hash(local_key_bytes)
  if not local_hash then
    return nil, hash_err
  end
  self_obj._local_hash = local_hash

  for i = 1, M.KEYSPACE_BITS do
    self_obj._buckets[i] = {}
  end

  return self_obj
end

M.RoutingTable = RoutingTable

return M
