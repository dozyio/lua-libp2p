--- KAD-DHT provider record storage.
-- @module lua_libp2p.kad_dht.providers
local datastore = require("lua_libp2p.datastore")
local memory_datastore = require("lua_libp2p.datastore.memory")
local error_mod = require("lua_libp2p.error")

local M = {}

-- IPFS KAD-DHT provider records have a 48h validity window. Providers should
-- republish before this expires; see reprovider defaults for the 22h interval.
M.DEFAULT_TTL_SECONDS = 48 * 60 * 60

local Store = {}
Store.__index = Store

local function hex_encode(value)
  return (value:gsub(".", function(c)
    return string.format("%02x", c:byte())
  end))
end

local function provider_key(key)
  return "kad_dht/providers/" .. hex_encode(key)
end

local function provider_peer_key(key, peer_id)
  return provider_key(key) .. "/" .. hex_encode(peer_id)
end

local function now_seconds(self)
  if type(self._now) == "function" then
    return self._now()
  end
  return os.time()
end

local function validate_key(key)
  if type(key) ~= "string" or key == "" then
    return nil, error_mod.new("input", "provider key must be non-empty bytes")
  end
  return true
end

local function validate_peer_info(peer_info)
  if type(peer_info) ~= "table" then
    return nil, error_mod.new("input", "provider peer info must be a table")
  end
  local peer_id = peer_info.peer_id or peer_info.id
  if type(peer_id) ~= "string" or peer_id == "" then
    return nil, error_mod.new("input", "provider peer id must be non-empty")
  end
  local addrs = {}
  if peer_info.addrs ~= nil then
    if type(peer_info.addrs) ~= "table" then
      return nil, error_mod.new("input", "provider addrs must be a list")
    end
    for _, addr in ipairs(peer_info.addrs) do
      if type(addr) ~= "string" then
        return nil, error_mod.new("input", "provider addr must be a string")
      end
      addrs[#addrs + 1] = addr
    end
  end
  return {
    peer_id = peer_id,
    addrs = addrs,
  }
end

local function expires_at_for(now, opts, default_ttl)
  local ttl = opts and opts.ttl_seconds
  if ttl == nil then
    ttl = opts and opts.ttl
  end
  if ttl == nil then
    ttl = default_ttl
  end
  if ttl == false or ttl == math.huge then
    return nil
  end
  if type(ttl) ~= "number" or ttl <= 0 then
    return nil, error_mod.new("input", "provider ttl must be a positive number, false, or math.huge")
  end
  return now + ttl
end

local function expired(entry, now)
  return type(entry.expires_at) == "number" and entry.expires_at <= now
end

local function validate_persisted_entry(entry, expected_key)
  if type(entry) ~= "table" then
    return nil
  end
  if expected_key ~= nil and entry.key ~= expected_key then
    return nil
  end
  if type(entry.key) ~= "string" or entry.key == "" then
    return nil
  end
  if type(entry.peer_id) ~= "string" or entry.peer_id == "" then
    return nil
  end
  if type(entry.addrs) ~= "table" then
    return nil
  end
  local addrs = {}
  for _, addr in ipairs(entry.addrs) do
    if type(addr) ~= "string" then
      return nil
    end
    addrs[#addrs + 1] = addr
  end
  if entry.first_seen_at ~= nil and type(entry.first_seen_at) ~= "number" then
    return nil
  end
  if entry.updated_at ~= nil and type(entry.updated_at) ~= "number" then
    return nil
  end
  if entry.expires_at ~= nil and type(entry.expires_at) ~= "number" then
    return nil
  end
  return {
    key = entry.key,
    peer_id = entry.peer_id,
    addrs = addrs,
    first_seen_at = entry.first_seen_at,
    updated_at = entry.updated_at,
    expires_at = entry.expires_at,
  }
end

function Store:add(key, peer_info, opts)
  local ok, key_err = validate_key(key)
  if not ok then
    return nil, key_err
  end
  local normalized, peer_err = validate_peer_info(peer_info)
  if not normalized then
    return nil, peer_err
  end

  local now = now_seconds(self)
  local expires_at, ttl_err = expires_at_for(now, opts, self._default_ttl_seconds)
  if ttl_err then
    return nil, ttl_err
  end

  local store_key = provider_peer_key(key, normalized.peer_id)
  local existing = self._datastore:get(store_key)
  local put_ok, put_err = self._datastore:put(store_key, {
    key = key,
    peer_id = normalized.peer_id,
    addrs = normalized.addrs,
    first_seen_at = existing and existing.first_seen_at or now,
    updated_at = now,
    expires_at = expires_at,
  })
  if not put_ok then
    return nil, put_err
  end
  return true
end

function Store:get(key, opts)
  local ok, key_err = validate_key(key)
  if not ok then
    return nil, key_err
  end
  local options = opts or {}
  local limit = options.limit
  if limit ~= nil and (type(limit) ~= "number" or limit < 1) then
    return nil, error_mod.new("input", "provider limit must be a positive number")
  end

  local now = now_seconds(self)
  local keys, list_err = self._datastore:list(provider_key(key) .. "/")
  if not keys then
    return nil, list_err
  end
  if #keys == 0 then
    return {}
  end

  local out = {}
  for _, store_key in ipairs(keys) do
    local entry, get_err = self._datastore:get(store_key)
    if get_err then
      return nil, get_err
    end
    if not entry then
      goto continue
    end
    local normalized = validate_persisted_entry(entry, key)
    if not normalized then
      self._datastore:delete(store_key)
    elseif expired(normalized, now) then
      self._datastore:delete(store_key)
    else
      out[#out + 1] = normalized
      if limit and #out >= limit then
        break
      end
    end
    ::continue::
  end
  table.sort(out, function(a, b)
    return a.peer_id < b.peer_id
  end)
  return out
end

function Store:list_keys(opts)
  local options = opts or {}
  local limit = options.limit
  if limit ~= nil and (type(limit) ~= "number" or limit < 1) then
    return nil, error_mod.new("input", "provider key limit must be a positive number")
  end

  local now = now_seconds(self)
  local keys, list_err = self._datastore:list("kad_dht/providers/")
  if not keys then
    return nil, list_err
  end
  local seen = {}
  local out = {}
  for _, store_key in ipairs(keys) do
    local entry, get_err = self._datastore:get(store_key)
    if get_err then
      return nil, get_err
    end
    if not entry then
      goto continue
    end
    local normalized = validate_persisted_entry(entry)
    if not normalized then
      self._datastore:delete(store_key)
      goto continue
    end
    if expired(normalized, now) then
      self._datastore:delete(store_key)
      goto continue
    end
    if options.peer_id ~= nil and normalized.peer_id ~= options.peer_id then
      goto continue
    end
    if not seen[normalized.key] then
      seen[normalized.key] = true
      out[#out + 1] = normalized.key
      if limit and #out >= limit then
        break
      end
    end
    ::continue::
  end
  table.sort(out)
  return out
end

function Store:cleanup()
  local now = now_seconds(self)
  local removed = 0
  local keys, list_err = self._datastore:list("kad_dht/providers/")
  if not keys then
    return nil, list_err
  end
  for _, store_key in ipairs(keys) do
    local entry, get_err = self._datastore:get(store_key)
    if get_err then
      return nil, get_err
    end
    local normalized = validate_persisted_entry(entry)
    if entry and not normalized then
      self._datastore:delete(store_key)
      removed = removed + 1
    elseif normalized and expired(normalized, now) then
      self._datastore:delete(store_key)
      removed = removed + 1
    end
  end
  return removed
end

function M.new(opts)
  local options = opts or {}
  local store, store_err = datastore.assert_store(options.datastore or memory_datastore.new())
  if not store then
    return nil, store_err
  end
  return setmetatable({
    _datastore = store,
    _default_ttl_seconds = options.default_ttl_seconds or M.DEFAULT_TTL_SECONDS,
    _now = options.now,
  }, Store)
end

return M
