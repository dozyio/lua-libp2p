--- KAD-DHT value record storage.
-- @module lua_libp2p.kad_dht.records
local datastore = require("lua_libp2p.datastore")
local memory_datastore = require("lua_libp2p.datastore.memory")
local error_mod = require("lua_libp2p.error")

local M = {}

M.DEFAULT_TTL_SECONDS = false

local Store = {}
Store.__index = Store

local function hex_encode(value)
  return (value:gsub(".", function(c)
    return string.format("%02x", c:byte())
  end))
end

local function record_key(key)
  return "kad_dht/records/" .. hex_encode(key)
end

local function now_seconds(self)
  if type(self._now) == "function" then
    return self._now()
  end
  return os.time()
end

local function validate_key(key)
  if type(key) ~= "string" or key == "" then
    return nil, error_mod.new("input", "record key must be non-empty bytes")
  end
  return true
end

local function validate_record(record, expected_key)
  if type(record) ~= "table" then
    return nil, error_mod.new("input", "record must be a table")
  end
  local key = record.key or expected_key
  if type(key) ~= "string" or key == "" then
    return nil, error_mod.new("input", "record key must be non-empty bytes")
  end
  if expected_key ~= nil and key ~= expected_key then
    return nil, error_mod.new("input", "record key does not match request key")
  end
  if type(record.value) ~= "string" then
    return nil, error_mod.new("input", "record value must be bytes")
  end
  return {
    key = key,
    value = record.value,
    time_received = record.time_received or record.timeReceived,
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
    return nil, error_mod.new("input", "record ttl must be a positive number, false, or math.huge")
  end
  return now + ttl
end

local function expired(entry, now)
  return type(entry.expires_at) == "number" and entry.expires_at <= now
end

local function validate_persisted_entry(entry, expected_key)
  local normalized = validate_record(entry, expected_key)
  if not normalized then
    return nil
  end
  if entry.updated_at ~= nil and type(entry.updated_at) ~= "number" then
    return nil
  end
  if entry.expires_at ~= nil and type(entry.expires_at) ~= "number" then
    return nil
  end
  if normalized.time_received ~= nil and type(normalized.time_received) ~= "string" then
    return nil
  end
  return {
    key = normalized.key,
    value = normalized.value,
    time_received = normalized.time_received,
    updated_at = entry.updated_at,
    expires_at = entry.expires_at,
  }
end

function Store:put(key, record, opts)
  local ok, key_err = validate_key(key)
  if not ok then
    return nil, key_err
  end
  local normalized, record_err = validate_record(record, key)
  if not normalized then
    return nil, record_err
  end
  local now = now_seconds(self)
  local expires_at, ttl_err = expires_at_for(now, opts, self._default_ttl_seconds)
  if ttl_err then
    return nil, ttl_err
  end
  normalized.time_received = normalized.time_received or os.date("!%Y-%m-%dT%H:%M:%SZ", now)
  local put_ok, put_err = self._datastore:put(record_key(key), {
    key = normalized.key,
    value = normalized.value,
    time_received = normalized.time_received,
    updated_at = now,
    expires_at = expires_at,
  })
  if not put_ok then
    return nil, put_err
  end
  return true
end

function Store:get(key)
  local ok, key_err = validate_key(key)
  if not ok then
    return nil, key_err
  end
  local entry, get_err = self._datastore:get(record_key(key))
  if get_err then
    return nil, get_err
  end
  if not entry then
    return nil
  end
  local normalized = validate_persisted_entry(entry, key)
  if not normalized then
    self._datastore:delete(record_key(key))
    return nil
  end
  if expired(normalized, now_seconds(self)) then
    self._datastore:delete(record_key(key))
    return nil
  end
  return {
    key = normalized.key,
    value = normalized.value,
    time_received = normalized.time_received,
    timeReceived = normalized.time_received,
    updated_at = normalized.updated_at,
    expires_at = normalized.expires_at,
  }
end

function Store:cleanup()
  local now = now_seconds(self)
  local keys, list_err = self._datastore:list("kad_dht/records/")
  if not keys then
    return nil, list_err
  end
  local removed = 0
  for _, key in ipairs(keys) do
    local entry, get_err = self._datastore:get(key)
    if get_err then
      return nil, get_err
    end
    local normalized = validate_persisted_entry(entry)
    if entry and not normalized then
      self._datastore:delete(key)
      removed = removed + 1
    elseif normalized and expired(normalized, now) then
      self._datastore:delete(key)
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
