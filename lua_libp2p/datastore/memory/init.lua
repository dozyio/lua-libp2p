--- In-memory datastore implementation.
---@class Libp2pMemoryDatastore: Libp2pDatastore

local datastore = require("lua_libp2p.datastore")
local error_mod = require("lua_libp2p.error")

local M = {}

local Store = {}
Store.__index = Store

local function now_seconds()
  return os.time()
end

local function expired(entry)
  return type(entry.expires_at) == "number" and entry.expires_at <= now_seconds()
end

local function ttl_deadline(opts)
  local ttl = opts and opts.ttl or nil
  if ttl == nil or ttl == false or ttl == math.huge then
    return nil
  end
  if type(ttl) ~= "number" or ttl <= 0 then
    return nil, error_mod.new("input", "datastore ttl must be a positive number, false, or math.huge")
  end
  return now_seconds() + ttl
end

function Store:get(key)
  local ok, key_err = datastore.validate_key(key)
  if not ok then
    return nil, key_err
  end
  local entry = self._entries[key]
  if not entry then
    return nil
  end
  if expired(entry) then
    self._entries[key] = nil
    return nil
  end
  return entry.value
end

function Store:put(key, value, opts)
  local ok, key_err = datastore.validate_key(key)
  if not ok then
    return nil, key_err
  end
  if value == nil then
    return nil, error_mod.new("input", "datastore value cannot be nil")
  end
  local expires_at, ttl_err = ttl_deadline(opts)
  if ttl_err then
    return nil, ttl_err
  end
  self._entries[key] = {
    value = value,
    expires_at = expires_at,
  }
  return true
end

function Store:delete(key)
  local ok, key_err = datastore.validate_key(key)
  if not ok then
    return nil, key_err
  end
  local existed = self._entries[key] ~= nil
  self._entries[key] = nil
  return existed
end

function Store:list(prefix)
  local ok, key_err = datastore.validate_key(prefix)
  if not ok then
    return nil, key_err
  end
  local keys = {}
  local prefix_len = #prefix
  for key, entry in pairs(self._entries) do
    if expired(entry) then
      self._entries[key] = nil
    elseif key:sub(1, prefix_len) == prefix then
      keys[#keys + 1] = key
    end
  end
  table.sort(keys)
  return keys
end

function Store:query(query)
  local q = query or {}
  local prefix = q.prefix or ""
  if prefix == "" then
    return nil, error_mod.new("input", "query prefix must be non-empty")
  end
  local keys, list_err = self:list(prefix)
  if not keys then
    return nil, list_err
  end
  local offset = tonumber(q.offset) or 0
  local limit = q.limit ~= nil and tonumber(q.limit) or nil
  if offset < 0 or (limit ~= nil and limit < 0) then
    return nil, error_mod.new("input", "query offset and limit must be non-negative")
  end
  offset = math.floor(offset)
  limit = limit and math.floor(limit) or nil
  local index = offset + 1
  local returned = 0
  return datastore.results_from_next(function()
    if limit and returned >= limit then
      return nil
    end
    local key = keys[index]
    if not key then
      return nil
    end
    index = index + 1
    returned = returned + 1
    if q.keys_only then
      return { key = key }
    end
    return { key = key, value = self:get(key) }
  end)
end

function Store:close()
  return true
end

---@param initial? table<string, any> Initial key/value entries.
---@return Libp2pMemoryDatastore store
function M.new(initial)
  local store = setmetatable({
    _entries = {},
  }, Store)
  for key, value in pairs(initial or {}) do
    store._entries[key] = { value = value }
  end
  return store
end

return M
