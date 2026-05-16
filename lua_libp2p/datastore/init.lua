--- Minimal synchronous datastore/KV interface helpers.
-- Datastores expose `get`, `put`, `delete`, `list`, and `query` methods.
-- Backends used with the current peerstore must be local/fast enough to call
-- synchronously from host/service code; remote or blocking IO backends need a
-- future async adapter instead of implementing this interface directly.
---@class Libp2pDatastorePutOptions
---@field ttl? number|false Time-to-live seconds, `false`, or `math.huge`.

---@class Libp2pDatastore
---@field get fun(self: Libp2pDatastore, key: string): any, table|nil
---@field put fun(self: Libp2pDatastore, key: string, value: any, opts?: Libp2pDatastorePutOptions): true|nil, table|nil
---@field delete fun(self: Libp2pDatastore, key: string): boolean|nil, table|nil
---@field list fun(self: Libp2pDatastore, prefix: string): string[]|nil, table|nil
---@field query fun(self: Libp2pDatastore, query: Libp2pDatastoreQuery): Libp2pDatastoreResults|nil, table|nil
---@field close? fun(self: Libp2pDatastore): true|nil, table|nil

---@class Libp2pDatastoreQuery
---@field prefix? string
---@field keys_only? boolean
---@field limit? integer
---@field offset? integer

---@class Libp2pDatastoreEntry
---@field key string
---@field value? any

---@class Libp2pDatastoreResults
---@field next fun(self: Libp2pDatastoreResults): Libp2pDatastoreEntry|nil, table|nil
---@field close fun(self: Libp2pDatastoreResults): true|nil, table|nil
---@field rest fun(self: Libp2pDatastoreResults): Libp2pDatastoreEntry[]|nil, table|nil

local error_mod = require("lua_libp2p.error")

local M = {}

local REQUIRED_METHODS = { "get", "put", "delete", "list", "query" }

local function validate_key(key)
  if type(key) ~= "string" or key == "" then
    return nil, error_mod.new("input", "datastore key must be a non-empty string")
  end
  return true
end

--- Validate that a value implements the datastore interface.
---@param store any Candidate datastore object.
---@return Libp2pDatastore|nil store
---@return table|nil err
function M.assert_store(store)
  if type(store) ~= "table" then
    return nil, error_mod.new("input", "datastore must be a table")
  end
  for _, method in ipairs(REQUIRED_METHODS) do
    if type(store[method]) ~= "function" then
      return nil, error_mod.new("input", "datastore missing required method", {
        method = method,
      })
    end
  end
  return store
end

--- Validate a datastore key.
---@param key string
---@return true|nil ok
---@return table|nil err
function M.validate_key(key)
  return validate_key(key)
end

--- Join key path segments with `/`.
-- Empty/nil segments are rejected to avoid ambiguous keys.
---@vararg string
---@return string|nil key
---@return table|nil err
function M.key(...)
  local parts = { ... }
  if #parts == 0 then
    return nil, error_mod.new("input", "datastore key requires at least one segment")
  end
  for i, part in ipairs(parts) do
    if type(part) ~= "string" or part == "" then
      return nil,
        error_mod.new("input", "datastore key segment must be a non-empty string", {
          index = i,
        })
    end
  end
  return table.concat(parts, "/")
end

function M.results_from_next(next_fn, close_fn)
  local closed = false
  local results = {}
  function results:next()
    if closed then
      return nil
    end
    local entry, err = next_fn()
    if entry == nil and err == nil then
      self:close()
    end
    return entry, err
  end
  function results:close()
    if closed then
      return true
    end
    closed = true
    if close_fn then
      return close_fn()
    end
    return true
  end
  function results:rest()
    local out = {}
    while true do
      local entry, err = self:next()
      if err then
        return nil, err
      end
      if not entry then
        break
      end
      out[#out + 1] = entry
    end
    return out
  end
  return results
end

return M
