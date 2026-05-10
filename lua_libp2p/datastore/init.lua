--- Minimal synchronous datastore/KV interface helpers.
-- Datastores expose `get`, `put`, `delete`, and `list` methods.
-- Backends used with the current peerstore must be local/fast enough to call
-- synchronously from host/service code; remote or blocking IO backends need a
-- future async adapter instead of implementing this interface directly.
-- @module lua_libp2p.datastore
local error_mod = require("lua_libp2p.error")

local M = {}

local REQUIRED_METHODS = { "get", "put", "delete", "list" }

local function validate_key(key)
  if type(key) ~= "string" or key == "" then
    return nil, error_mod.new("input", "datastore key must be a non-empty string")
  end
  return true
end

--- Validate that a value implements the datastore interface.
-- @param store Candidate datastore object.
-- @treturn table|nil store
-- @treturn[opt] table err
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
-- @tparam string key
-- @treturn true|nil ok
-- @treturn[opt] table err
function M.validate_key(key)
  return validate_key(key)
end

--- Join key path segments with `/`.
-- Empty/nil segments are rejected to avoid ambiguous keys.
-- @treturn string|nil key
-- @treturn[opt] table err
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

return M
