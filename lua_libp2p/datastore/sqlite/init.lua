--- SQLite datastore implementation backed by LuaSQL (`luasql.sqlite3`).
---@class Libp2pSqliteDatastoreConfig
---@field path? string Database file path.
---@field filename? string Alias for `path`.
---@field table_name? string KV table name. Default: `kv`.

---@class Libp2pSqliteDatastore: Libp2pDatastore

local datastore = require("lua_libp2p.datastore")
local error_mod = require("lua_libp2p.error")
local varint = require("lua_libp2p.multiformats.varint")

local M = {}

local Store = {}
Store.__index = Store

local function sql_ident(name)
  if type(name) ~= "string" or not name:match("^[A-Za-z_][A-Za-z0-9_]*$") then
    return nil, error_mod.new("input", "sqlite datastore table name must be a SQL identifier")
  end
  return '"' .. name .. '"'
end

local function sql_string(value)
  return "'" .. tostring(value):gsub("'", "''") .. "'"
end

local function prefix_upper_bound(prefix)
  if prefix == "" then
    return nil
  end
  for i = #prefix, 1, -1 do
    local byte = prefix:byte(i)
    if byte < 255 then
      return prefix:sub(1, i - 1) .. string.char(byte + 1)
    end
  end
  return nil
end

local function prefix_where(prefix)
  local upper = prefix_upper_bound(prefix)
  local where = "key >= " .. sql_string(prefix)
  if upper then
    where = where .. " AND key < " .. sql_string(upper)
  end
  return where
end

local function hex_encode(value)
  return (value:gsub(".", function(c)
    return string.format("%02x", c:byte())
  end))
end

local function blob_literal(value)
  return "X'" .. hex_encode(value) .. "'"
end

local function cursor_close(cursor)
  if cursor and type(cursor.close) == "function" then
    cursor:close()
  end
end

local function exec(conn, sql)
  local result, err = conn:execute(sql)
  if result == nil then
    return nil, error_mod.new("io", "sqlite datastore query failed", { cause = err, sql = sql })
  end
  return result
end

local function ttl_deadline(opts)
  local ttl = opts and opts.ttl or nil
  if ttl == nil or ttl == false or ttl == math.huge then
    return nil
  end
  if type(ttl) ~= "number" or ttl <= 0 then
    return nil, error_mod.new("input", "datastore ttl must be a positive number, false, or math.huge")
  end
  return os.time() + ttl
end

local function sorted_table_keys(tbl)
  local keys = {}
  for k in pairs(tbl) do
    keys[#keys + 1] = k
  end
  table.sort(keys, function(a, b)
    local ta, tb = type(a), type(b)
    if ta ~= tb then
      return ta < tb
    end
    return tostring(a) < tostring(b)
  end)
  return keys
end

local function encode_value(value)
  local value_type = type(value)
  if value_type == "string" then
    return "s" .. assert(varint.encode_u64(#value)) .. value
  end
  if value_type == "number" then
    return "n" .. string.pack("<d", value)
  end
  if value_type == "boolean" then
    return "b" .. (value and "\1" or "\0")
  end
  if value_type == "table" then
    local keys = sorted_table_keys(value)
    local out = { "t", assert(varint.encode_u64(#keys)) }
    for _, key in ipairs(keys) do
      out[#out + 1] = encode_value(key)
      out[#out + 1] = encode_value(value[key])
    end
    return table.concat(out)
  end
  error("unsupported sqlite datastore value type: " .. value_type)
end

local function decode_value(payload, index)
  local tag = payload:sub(index, index)
  index = index + 1
  if tag == "s" then
    local len, after_len = assert(varint.decode_u64(payload, index))
    local finish = after_len + len - 1
    return payload:sub(after_len, finish), finish + 1
  end
  if tag == "n" then
    local value, next_i = string.unpack("<d", payload, index)
    return value, next_i
  end
  if tag == "b" then
    return payload:byte(index) ~= 0, index + 1
  end
  if tag == "t" then
    local count, after_count = assert(varint.decode_u64(payload, index))
    local out = {}
    index = after_count
    for _ = 1, count do
      local key
      key, index = decode_value(payload, index)
      local value
      value, index = decode_value(payload, index)
      out[key] = value
    end
    return out, index
  end
  error("unsupported sqlite datastore value tag: " .. tostring(tag))
end

local function encode_for_store(value)
  local ok, encoded_or_err = pcall(encode_value, value)
  if not ok then
    return nil, error_mod.new("input", "sqlite datastore value cannot be encoded", { cause = encoded_or_err })
  end
  return encoded_or_err
end

local function decode_from_store(payload)
  local ok, value_or_err = pcall(function()
    local value = decode_value(payload, 1)
    return value
  end)
  if not ok then
    return nil, error_mod.new("decode", "sqlite datastore value cannot be decoded", { cause = value_or_err })
  end
  return value_or_err
end

function Store:get(key)
  local ok, key_err = datastore.validate_key(key)
  if not ok then
    return nil, key_err
  end
  local cursor, query_err = exec(
    self._conn,
    "SELECT value, expires_at FROM " .. self._table .. " WHERE key = " .. sql_string(key) .. " LIMIT 1"
  )
  if not cursor then
    return nil, query_err
  end
  local row = cursor:fetch({}, "a")
  cursor_close(cursor)
  if not row or row.value == nil then
    return nil
  end
  local expires_at = tonumber(row.expires_at)
  if expires_at and expires_at <= os.time() then
    local _, delete_err = exec(self._conn, "DELETE FROM " .. self._table .. " WHERE key = " .. sql_string(key))
    if delete_err then
      return nil, delete_err
    end
    return nil
  end
  return decode_from_store(row.value)
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
  local encoded, encode_err = encode_for_store(value)
  if not encoded then
    return nil, encode_err
  end
  local expires_sql = expires_at and tostring(expires_at) or "NULL"
  local sql = "REPLACE INTO "
    .. self._table
    .. " (key, value, expires_at, updated_at) VALUES ("
    .. sql_string(key)
    .. ", "
    .. blob_literal(encoded)
    .. ", "
    .. expires_sql
    .. ", "
    .. tostring(os.time())
    .. ")"
  local result, put_err = exec(self._conn, sql)
  if not result then
    return nil, put_err
  end
  return true
end

function Store:delete(key)
  local ok, key_err = datastore.validate_key(key)
  if not ok then
    return nil, key_err
  end
  local cursor, query_err =
    exec(self._conn, "SELECT 1 AS present FROM " .. self._table .. " WHERE key = " .. sql_string(key) .. " LIMIT 1")
  if not cursor then
    return nil, query_err
  end
  local row = cursor:fetch({}, "a")
  cursor_close(cursor)
  local existed = row ~= nil and row.present ~= nil
  local result, delete_err = exec(self._conn, "DELETE FROM " .. self._table .. " WHERE key = " .. sql_string(key))
  if not result then
    return nil, delete_err
  end
  return existed
end

function Store:list(prefix)
  local ok, key_err = datastore.validate_key(prefix)
  if not ok then
    return nil, key_err
  end
  local cursor, query_err = exec(
    self._conn,
    "SELECT key, expires_at FROM " .. self._table .. " WHERE " .. prefix_where(prefix) .. " ORDER BY key"
  )
  if not cursor then
    return nil, query_err
  end
  local keys = {}
  while true do
    local row = cursor:fetch({}, "a")
    if not row or row.key == nil then
      break
    end
    local key = row.key
    local expires_at = tonumber(row.expires_at)
    if expires_at and expires_at <= os.time() then
      self:delete(key)
    else
      keys[#keys + 1] = key
    end
  end
  cursor_close(cursor)
  return keys
end

function Store:count(prefix)
  local ok, key_err = datastore.validate_key(prefix)
  if not ok then
    return nil, key_err
  end
  local now = os.time()
  local cursor, query_err = exec(
    self._conn,
    "SELECT COUNT(*) AS count FROM "
      .. self._table
      .. " WHERE "
      .. prefix_where(prefix)
      .. " AND (expires_at IS NULL OR expires_at > "
      .. tostring(now)
      .. ")"
  )
  if not cursor then
    return nil, query_err
  end
  local row = cursor:fetch({}, "a")
  cursor_close(cursor)
  return tonumber(row and row.count) or 0
end

function Store:close()
  if self._conn then
    self._conn:close()
  end
  if self._env then
    self._env:close()
  end
  self._conn = nil
  self._env = nil
  return true
end

---@param opts Libp2pSqliteDatastoreConfig
---@return Libp2pSqliteDatastore|nil store
---@return table|nil err
function M.new(opts)
  local options = opts or {}
  local path = options.path or options.filename
  if type(path) ~= "string" or path == "" then
    return nil, error_mod.new("input", "sqlite datastore path is required")
  end
  local table_name, table_err = sql_ident(options.table_name or "kv")
  if not table_name then
    return nil, table_err
  end
  local ok, sqlite_or_err = pcall(require, "luasql.sqlite3")
  if not ok then
    return nil, error_mod.new("dependency", "luasql.sqlite3 is not available", { cause = sqlite_or_err })
  end
  local env, env_err = sqlite_or_err.sqlite3()
  if not env then
    return nil, error_mod.new("io", "failed to create sqlite environment", { cause = env_err })
  end
  local conn, conn_err = env:connect(path)
  if not conn then
    env:close()
    return nil, error_mod.new("io", "failed to open sqlite datastore", { cause = conn_err, path = path })
  end
  local create_ok, create_err = exec(
    conn,
    "CREATE TABLE IF NOT EXISTS "
      .. table_name
      .. " (key TEXT PRIMARY KEY, value BLOB NOT NULL, expires_at INTEGER NULL, updated_at INTEGER NOT NULL)"
  )
  if not create_ok then
    conn:close()
    env:close()
    return nil, create_err
  end
  return setmetatable({ _env = env, _conn = conn, _table = table_name }, Store)
end

return M
