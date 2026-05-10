local conformance = require("tests.unit.datastore_conformance")
local sqlite = require("lua_libp2p.datastore.sqlite")

local function close_remove(store, path)
  if store then
    store:close()
  end
  os.remove(path)
end

local function exec_raw(store, sql)
  local result, err = store._conn:execute(sql)
  if result == nil then
    return nil, err
  end
  if type(result) == "userdata" and type(result.close) == "function" then
    result:close()
  end
  return true
end

local function run()
  local has_luasql = pcall(require, "luasql.sqlite3")
  if not has_luasql then
    return true
  end

  local path = os.tmpname()
  os.remove(path)

  local store, store_err = sqlite.new({ path = path })
  if not store then
    return nil, store_err
  end
  local missing, missing_err = store:get("missing")
  if missing ~= nil or missing_err ~= nil then
    close_remove(store, path)
    return nil, "sqlite missing key should return nil without decode error"
  end
  local empty_keys, empty_list_err = store:list("missing/prefix")
  if not empty_keys or empty_list_err ~= nil or #empty_keys ~= 0 then
    close_remove(store, path)
    return nil, "sqlite empty list should return an empty table without decode error"
  end

  local raw_ok, raw_err = exec_raw(
    store,
    "REPLACE INTO " .. store._table .. " (key, value, expires_at, updated_at) VALUES ('corrupt/delete', X'ff', NULL, 1)"
  )
  if not raw_ok then
    close_remove(store, path)
    return nil, raw_err
  end
  local deleted_corrupt, delete_corrupt_err = store:delete("corrupt/delete")
  if deleted_corrupt ~= true or delete_corrupt_err ~= nil then
    close_remove(store, path)
    return nil, "sqlite delete should not decode existing values"
  end

  local ttl_ok, ttl_err = store:put("expired/direct", "expired")
  if not ttl_ok then
    close_remove(store, path)
    return nil, ttl_err
  end
  raw_ok, raw_err = exec_raw(store, "UPDATE " .. store._table .. " SET expires_at = 1 WHERE key = 'expired/direct'")
  if not raw_ok then
    close_remove(store, path)
    return nil, raw_err
  end
  local expired, expired_err = store:get("expired/direct")
  if expired ~= nil or expired_err ~= nil then
    close_remove(store, path)
    return nil, "sqlite expired get should return nil without recursive delete errors"
  end
  local deleted_expired, delete_expired_err = store:delete("expired/direct")
  if deleted_expired ~= false or delete_expired_err ~= nil then
    close_remove(store, path)
    return nil, "sqlite expired get should remove the expired row"
  end

  local ok, err = conformance.run_store(store, "sqlite")
  if not ok then
    close_remove(store, path)
    return nil, err
  end

  store:close()
  local reopened, reopen_err = sqlite.new({ path = path })
  if not reopened then
    os.remove(path)
    return nil, reopen_err
  end
  local got = assert(reopened:get("peerstore/peer-b/addrs"))
  if type(got) ~= "table" then
    close_remove(reopened, path)
    return nil, "sqlite store should persist values across reopen"
  end
  close_remove(reopened, path)
  return true
end

return {
  name = "datastore sqlite kv interface",
  run = run,
}
