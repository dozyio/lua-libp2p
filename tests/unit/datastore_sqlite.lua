local conformance = require("tests.unit.datastore_conformance")
local sqlite = require("lua_libp2p.datastore.sqlite")

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
  local ok, err = conformance.run_store(store, "sqlite")
  if not ok then
    store:close()
    os.remove(path)
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
    reopened:close()
    os.remove(path)
    return nil, "sqlite store should persist values across reopen"
  end
  reopened:close()
  os.remove(path)
  return true
end

return {
  name = "datastore sqlite kv interface",
  run = run,
}
