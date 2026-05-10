local datastore = require("lua_libp2p.datastore")
local conformance = require("tests.unit.datastore_conformance")
local memory = require("lua_libp2p.datastore.memory")

local function run()
  local store = memory.new()
  local ok, err = conformance.run_store(store, "memory")
  if not ok then
    return nil, err
  end
  local closed, close_err = store:close()
  if not closed then
    return nil, close_err
  end

  local bad_store, bad_store_err = datastore.assert_store({ get = function() end })
  if bad_store ~= nil or not bad_store_err or bad_store_err.kind ~= "input" then
    return nil, "incomplete datastore should fail validation"
  end

  return true
end

return {
  name = "datastore memory kv interface",
  run = run,
}
