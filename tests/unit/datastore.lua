local datastore = require("lua_libp2p.datastore")
local memory = require("lua_libp2p.datastore.memory")

local function run()
  local store = memory.new()
  local checked, check_err = datastore.assert_store(store)
  if checked ~= store then
    return nil, check_err or "expected memory store to satisfy datastore interface"
  end

  local key, key_err = datastore.key("peerstore", "peer-a", "addrs")
  if key ~= "peerstore/peer-a/addrs" then
    return nil, key_err or "expected datastore key helper to join path segments"
  end

  local put_ok, put_err = store:put(key, { "/ip4/127.0.0.1/tcp/4001" })
  if not put_ok then
    return nil, put_err
  end
  local value, get_err = store:get(key)
  if not value then
    return nil, get_err or "expected value from datastore get"
  end
  if value[1] ~= "/ip4/127.0.0.1/tcp/4001" then
    return nil, "datastore should preserve stored Lua values"
  end

  assert(store:put("peerstore/peer-a/protocols", { "/ipfs/id/1.0.0" }))
  assert(store:put("peerstore/peer-b/addrs", {}))
  local keys, list_err = store:list("peerstore/peer-a")
  if not keys then
    return nil, list_err
  end
  if #keys ~= 2 or keys[1] ~= "peerstore/peer-a/addrs" or keys[2] ~= "peerstore/peer-a/protocols" then
    return nil, "datastore list should return sorted keys for prefix"
  end

  local deleted, delete_err = store:delete(key)
  if not deleted then
    return nil, delete_err or "delete should report existing key removal"
  end
  if store:get(key) ~= nil then
    return nil, "deleted key should not be readable"
  end
  if store:delete(key) ~= false then
    return nil, "delete should return false for missing keys"
  end

  local bad_put, bad_put_err = store:put("bad/ttl", true, { ttl = -1 })
  if bad_put ~= nil or not bad_put_err or bad_put_err.kind ~= "input" then
    return nil, "invalid ttl should return input error"
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
