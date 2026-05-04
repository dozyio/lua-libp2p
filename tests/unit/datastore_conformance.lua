local datastore = require("lua_libp2p.datastore")

local M = {}

function M.run_store(store, label)
  local checked, check_err = datastore.assert_store(store)
  if checked ~= store then
    return nil, check_err or label .. " store should satisfy datastore interface"
  end

  local key, key_err = datastore.key("peerstore", "peer-a", "addrs")
  if key ~= "peerstore/peer-a/addrs" then
    return nil, key_err or label .. " datastore key helper should join path segments"
  end

  local value = {
    name = "peer-a",
    count = 2,
    active = true,
    addrs = { "/ip4/127.0.0.1/tcp/4001", "/ip4/127.0.0.1/tcp/4002" },
  }
  local put_ok, put_err = store:put(key, value)
  if not put_ok then return nil, put_err end

  local got, get_err = store:get(key)
  if not got then return nil, get_err or label .. " datastore should return stored value" end
  if got.name ~= value.name or got.count ~= value.count or got.active ~= true or got.addrs[2] ~= value.addrs[2] then
    return nil, label .. " datastore should preserve table values"
  end

  assert(store:put("peerstore/peer-a/protocols", { "/ipfs/id/1.0.0" }))
  assert(store:put("peerstore/peer-b/addrs", {}))
  local keys, list_err = store:list("peerstore/peer-a")
  if not keys then return nil, list_err end
  if #keys ~= 2 or keys[1] ~= "peerstore/peer-a/addrs" or keys[2] ~= "peerstore/peer-a/protocols" then
    return nil, label .. " datastore list should return sorted keys for prefix"
  end

  local deleted, delete_err = store:delete(key)
  if not deleted then return nil, delete_err or label .. " delete should report existing key removal" end
  if store:get(key) ~= nil then return nil, label .. " deleted key should not be readable" end
  if store:delete(key) ~= false then return nil, label .. " delete should return false for missing keys" end

  local bad_put, bad_put_err = store:put("bad/ttl", true, { ttl = -1 })
  if bad_put ~= nil or not bad_put_err or bad_put_err.kind ~= "input" then
    return nil, label .. " invalid ttl should return input error"
  end

  local ttl_ok, ttl_err = store:put("ttl/key", "expires", { ttl = 1 })
  if not ttl_ok then return nil, ttl_err end
  os.execute("sleep 2")
  if store:get("ttl/key") ~= nil then
    return nil, label .. " datastore should expire ttl values"
  end

  return true
end

return M
