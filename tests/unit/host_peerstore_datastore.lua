local ed25519 = require("lua_libp2p.crypto.ed25519")
local host_mod = require("lua_libp2p.host")
local memory_datastore = require("lua_libp2p.datastore.memory")
local peerstore = require("lua_libp2p.peerstore")
local peerstore_codec = require("lua_libp2p.peerstore.codec")

local function run()
  local ds = memory_datastore.new()
  local ps, ps_err = peerstore.new({ datastore = ds })
  if not ps then
    return nil, ps_err
  end

  local h, host_err = host_mod.new({
    identity = assert(ed25519.generate_keypair()),
    runtime = "luv",
    listen_addrs = {},
    peerstore = ps,
  })
  if not h then
    return nil, host_err
  end

  local before = h.peerstore:all()
  if #before ~= 0 then
    return nil, "new datastore peerstore should start empty"
  end

  local merged, merge_err = h.peerstore:merge("peer-a", {
    addrs = { "/ip4/127.0.0.1/tcp/4001" },
    protocols = { "/tests/proto/1.0.0" },
  })
  if not merged then
    return nil, merge_err
  end

  local observed = false
  local sub, sub_err = h:on_protocol("/tests/proto/1.0.0", function(peer_id, payload)
    if peer_id == "peer-a" and payload.source == "peerstore" then
      observed = true
    end
    return true
  end)
  if not sub then
    return nil, sub_err
  end
  if not observed then
    return nil, "host protocol replay should work with datastore-backed peerstore"
  end

  local stored = ds:get("peerstore/peers/peer-a")
  if type(stored) ~= "string" then
    return nil, "host should persist encoded peerstore records into supplied datastore"
  end
  stored = assert(peerstore_codec.decode(stored))
  if not stored.protocols["/tests/proto/1.0.0"] then
    return nil, "host should persist peerstore mutations into supplied datastore"
  end

  local reloaded = assert(peerstore.new({ datastore = ds }))
  local addrs = reloaded:get_addrs("peer-a")
  if #addrs ~= 1 or addrs[1] ~= "/ip4/127.0.0.1/tcp/4001" then
    return nil, "host datastore peerstore records should be reusable by a new peerstore"
  end

  return true
end

return {
  name = "host datastore-backed peerstore integration",
  run = run,
}
