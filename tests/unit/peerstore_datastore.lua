local memory_datastore = require("lua_libp2p.datastore.memory")
local peerstore = require("lua_libp2p.peerstore")
local peerstore_codec = require("lua_libp2p.peerstore.codec")

local function run()
  local ds = memory_datastore.new()
  local ps, ps_err = peerstore.new({
    datastore = ds,
    default_addr_ttl = 60,
  })
  if not ps then
    return nil, ps_err
  end

  local peer, merge_err = ps:merge("peer-a", {
    addrs = {
      "/ip4/127.0.0.1/tcp/4001",
      "/ip4/127.0.0.1/tcp/4001",
    },
    protocols = { "/ipfs/id/1.0.0" },
    metadata = { agent = "test-agent" },
    public_key = "public-key-bytes",
  })
  if not peer then
    return nil, merge_err
  end
  if #peer.addrs ~= 1 or peer.metadata.agent ~= "test-agent" then
    return nil, "datastore peerstore merge should dedupe addrs and store metadata"
  end

  local record = ds:get("peerstore/peers/peer-a")
  if type(record) ~= "string" then
    return nil, "datastore peerstore should write encoded peer record bytes"
  end
  record = assert(peerstore_codec.decode(record))
  if record.peer_id ~= "peer-a" or type(record.addrs) ~= "table" then
    return nil, "encoded datastore peerstore record should decode to one peer record"
  end

  local merged, second_merge_err = ps:merge("peer-a", {
    addrs = { "/ip4/127.0.0.1/tcp/4002" },
    protocols = { "/ipfs/ping/1.0.0" },
    metadata = { latency = 12 },
  })
  if not merged then
    return nil, second_merge_err
  end
  local addrs = ps:get_addrs("peer-a")
  if #addrs ~= 2 then
    return nil, "merge should add addresses without replacing existing addresses"
  end
  if not ps:supports_protocol("peer-a", "/ipfs/id/1.0.0")
    or not ps:supports_protocol("peer-a", "/ipfs/ping/1.0.0")
  then
    return nil, "merge should add protocols without replacing existing protocols"
  end
  local merged_view = ps:get("peer-a")
  if merged_view.metadata.agent ~= "test-agent" or merged_view.metadata.latency ~= 12 then
    return nil, "merge should overlay metadata without clearing existing metadata"
  end

  local tagged, tag_err = ps:tag("peer-a", "bootstrap", { value = 50, ttl = math.huge })
  if not tagged then
    return nil, tag_err
  end
  local tags = ps:get_tags("peer-a")
  if not tags.bootstrap or tags.bootstrap.value ~= 50 or tags.bootstrap.expires_at ~= nil then
    return nil, "datastore peerstore should preserve non-expiring tags"
  end

  record = assert(peerstore_codec.decode(assert(ds:get("peerstore/peers/peer-a"))))
  record.addrs["/ip4/127.0.0.1/tcp/4001"].expires_at = os.time() - 1
  record.tags.bootstrap.expires_at = os.time() - 1
  assert(ds:put("peerstore/peers/peer-a", assert(peerstore_codec.encode(record))))
  local live_addrs = ps:get_addrs("peer-a")
  if #live_addrs ~= 1 or live_addrs[1] ~= "/ip4/127.0.0.1/tcp/4002" then
    return nil, "expired datastore peerstore addresses should be pruned on read"
  end
  if ps:get_tags("peer-a").bootstrap ~= nil then
    return nil, "expired datastore peerstore tags should be pruned on read"
  end
  record = assert(peerstore_codec.decode(assert(ds:get("peerstore/peers/peer-a"))))
  if record.addrs["/ip4/127.0.0.1/tcp/4001"] ~= nil or record.tags.bootstrap ~= nil then
    return nil, "expiry cleanup should be persisted back to datastore"
  end

  local reloaded, reload_err = peerstore.new({ datastore = ds })
  if not reloaded then
    return nil, reload_err
  end
  local reloaded_peer = reloaded:get("peer-a")
  if not reloaded_peer or #reloaded_peer.addrs ~= 1 or not reloaded:supports_protocol("peer-a", "/ipfs/ping/1.0.0") then
    return nil, "new datastore peerstore instance should read existing peer records"
  end

  local patched, patch_err = reloaded:patch("peer-a", {
    addrs = { "/ip4/127.0.0.1/tcp/5001" },
    protocols = {},
    metadata = { replaced = true },
  })
  if not patched then
    return nil, patch_err
  end
  if #reloaded:get_addrs("peer-a") ~= 1 then
    return nil, "patch should replace addresses"
  end
  if reloaded:supports_protocol("peer-a", "/ipfs/id/1.0.0") then
    return nil, "patch should replace protocols"
  end
  local patched_view = reloaded:get("peer-a")
  if patched_view.metadata.agent ~= nil or patched_view.metadata.replaced ~= true then
    return nil, "patch should replace metadata"
  end

  assert(reloaded:merge("peer-b", { protocols = { "/x/1.0.0" } }))
  local peers = reloaded:all()
  if #peers ~= 2 or peers[1].peer_id ~= "peer-a" or peers[2].peer_id ~= "peer-b" then
    return nil, "all should list datastore peers sorted by peer id"
  end

  local deleted, delete_err = reloaded:delete("peer-a")
  if not deleted then
    return nil, delete_err or "delete should remove datastore peer record"
  end
  if reloaded:has("peer-a") or ds:get("peerstore/peers/peer-a") ~= nil then
    return nil, "deleted datastore peer should not be visible"
  end

  return true
end

return {
  name = "peerstore datastore-backed records",
  run = run,
}
