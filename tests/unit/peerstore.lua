local peerstore = require("lua_libp2p.peerstore")

local function run()
  local ps = peerstore.new({ default_addr_ttl = 60 })

  local peer, merge_err = ps:merge("peer-a", {
    addrs = {
      "/ip4/127.0.0.1/tcp/4001/p2p/peer-a",
      "/ip4/127.0.0.1/tcp/4001/p2p/peer-a",
    },
    protocols = { "/ipfs/kad/1.0.0" },
    metadata = { agent = "test" },
  })
  if not peer then
    return nil, merge_err
  end

  local addrs = ps:get_addrs("peer-a")
  if #addrs ~= 1 then
    return nil, "expected deduped peer address"
  end
  if not ps:supports_protocol("peer-a", "/ipfs/kad/1.0.0") then
    return nil, "expected stored protocol support"
  end
  local tagged, tag_err = ps:tag("peer-a", "bootstrap", { value = 50, ttl = math.huge })
  if not tagged then
    return nil, tag_err
  end
  local tags = ps:get_tags("peer-a")
  if not tags.bootstrap or tags.bootstrap.value ~= 50 or tags.bootstrap.expires_at ~= nil then
    return nil, "expected bootstrap tag metadata"
  end
  local untagged = ps:untag("peer-a", "bootstrap")
  if not untagged or ps:get_tags("peer-a").bootstrap ~= nil then
    return nil, "expected bootstrap tag removal"
  end

  local patched, patch_err = ps:patch("peer-a", {
    addrs = {
      "/ip4/127.0.0.1/tcp/4002/p2p/peer-a",
      "/ip4/127.0.0.1/udp/4001/quic-v1/p2p/peer-a",
    },
    protocols = {},
  })
  if not patched then
    return nil, patch_err
  end
  addrs = ps:get_addrs("peer-a")
  if #addrs ~= 2 then
    return nil, "expected patch to replace addresses"
  end
  if ps:supports_protocol("peer-a", "/ipfs/kad/1.0.0") then
    return nil, "expected patch to replace protocols"
  end

  local deleted = ps:delete("peer-a")
  if not deleted or ps:has("peer-a") then
    return nil, "expected peer deletion"
  end

  return true
end

return {
  name = "peerstore in-memory address and protocol books",
  run = run,
}
