local bootstrap = require("lua_libp2p.peer_discovery_bootstrap")
local discovery = require("lua_libp2p.discovery")

local function run()
  local source, source_err = bootstrap.new({
    list = {
      "/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
      "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
    },
    dnsaddr_resolver = function(domain)
      if domain ~= "bootstrap.libp2p.io" then
        return nil, "unexpected domain"
      end
      return {
        "/ip4/1.2.3.4/tcp/4001/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
      }
    end,
  })
  if not source then
    return nil, source_err
  end

  local peers, peers_err = source:discover()
  if not peers then
    return nil, peers_err
  end
  if #peers ~= 2 then
    return nil, "expected bootstrap source to discover two peers"
  end

  local filtered, filtered_err = source:discover({
    dialable_only = true,
    addr_filter = function(addr)
      return addr:find("/ip4/", 1, true) ~= nil
    end,
  })
  if not filtered then
    return nil, filtered_err
  end
  if #filtered ~= 2 then
    return nil, "expected addr_filter to keep only ipv4-compatible bootstrap addresses"
  end

  local manager, manager_err = discovery.new({
    sources = {
      source,
      function()
        return {
          peers[1],
          {
            peer_id = "QmAnotherPeerId111111111111111111111111111111111111",
            addrs = { "/ip4/127.0.0.1/tcp/4001/p2p/QmAnotherPeerId111111111111111111111111111111111111" },
          },
        }
      end,
    },
  })
  if not manager then
    return nil, manager_err
  end

  local merged, merged_err = manager:discover()
  if not merged then
    return nil, merged_err
  end
  if #merged ~= 3 then
    return nil, "expected discovery manager to merge and dedupe candidates"
  end

  return true
end

return {
  name = "discovery sources and aggregation",
  run = run,
}
