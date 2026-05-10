local kad_dht = require("lua_libp2p.kad_dht")
local dnsaddr = require("lua_libp2p.dnsaddr")

local function run()
  local dns_addr = "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN"
  if not dnsaddr.is_dnsaddr(dns_addr) then
    return nil, "expected dnsaddr address detection"
  end

  local resolved, resolve_err = dnsaddr.resolve(dns_addr, {
    resolver = function(domain)
      if domain ~= "bootstrap.libp2p.io" then
        return nil, "unexpected domain"
      end
      return {
        "dnsaddr=/ip4/1.2.3.4/tcp/4001/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
        "dnsaddr=/ip4/5.6.7.8/tcp/4001/p2p/QmWrongPeer111111111111111111111111111111111111",
      }
    end,
  })
  if not resolved then
    return nil, resolve_err
  end
  if #resolved ~= 1 then
    return nil, "expected resolver peer-id filtering"
  end

  local recursive_addr = "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN"
  local recursive, recursive_err = dnsaddr.resolve(recursive_addr, {
    resolver = function(domain)
      if domain == "bootstrap.libp2p.io" then
        return {
          "dnsaddr=/dnsaddr/sv15.bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
        }
      end
      if domain == "sv15.bootstrap.libp2p.io" then
        return {
          "dnsaddr=/ip4/147.75.87.27/tcp/4001/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
        }
      end
      return {}
    end,
  })
  if not recursive then
    return nil, recursive_err
  end
  if
    #recursive ~= 1 or recursive[1] ~= "/ip4/147.75.87.27/tcp/4001/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN"
  then
    return nil, "expected recursive dnsaddr resolution to terminal ip4 address"
  end

  local _, loop_err = dnsaddr.resolve("/dnsaddr/loop.test/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN", {
    resolver = function()
      return {
        "dnsaddr=/dnsaddr/loop.test/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
      }
    end,
    max_depth = 1,
  })
  if loop_err ~= nil then
    return nil, "recursive loop should terminate safely without error"
  end

  local out, out_err = kad_dht.resolve_bootstrap_addrs({
    "/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
    dns_addr,
  }, {
    dnsaddr_resolver = function()
      return {
        "/ip4/1.2.3.4/tcp/4001/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
      }
    end,
  })
  if not out then
    return nil, out_err
  end
  if #out ~= 2 then
    return nil, "expected mixed ip4 and resolved dnsaddr targets"
  end

  local dht, dht_err = kad_dht.new(nil, {
    local_peer_id = "local",
    hash_function = function(value)
      if value == "local" then
        return string.char(0) .. string.rep("\0", 31)
      end
      return string.char(255) .. string.rep("\0", 31)
    end,
    bootstrappers = { dns_addr },
    dnsaddr_resolver = function()
      return {
        "/ip4/1.2.3.4/tcp/4001/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
      }
    end,
  })
  if not dht then
    return nil, dht_err
  end

  local targets, targets_err = dht:bootstrap_targets()
  if not targets then
    return nil, targets_err
  end
  if #targets ~= 1 then
    return nil, "expected resolved bootstrap target from DHT helper"
  end

  local discovered, discovered_err = dht:discover_peers({
    dnsaddr_resolver = dht._dnsaddr_resolver,
  })
  if not discovered then
    return nil, discovered_err
  end
  if #discovered ~= 1 then
    return nil, "expected one discovered peer candidate"
  end

  return true
end

return {
  name = "kad-dht dnsaddr resolver abstraction",
  run = run,
}
