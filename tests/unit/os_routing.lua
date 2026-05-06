local os_routing = require("lua_libp2p.os_routing")
local macos = require("lua_libp2p.os_routing.macos")

local function run()
  local route = assert(macos.parse_route_get_default([[   route to: default
destination: default
       mask: default
    gateway: fe80::56b7:bdff:fe9e:91
  interface: en0
      flags: <UP,GATEWAY,DONE,STATIC,PRCLONING,GLOBAL>
]]))
  if route.gateway ~= "fe80::56b7:bdff:fe9e:91%en0" or route.ifname ~= "en0" then
    return nil, "macos route parser should scope link-local default gateway"
  end

  local neighbors = macos.parse_ndp_neighbors([[Neighbor                                Linklayer Address  Netif Expire    St Flgs Prbs
2a00:23c7:ad38:6601:56b7:bdff:fe9e:91   54:b7:bd:9e:0:91     en0 21h57m0s  S  R
fe80::56b7:bdff:fe9e:91%en0             54:b7:bd:9e:0:91     en0 1s        R  R
]])
  if #neighbors ~= 2 then
    return nil, "macos ndp parser should parse neighbor rows"
  end
  if not neighbors[1].is_router or neighbors[1].ifname ~= "en0" then
    return nil, "macos ndp parser should surface router flag and interface"
  end

  local snapshot = assert(os_routing.snapshot({
    platform = "macos",
    command_runner = function(command)
      if command:find("route %-n get %-inet6 default", 1, false) then
        return "\n    gateway: fe80::1\n  interface: en0\n"
      elseif command:find("route %-n get default", 1, false) then
        return "\n    gateway: 192.168.1.1\n  interface: en0\n"
      elseif command:find("ndp %-an", 1, false) then
        return "Neighbor                                Linklayer Address  Netif Expire    St Flgs Prbs\n"
      end
      return ""
    end,
  }))
  if snapshot.default_route_v4.gateway ~= "192.168.1.1" then
    return nil, "os_routing snapshot should include ipv4 default route"
  end
  if snapshot.default_route_v6.gateway ~= "fe80::1%en0" then
    return nil, "os_routing snapshot should include scoped ipv6 default route"
  end

  local fallback_snapshot = assert(os_routing.snapshot({
    platform = "macos",
    command_runner = function(command)
      if command:find("route %-n get %-inet6 default", 1, false) then
        return ""
      elseif command:find("route %-n get default", 1, false) then
        return "\n    gateway: 192.168.1.1\n  interface: en0\n"
      elseif command:find("ndp %-an", 1, false) then
        return "Neighbor                                Linklayer Address  Netif Expire    St Flgs Prbs\n"
          .. "2a00:23c7:ad38:6601:56b7:bdff:fe9e:91   54:b7:bd:9e:0:91     en0 21h57m0s  S  R\n"
          .. "fe80::56b7:bdff:fe9e:91%en0             54:b7:bd:9e:0:91     en0 1s        R  R\n"
      end
      return ""
    end,
  }))
  if fallback_snapshot.default_route_v6.gateway ~= "fe80::56b7:bdff:fe9e:91%en0" then
    return nil, "os_routing should fall back to ndp router when ipv6 default route is missing"
  end
  local candidates = fallback_snapshot.router_candidates_v6 or {}
  local saw_global = false
  for _, candidate in ipairs(candidates) do
    if candidate.gateway == "2a00:23c7:ad38:6601:56b7:bdff:fe9e:91" then
      saw_global = true
      break
    end
  end
  if not saw_global then
    return nil, "os_routing should include ndp global router candidate with same iid"
  end

  return true
end

return {
  name = "os routing facade and macos parsers",
  run = run,
}
