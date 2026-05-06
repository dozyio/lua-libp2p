package.path = table.concat({
  "./?.lua",
  "./?/init.lua",
  package.path,
}, ";")

local function usage()
  io.stderr:write([[
Usage:
  lua examples/route_parse_demo.lua [--family 4|6|both]
]])
end

local os_routing = require("lua_libp2p.os_routing")

local function parse_args(args)
  local family = "both"
  local i = 1
  while i <= #args do
    local name = args[i]
    local value = args[i + 1]
    if name == "--family" then
      if value ~= "4" and value ~= "6" and value ~= "both" then
        return nil, "--family must be one of: 4, 6, both"
      end
      family = value
      i = i + 2
    else
      return nil, "unknown argument: " .. tostring(name)
    end
  end
  return { family = family }
end

local opts, err = parse_args(arg)
if not opts then
  io.stderr:write(tostring(err) .. "\n")
  usage()
  os.exit(2)
end

if opts.family == "4" or opts.family == "both" then
  local route4, route4_err = os_routing.default_route_v4()
  if route4 then
    print("ipv4 default gateway: " .. tostring(route4.gateway))
    print("ipv4 interface: " .. tostring(route4.ifname))
  else
    print("ipv4 default route: unavailable (" .. tostring(route4_err) .. ")")
  end
end

if opts.family == "6" or opts.family == "both" then
  local route6, route6_err = os_routing.default_route_v6()
  if route6 then
    print("ipv6 default gateway: " .. tostring(route6.gateway))
    print("ipv6 interface: " .. tostring(route6.ifname))
    local candidates, candidates_err = os_routing.router_candidates_v6()
    if candidates then
      print("ipv6 router candidates:")
      for _, candidate in ipairs(candidates) do
        print(
          "  "
            .. tostring(candidate.gateway)
            .. " if="
            .. tostring(candidate.ifname)
            .. " reason="
            .. tostring(candidate.reason)
        )
      end
    else
      print("ipv6 router candidates: unavailable (" .. tostring(candidates_err) .. ")")
    end
  else
    print("ipv6 default route: unavailable (" .. tostring(route6_err) .. ")")
  end
end
