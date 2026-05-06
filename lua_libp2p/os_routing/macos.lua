--- macOS route and neighbor discovery helpers.
-- @module lua_libp2p.os_routing.macos
local M = {}

local function run_command(command, runner)
  local run = runner
  if type(run) ~= "function" then
    run = function(cmd)
      local pipe = io.popen(cmd)
      if not pipe then
        return nil, "failed to run command"
      end
      local out = pipe:read("*a") or ""
      local ok, why, code = pipe:close()
      if not ok and out == "" then
        if why and code then
          return nil, tostring(why) .. ": " .. tostring(code)
        end
        return nil, "command failed"
      end
      return out
    end
  end
  return run(command)
end

local function trim(value)
  return (value or ""):gsub("^%s+", ""):gsub("%s+$", "")
end

function M.parse_route_get_default(output)
  local gateway
  local ifname
  for line in (output or ""):gmatch("[^\n]+") do
    if not gateway then
      gateway = line:match("^%s*gateway:%s*(%S+)")
    end
    if not ifname then
      ifname = line:match("^%s*interface:%s*(%S+)")
    end
  end
  if not gateway or gateway == "" then
    return nil, "gateway not found"
  end
  gateway = trim(gateway)
  ifname = trim(ifname)
  if gateway:lower():match("^fe80:") and ifname ~= "" and not gateway:find("%", 1, true) then
    gateway = gateway .. "%" .. ifname
  end
  return {
    gateway = gateway,
    ifname = ifname ~= "" and ifname or nil,
  }
end

function M.parse_ndp_neighbors(output)
  local out = {}
  for line in (output or ""):gmatch("[^\n]+") do
    if not line:match("^Neighbor%s+") and trim(line) ~= "" then
      local address, lladdr, netif, expire, state, flags, probes =
        line:match("^(%S+)%s+(%S+)%s+(%S+)%s+(%S+)%s*(%S*)%s*(%S*)%s*(%S*)")
      if address and netif then
        local _, scoped_ifname = address:match("^([^%%]+)%%(.+)$")
        if scoped_ifname and scoped_ifname ~= "" then
          netif = scoped_ifname
        end
        local is_router = false
        if flags and flags:find("R", 1, true) then
          is_router = true
        end
        out[#out + 1] = {
          address = address,
          lladdr = lladdr ~= "(incomplete)" and lladdr or nil,
          ifname = netif,
          expire = expire,
          state = state ~= "" and state or nil,
          flags = flags ~= "" and flags or nil,
          probes = probes ~= "" and probes or nil,
          is_router = is_router,
        }
      end
    end
  end
  return out
end

local function ensure_link_local_scope(address, ifname)
  if type(address) ~= "string" then
    return address
  end
  if address:lower():match("^fe80:") and ifname and ifname ~= "" then
    local base = address:match("^([^%%]+)") or address
    return base .. "%" .. ifname
  end
  return address
end

local function fallback_ipv6_default_route(neighbors)
  for _, entry in ipairs(neighbors or {}) do
    if entry.is_router and type(entry.address) == "string" then
      local candidate = ensure_link_local_scope(entry.address, entry.ifname)
      if candidate:lower():match("^fe80:") then
        return {
          gateway = candidate,
          ifname = entry.ifname,
          source = "ndp_router",
        }, nil
      end
    end
  end
  return nil, "gateway not found"
end

local function normalize_ipv6_base(address)
  if type(address) ~= "string" then
    return nil
  end
  return (address:match("^([^%%]+)") or address):lower()
end

local function ipv6_iid(address)
  local base = normalize_ipv6_base(address)
  if not base or not base:find(":", 1, true) then
    return nil
  end
  local segments = {}
  local head, tail = base:match("^(.-)::(.-)$")
  if head then
    local head_parts = {}
    for part in head:gmatch("[^:]+") do
      head_parts[#head_parts + 1] = part
    end
    local tail_parts = {}
    for part in tail:gmatch("[^:]+") do
      tail_parts[#tail_parts + 1] = part
    end
    local missing = 8 - (#head_parts + #tail_parts)
    if missing < 0 then
      return nil
    end
    for _, part in ipairs(head_parts) do
      segments[#segments + 1] = part
    end
    for _ = 1, missing do
      segments[#segments + 1] = "0"
    end
    for _, part in ipairs(tail_parts) do
      segments[#segments + 1] = part
    end
  else
    for part in base:gmatch("[^:]+") do
      segments[#segments + 1] = part
    end
    if #segments ~= 8 then
      return nil
    end
  end
  if #segments ~= 8 then
    return nil
  end
  return table.concat({ segments[5], segments[6], segments[7], segments[8] }, ":")
end

local function router_candidates_v6(neighbors)
  local out = {}
  local seen = {}
  local function add_candidate(addr, ifname, reason)
    if type(addr) ~= "string" then
      return
    end
    local normalized = normalize_ipv6_base(addr)
    if not normalized or seen[normalized] then
      return
    end
    seen[normalized] = true
    out[#out + 1] = {
      gateway = ensure_link_local_scope(addr, ifname),
      ifname = ifname,
      reason = reason,
    }
  end

  local routers = {}
  local by_iid = {}
  for _, entry in ipairs(neighbors or {}) do
    if entry.is_router and type(entry.address) == "string" then
      routers[#routers + 1] = entry
      local iid = ipv6_iid(entry.address)
      if iid then
        by_iid[iid] = by_iid[iid] or {}
        by_iid[iid][#by_iid[iid] + 1] = entry
      end
    end
  end

  for _, entry in ipairs(routers) do
    local base = normalize_ipv6_base(entry.address)
    if base and base:match("^fe80:") then
      add_candidate(entry.address, entry.ifname, "ndp_link_local_router")
    end
  end
  for _, entry in ipairs(routers) do
    local base = normalize_ipv6_base(entry.address)
    if base and not base:match("^fe80:") then
      add_candidate(entry.address, entry.ifname, "ndp_global_router")
    end
  end
  for iid, list in pairs(by_iid) do
    local has_link_local = false
    local has_global = false
    local ifname = nil
    local global_addr = nil
    for _, entry in ipairs(list) do
      local base = normalize_ipv6_base(entry.address)
      ifname = ifname or entry.ifname
      if base and base:match("^fe80:") then
        has_link_local = true
      elseif base then
        has_global = true
        global_addr = entry.address
      end
    end
    if has_link_local and has_global then
      add_candidate(global_addr, ifname, "ndp_same_iid_global")
    end
  end

  return out
end

function M.snapshot(opts)
  local options = opts or {}
  local route4_out = run_command("route -n get default 2>/dev/null", options.command_runner)
  local route6_out = run_command("route -n get -inet6 default 2>/dev/null", options.command_runner)
  local ndp_out = run_command("ndp -an 2>/dev/null", options.command_runner)

  local neighbors_v6 = ndp_out and M.parse_ndp_neighbors(ndp_out) or {}

  local route4 = nil
  local route4_err = nil
  if route4_out then
    route4, route4_err = M.parse_route_get_default(route4_out)
  end
  local route6 = nil
  local route6_err = nil
  if route6_out then
    route6, route6_err = M.parse_route_get_default(route6_out)
  end
  if not route6 then
    route6, route6_err = fallback_ipv6_default_route(neighbors_v6)
  end

  return {
    os = "macos",
    default_route_v4 = route4,
    default_route_v4_error = route4_err,
    default_route_v6 = route6,
    default_route_v6_error = route6_err,
    neighbors_v6 = neighbors_v6,
    router_candidates_v6 = router_candidates_v6(neighbors_v6),
  }
end

return M
