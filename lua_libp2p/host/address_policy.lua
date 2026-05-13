--- Host address-family policy helpers for dialing.
local multiaddr = require("lua_libp2p.multiformats.multiaddr")

local M = {}

local function dialable_families_from_listen_addrs(listen_addrs)
  local allow_ip4 = false
  local allow_ip6 = false
  for _, addr in ipairs(listen_addrs or {}) do
    local parsed = multiaddr.parse(addr)
    if parsed and type(parsed.components) == "table" then
      local host_part = parsed.components[1]
      if host_part and host_part.protocol == "ip4" then
        allow_ip4 = true
      elseif host_part and host_part.protocol == "ip6" then
        allow_ip6 = true
      end
    end
  end
  if not allow_ip4 and not allow_ip6 then
    return true, true
  end
  return allow_ip4, allow_ip6
end

local function is_dialable_for_families(addr, allow_ip4, allow_ip6)
  local parsed = multiaddr.parse(addr)
  if not parsed or type(parsed.components) ~= "table" or #parsed.components < 2 then
    return false
  end
  local host_part = parsed.components[1]
  local tcp_part = parsed.components[2]
  if host_part.protocol == "ip4" then
    if not allow_ip4 then
      return false
    end
  elseif host_part.protocol == "ip6" then
    if not allow_ip6 then
      return false
    end
  elseif host_part.protocol == "dns" then
    if not (allow_ip4 or allow_ip6) then
      return false
    end
  elseif host_part.protocol == "dns4" then
    if not allow_ip4 then
      return false
    end
  elseif host_part.protocol == "dns6" then
    if not allow_ip6 then
      return false
    end
  else
    return false
  end
  if tcp_part.protocol ~= "tcp" then
    return false
  end
  for i = 3, #parsed.components do
    local proto = parsed.components[i].protocol
    if proto ~= "p2p" then
      return false
    end
  end
  return true
end

function M.install(Host)
  function Host:_dialable_ip_families()
    return dialable_families_from_listen_addrs(self.listen_addrs)
  end

  function Host:is_dialable_addr(addr)
    local allow_ip4, allow_ip6 = self:_dialable_ip_families()
    return is_dialable_for_families(addr, allow_ip4, allow_ip6)
  end

  function Host:filter_dialable_addrs(addrs)
    local out = {}
    for _, addr in ipairs(addrs or {}) do
      if self:is_dialable_addr(addr) then
        out[#out + 1] = addr
      end
    end
    return out
  end
end

return M
