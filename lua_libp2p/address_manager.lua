--- Address manager and advertisement policy.
-- @module lua_libp2p.address_manager
local multiaddr = require("lua_libp2p.multiaddr")
local table_utils = require("lua_libp2p.util.tables")

local M = {}

local AddressManager = {}
AddressManager.__index = AddressManager

local LISTEN_ADDR_EXPANSION_CACHE_TTL = 60

local copy_list = table_utils.copy_list

local function add_unique(out, seen, addr)
  if type(addr) ~= "string" or addr == "" or seen[addr] then
    return
  end
  seen[addr] = true
  out[#out + 1] = addr
end

local function as_set(values)
  local out = {}
  for _, value in ipairs(values or {}) do
    if type(value) == "string" and value ~= "" then
      out[value] = true
    end
  end
  return out
end

local function list_equal(a, b)
  if a == b then
    return true
  end
  if type(a) ~= "table" or type(b) ~= "table" then
    return false
  end
  if #a ~= #b then
    return false
  end
  for i = 1, #a do
    if a[i] ~= b[i] then
      return false
    end
  end
  return true
end

local function local_ipv4_addrs()
  local ok_luv, uv = pcall(require, "luv")
  if not ok_luv or type(uv.interface_addresses) ~= "function" then
    return {}
  end
  local interfaces = uv.interface_addresses()
  local out = {}
  local seen = {}
  for _, entries in pairs(interfaces or {}) do
    for _, entry in ipairs(entries or {}) do
      local addr = entry.address or entry.ip
      if (entry.family == "IPv4" or entry.family == "inet")
          and not entry.internal
          and type(addr) == "string"
          and addr ~= "0.0.0.0"
          and not seen[addr] then
        seen[addr] = true
        out[#out + 1] = addr
      end
    end
  end
  table.sort(out)
  return out
end

local function local_ipv6_addrs()
  local ok_luv, uv = pcall(require, "luv")
  if not ok_luv or type(uv.interface_addresses) ~= "function" then
    return {}
  end
  local interfaces = uv.interface_addresses()
  local out = {}
  local seen = {}
  for _, entries in pairs(interfaces or {}) do
    for _, entry in ipairs(entries or {}) do
      local addr = entry.address or entry.ip
      if (entry.family == "IPv6" or entry.family == "inet6")
          and not entry.internal
          and type(addr) == "string"
          and addr ~= "::"
      then
        local normalized = addr:gsub("%%.+$", "")
        if normalized ~= "" and normalized:sub(1, 5):lower() ~= "fe80:" and not seen[normalized] then
          seen[normalized] = true
          out[#out + 1] = normalized
        end
      end
    end
  end
  table.sort(out)
  return out
end

local function expand_unspecified_listen_addr(addr)
  local parsed = multiaddr.parse(addr)
  if not parsed then
    return { addr }
  end
  local ip_index, transport_index, port
  for i, component in ipairs(parsed.components) do
    if component.protocol == "ip4" or component.protocol == "ip6" then
      ip_index = i
    elseif component.protocol == "tcp" or component.protocol == "udp" then
      transport_index = i
      port = component.value
      break
    end
  end
  if not ip_index or not transport_index or not port then
    return { addr }
  end
  local ip_component = parsed.components[ip_index]
  local ips
  if ip_component.protocol == "ip4" and ip_component.value == "0.0.0.0" then
    ips = local_ipv4_addrs()
  elseif ip_component.protocol == "ip6" and ip_component.value == "::" then
    ips = local_ipv6_addrs()
  else
    return { addr }
  end
  if #ips == 0 then
    return { addr }
  end
  local out = {}
  for _, ip in ipairs(ips) do
    local components = {}
    for i, component in ipairs(parsed.components) do
      components[i] = {
        protocol = component.protocol,
        value = component.value,
      }
    end
    components[ip_index].value = ip
    out[#out + 1] = multiaddr.format({ components = components })
  end
  return out
end

local function expand_listen_addrs(addrs)
  local out = {}
  local seen = {}
  for _, addr in ipairs(addrs or {}) do
    for _, expanded in ipairs(expand_unspecified_listen_addr(addr)) do
      add_unique(out, seen, expanded)
    end
  end
  return out
end

function AddressManager:set_listen_addrs(addrs)
  local next_listen_addrs = copy_list(addrs)
  local now = os.time()
  local cache_valid = self._transport_addrs_expanded_at ~= nil
    and (now - self._transport_addrs_expanded_at) < LISTEN_ADDR_EXPANSION_CACHE_TTL
  local unchanged = list_equal(self._listen_addrs, next_listen_addrs)

  self._listen_addrs = next_listen_addrs
  if not (unchanged and cache_valid) then
    self._transport_addrs = expand_listen_addrs(self._listen_addrs)
    self._transport_addrs_expanded_at = now
  end

  for _, addr in ipairs(self._listen_addrs) do
    self:_classify_private_addr(addr, "transport")
  end
  for _, addr in ipairs(self._transport_addrs) do
    self:_classify_private_addr(addr, "transport")
  end
  return true
end

function AddressManager:get_listen_addrs()
  return copy_list(self._listen_addrs)
end

function AddressManager:get_transport_addrs()
  return copy_list(self._transport_addrs)
end

function AddressManager:set_announce_addrs(addrs)
  self._announce_addrs = copy_list(addrs)
  for _, addr in ipairs(self._announce_addrs) do
    self:_classify_private_addr(addr, "announce")
  end
  return true
end

function AddressManager:add_announce_addr(addr)
  add_unique(self._announce_addrs, as_set(self._announce_addrs), addr)
  self:_classify_private_addr(addr, "announce")
  return true
end

function AddressManager:set_no_announce_addrs(addrs)
  self._no_announce_addrs = copy_list(addrs)
  return true
end

function AddressManager:add_observed_addr(addr)
  add_unique(self._observed_addrs, as_set(self._observed_addrs), addr)
  self:_classify_private_addr(addr, "observed")
  return true
end

function AddressManager:get_observed_addrs()
  return copy_list(self._observed_addrs)
end

function AddressManager:add_public_address_mapping(mapping)
  if type(mapping) ~= "table" or type(mapping.external_addr) ~= "string" or mapping.external_addr == "" then
    return false
  end
  local addr = mapping.external_addr
  add_unique(self._public_mapping_addrs, as_set(self._public_mapping_addrs), addr)
  local existing = self._reachability[addr]
  local existing_verified = type(existing) == "table" and existing.verified == true
  local incoming_verified = mapping.verified == true
  local metadata = {
    verified = incoming_verified or existing_verified,
    type = "ip-mapping",
    source = existing_verified and not incoming_verified and existing.source or (mapping.source or "upnp_nat"),
    status = incoming_verified and (mapping.status or "public")
      or (existing_verified and (existing.status or "public"))
      or (mapping.status or "unknown"),
    internal_addr = mapping.internal_addr,
    external_addr = addr,
    expires = mapping.expires,
    last_verified = incoming_verified and os.time() or (existing_verified and existing.last_verified or nil),
  }
  if type(existing) == "table" and existing_verified and not incoming_verified then
    for k, v in pairs(existing) do
      if metadata[k] == nil then
        metadata[k] = v
      end
    end
  end
  for k, v in pairs(mapping) do
    if metadata[k] == nil and not (existing_verified and not incoming_verified and (k == "verified" or k == "status" or k == "source" or k == "last_verified")) then
      metadata[k] = v
    end
  end
  self:set_reachability(addr, metadata)
  return true
end

function AddressManager:verify_public_address_mapping(addr, info)
  if type(addr) ~= "string" or addr == "" then
    return false
  end
  local existing = self._reachability[addr] or {}
  local metadata = {}
  for k, v in pairs(existing) do
    metadata[k] = v
  end
  for k, v in pairs(info or {}) do
    metadata[k] = v
  end
  metadata.verified = true
  metadata.status = "public"
  metadata.type = metadata.type or "ip-mapping"
  metadata.source = metadata.source or "autonat_v2"
  metadata.last_verified = os.time()
  add_unique(self._public_mapping_addrs, as_set(self._public_mapping_addrs), addr)
  self:set_reachability(addr, metadata)
  return true
end

function AddressManager:remove_public_address_mapping(external_addr)
  local removed = false
  for i = #self._public_mapping_addrs, 1, -1 do
    if self._public_mapping_addrs[i] == external_addr then
      table.remove(self._public_mapping_addrs, i)
      removed = true
    end
  end
  self:clear_reachability(external_addr)
  return removed
end

function AddressManager:get_public_address_mappings()
  return copy_list(self._public_mapping_addrs)
end

function AddressManager:_classify_private_addr(addr, addr_type)
  if type(addr) ~= "string" or addr == "" then
    return false
  end
  if multiaddr.is_private_addr(addr) then
    self:set_reachability(addr, {
      verified = false,
      type = addr_type or "observed",
      source = "address_manager",
      status = "private",
      last_verified = os.time(),
    })
    return true
  end
  return false
end

function AddressManager:set_reachability(addr, info)
  if type(addr) ~= "string" or addr == "" then
    return false
  end
  local value = {}
  for k, v in pairs(info or {}) do
    value[k] = v
  end
  self._reachability[addr] = value
  return true
end

function AddressManager:clear_reachability(addr)
  if type(addr) ~= "string" or addr == "" then
    return false
  end
  self._reachability[addr] = nil
  return true
end

function AddressManager:get_reachability(addr)
  local value = self._reachability[addr]
  if type(value) ~= "table" then
    return nil
  end
  local out = {}
  for k, v in pairs(value) do
    out[k] = v
  end
  return out
end

function AddressManager:add_relay_addr(addr, info)
  add_unique(self._relay_addrs, as_set(self._relay_addrs), addr)
  if type(addr) == "string" and addr ~= "" then
    local metadata = {
      verified = true,
      type = "transport",
      source = "autorelay",
      status = "public",
      last_verified = os.time(),
    }
    for k, v in pairs(info or {}) do
      metadata[k] = v
    end
    self:set_reachability(addr, metadata)
  end
  return true
end

function AddressManager:remove_relay_addr(addr)
  for i = #self._relay_addrs, 1, -1 do
    if self._relay_addrs[i] == addr then
      table.remove(self._relay_addrs, i)
      self:clear_reachability(addr)
      return true
    end
  end
  return false
end

function AddressManager:remove_relay_addrs(addrs)
  local removed = 0
  for _, addr in ipairs(addrs or {}) do
    if self:remove_relay_addr(addr) then
      removed = removed + 1
    end
  end
  return removed
end

function AddressManager:get_relay_addrs()
  return copy_list(self._relay_addrs)
end

function AddressManager:get_advertise_addrs()
  local out = {}
  local seen = {}
  local blocked = as_set(self._no_announce_addrs)
  local sources = {}
  if #self._announce_addrs > 0 then
    sources[#sources + 1] = self._announce_addrs
  else
    sources[#sources + 1] = self._transport_addrs
    if self._advertise_observed then
      sources[#sources + 1] = self._observed_addrs
    end
  end
  sources[#sources + 1] = self._relay_addrs
  for _, addr in ipairs(self._public_mapping_addrs) do
    local metadata = self._reachability[addr]
    if metadata and metadata.verified == true then
      sources[#sources + 1] = { addr }
    end
  end

  for _, list in ipairs(sources) do
    for _, addr in ipairs(list) do
      if not blocked[addr] then
        add_unique(out, seen, addr)
      end
    end
  end
  return out
end

--- Construct an address manager.
-- `opts` keys include `listen_addrs`, `announce_addrs`, `no_announce_addrs`,
-- `observed_addrs`, `relay_addrs`, `public_mapping_addrs`, and `advertise_observed`.
-- @tparam[opt] table opts
-- @treturn table manager
function M.new(opts)
  local options = opts or {}
  local manager = setmetatable({
    _listen_addrs = copy_list(options.listen_addrs),
    _transport_addrs = expand_listen_addrs(options.listen_addrs),
    _announce_addrs = copy_list(options.announce_addrs),
    _no_announce_addrs = copy_list(options.no_announce_addrs),
    _observed_addrs = copy_list(options.observed_addrs),
    _relay_addrs = copy_list(options.relay_addrs),
    _public_mapping_addrs = copy_list(options.public_mapping_addrs),
    _reachability = {},
    _advertise_observed = options.advertise_observed == true,
    _transport_addrs_expanded_at = os.time(),
  }, AddressManager)
  for _, addr in ipairs(manager._listen_addrs) do
    manager:_classify_private_addr(addr, "transport")
  end
  for _, addr in ipairs(manager._transport_addrs) do
    manager:_classify_private_addr(addr, "transport")
  end
  for _, addr in ipairs(manager._announce_addrs) do
    manager:_classify_private_addr(addr, "announce")
  end
  for _, addr in ipairs(manager._observed_addrs) do
    manager:_classify_private_addr(addr, "observed")
  end
  return manager
end

M.AddressManager = AddressManager

return M
