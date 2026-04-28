local error_mod = require("lua_libp2p.error")
local igd = require("lua_libp2p.upnp.igd")
local log = require("lua_libp2p.log")
local multiaddr = require("lua_libp2p.multiaddr")

local M = {}

local Service = {}
Service.__index = Service

local function is_loopback_ip(ip)
  if type(ip) ~= "string" then
    return false
  end
  if ip:match("^127%.") or ip == "::1" then
    return true
  end
  return false
end

local function parse_transport_addr(addr)
  local parsed = multiaddr.parse(addr)
  if not parsed then
    return nil
  end
  local ip, ip_proto, protocol, port
  for _, component in ipairs(parsed.components) do
    if component.protocol == "ip4" or component.protocol == "ip6" then
      ip = component.value
      ip_proto = component.protocol
    elseif component.protocol == "tcp" or component.protocol == "udp" then
      protocol = component.protocol
      port = tonumber(component.value)
      break
    end
  end
  if not ip or not protocol or not port then
    return nil
  end
  return {
    addr = addr,
    ip = ip,
    ip_proto = ip_proto,
    protocol = protocol,
    port = port,
  }
end

local function external_addr(ip, protocol, port)
  local ip_proto = ip and ip:find(":", 1, true) and "ip6" or "ip4"
  return "/" .. ip_proto .. "/" .. ip .. "/" .. protocol .. "/" .. tostring(port)
end

local function emit_event(host, name, payload)
  if not (host and type(host.emit) == "function") then
    return true
  end
  local ok, err = host:emit(name, payload)
  if not ok then
    log.warn("upnp nat event handler failed", { event = name, cause = tostring(err) })
  end
  return true
end

local function default_description()
  return "lua-libp2p-" .. tostring(math.random(100000, 999999))
end

function Service:_client()
  if self.client then
    return self.client
  end
  local client, err
  if type(self.discover_client) == "function" then
    client, err = self.discover_client(self.options)
  else
    client, err = igd.discover(self.options)
  end
  if not client then
    return nil, err
  end
  if self.debug_soap ~= nil then
    client.debug_soap = self.debug_soap
  end
  self.client = client
  return client
end

function Service:_eligible_addrs()
  local addrs = {}
  local source = {}
  if self.host and self.host.address_manager then
    if type(self.host.address_manager.get_transport_addrs) == "function" then
      source = self.host.address_manager:get_transport_addrs()
    else
      source = self.host.address_manager:get_listen_addrs()
    end
  end
  for _, addr in ipairs(source) do
    local parsed = parse_transport_addr(addr)
    if parsed and (parsed.protocol == "tcp" or parsed.protocol == "udp") then
      if parsed.ip == "0.0.0.0" or parsed.ip == "::" then
        if self.internal_client then
          parsed.ip = self.internal_client
        else
          goto continue_addr
        end
      end
      if multiaddr.is_private_addr("/" .. parsed.ip_proto .. "/" .. parsed.ip .. "/" .. parsed.protocol .. "/" .. parsed.port)
        and not is_loopback_ip(parsed.ip)
      then
        addrs[#addrs + 1] = parsed
      end
    end
    ::continue_addr::
  end
  return addrs
end

function Service:map_ip_addresses()
  local client, client_err = self:_client()
  if not client then
    self.last_error = client_err
    emit_event(self.host, "upnp_nat:mapping:failed", { error = client_err, error_message = tostring(client_err) })
    return nil, client_err
  end
  local external_ip, ip_err = client:get_external_ip()
  if not external_ip then
    self.last_error = ip_err
    emit_event(self.host, "upnp_nat:mapping:failed", { error = ip_err, error_message = tostring(ip_err) })
    return nil, ip_err
  end
  if multiaddr.is_private_addr("/ip4/" .. external_ip .. "/tcp/1") then
    local err = error_mod.new("state", "UPnP external address is private; likely double NAT", { external_ip = external_ip })
    self.last_error = err
    emit_event(self.host, "upnp_nat:mapping:failed", { external_ip = external_ip, error = err, error_message = tostring(err) })
    return nil, err
  end

  local candidates = self:_eligible_addrs()
  if #candidates == 0 then
    local err = error_mod.new("state", "no eligible private transport addresses for UPnP mapping")
    self.last_error = err
    emit_event(self.host, "upnp_nat:mapping:failed", { error = err, error_message = tostring(err) })
    return {}, nil
  end

  local mapped = {}
  for _, addr in ipairs(candidates) do
    local requested_external_port = self.external_port or addr.port
    if self.replace_existing and type(client.delete_port_mapping) == "function" then
      pcall(function()
        client:delete_port_mapping(addr.protocol, requested_external_port)
      end)
    end
    local mapping, map_err = client:add_port_mapping(
      addr.protocol,
      addr.port,
      requested_external_port,
      addr.ip,
      self.ttl,
      self.description
    )
    if mapping then
      local ext_port = mapping.external_port or requested_external_port
      local actual
      if type(client.get_specific_port_mapping) == "function" then
        actual = client:get_specific_port_mapping(addr.protocol, ext_port)
      end
      if actual
          and actual.internal_client ~= ""
          and (actual.internal_client ~= addr.ip or actual.internal_port ~= addr.port) then
        local mismatch_err = error_mod.new("state", "UPnP mapping points at a different internal address", {
          expected_internal_client = addr.ip,
          expected_internal_port = addr.port,
          actual_internal_client = actual.internal_client,
          actual_internal_port = actual.internal_port,
          external_port = ext_port,
        })
        self.last_error = mismatch_err
        emit_event(self.host, "upnp_nat:mapping:failed", {
          internal_addr = addr.addr,
          external_port = ext_port,
          actual = actual,
          error = mismatch_err,
          error_message = tostring(mismatch_err),
        })
        goto continue_addr
      end
      local ext_addr = external_addr(external_ip, addr.protocol, ext_port)
      local info = {
        internal_addr = addr.addr,
        internal_client = addr.ip,
        internal_port = addr.port,
        external_addr = ext_addr,
        external_port = ext_port,
        protocol = addr.protocol,
        expires = os.time() + self.ttl,
        verified = self.auto_confirm_address == true,
        status = self.auto_confirm_address and "public" or "unknown",
        source = "upnp_nat",
      }
      if actual then
        info.actual_internal_client = actual.internal_client
        info.actual_internal_port = actual.internal_port
      end
      if self.host and self.host.address_manager then
        self.host.address_manager:add_public_address_mapping(info)
      end
      self.last_error = nil
      self.mappings[addr.protocol .. ":" .. tostring(addr.port)] = info
      mapped[#mapped + 1] = info
      emit_event(self.host, "upnp_nat:mapping:active", info)
      if self.host and type(self.host._emit_self_peer_update_if_changed) == "function" then
        self.host:_emit_self_peer_update_if_changed()
      end
    else
      self.last_error = map_err
      emit_event(self.host, "upnp_nat:mapping:failed", {
        internal_addr = addr.addr,
        error = map_err,
        error_message = tostring(map_err),
      })
    end
    ::continue_addr::
  end
  return mapped
end

function Service:start()
  if self.started then
    return true
  end
  self.started = true
  if self.host and type(self.host.on) == "function" then
    self._event_handler = function()
      return self:map_ip_addresses()
    end
    self.host:on("self_peer_update", self._event_handler)
  end
  local mapped, err = self:map_ip_addresses()
  if not mapped and self.fail_on_start_error then
    return nil, err
  end
  return true
end

function Service:stop()
  self.started = false
  if self.host and self._event_handler and type(self.host.off) == "function" then
    self.host:off("self_peer_update", self._event_handler)
  end
  for _, mapping in pairs(self.mappings) do
    if self.host and self.host.address_manager then
      self.host.address_manager:remove_public_address_mapping(mapping.external_addr)
    end
  end
  self.mappings = {}
  return true
end

function Service:status()
  return {
    started = self.started,
    mappings = self.mappings,
    last_error = self.last_error,
  }
end

function Service:list_port_mappings(opts)
  local client, err = self:_client()
  if not client then
    return nil, err
  end
  if type(client.list_port_mappings) ~= "function" then
    return nil, error_mod.new("unsupported", "UPnP client does not support mapping enumeration")
  end
  return client:list_port_mappings(opts)
end

function M.new(host, opts)
  if type(host) ~= "table" then
    return nil, error_mod.new("input", "upnp nat service requires host")
  end
  local options = opts or {}
  return setmetatable({
    host = host,
    options = options,
    client = options.client,
    discover_client = options.discover_client,
    internal_client = options.internal_client,
    external_port = options.external_port,
    replace_existing = options.replace_existing == true,
    debug_soap = options.debug_soap == true,
    ttl = options.ttl or 720,
    auto_confirm_address = options.auto_confirm_address == true,
    description = options.description or default_description(),
    fail_on_start_error = options.fail_on_start_error == true,
    mappings = {},
    started = false,
  }, Service)
end

M.Service = Service

return M
