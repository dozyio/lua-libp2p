--- NAT-PMP mapping service.
--
-- NAT-PMP creates public TCP/UDP mappings for eligible private transport listen
-- addresses and adds the mapped external address to the host address manager.
-- The service emits `nat_pmp:mapping:active` and `nat_pmp:mapping:failed`
-- events.
---@class Libp2pNatPmpServiceConfig
---@field enabled? boolean
---@field gateway? string
---@field internal_port? integer
---@field external_port? integer
---@field ttl? number
---@field protocol? 'tcp'|'udp'
---@field client? Libp2pNatPmpClient

local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log").subsystem("nat_pmp")
local multiaddr = require("lua_libp2p.multiformats.multiaddr")
local nat_pmp_client = require("lua_libp2p.port_mapping.nat_pmp.client")

local M = {}
M.provides = { "nat_pmp" }
M.requires = {}

local Service = {}
Service.__index = Service

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

local function is_loopback_ip(ip)
  if type(ip) ~= "string" then
    return false
  end
  return ip:match("^127%.") ~= nil or ip == "::1"
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
    log.warn("nat-pmp event handler failed", { event = name, cause = tostring(err) })
  end
  return true
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
    if parsed and parsed.protocol == "tcp" then
      if parsed.ip == "0.0.0.0" or parsed.ip == "::" then
        if self.internal_client then
          parsed.ip = self.internal_client
        else
          goto continue_addr
        end
      end
      if
        multiaddr.is_private_addr(
          "/" .. parsed.ip_proto .. "/" .. parsed.ip .. "/" .. parsed.protocol .. "/" .. parsed.port
        ) and not is_loopback_ip(parsed.ip)
      then
        addrs[#addrs + 1] = parsed
      end
    end
    ::continue_addr::
  end
  log.debug("nat-pmp eligible addresses selected", {
    candidates = #addrs,
  })
  return addrs
end

function Service:_client()
  if self.client then
    return self.client
  end
  local client, err = nat_pmp_client.new({
    gateway = self.gateway,
    timeout = self.timeout,
    retries = self.retries,
  })
  if not client then
    return nil, err
  end
  self.client = client
  return client
end

--- Create/refresh NAT-PMP mappings for eligible listen addresses.
--- table|nil mappings
--- table|nil err
function Service:map_ip_addresses()
  log.debug("nat-pmp mapping refresh started", {
    gateway = self.gateway,
    ttl = self.ttl,
  })
  local client, client_err = self:_client()
  if not client then
    self.last_error = client_err
    emit_event(self.host, "nat_pmp:mapping:failed", { error = client_err, error_message = tostring(client_err) })
    return nil, client_err
  end

  local external_ip, ip_meta_or_err = client:get_external_address()
  if not external_ip then
    self.last_error = ip_meta_or_err
    log.debug("nat-pmp external address failed", {
      gateway = self.gateway,
      cause = tostring(ip_meta_or_err),
    })
    emit_event(
      self.host,
      "nat_pmp:mapping:failed",
      { error = ip_meta_or_err, error_message = tostring(ip_meta_or_err) }
    )
    return nil, ip_meta_or_err
  end

  local candidates = self:_eligible_addrs()
  if #candidates == 0 then
    local err = error_mod.new("state", "no eligible private tcp transport addresses for NAT-PMP mapping")
    self.last_error = err
    log.debug("nat-pmp mapping refresh skipped", {
      reason = "no_eligible_addresses",
    })
    emit_event(self.host, "nat_pmp:mapping:failed", { error = err, error_message = tostring(err) })
    return {}, nil
  end

  local mapped = {}
  for _, addr in ipairs(candidates) do
    local requested_external_port = self.external_port or addr.port
    log.debug("nat-pmp mapping attempt", {
      gateway = self.gateway,
      internal_client = addr.ip,
      internal_port = addr.port,
      requested_external_port = requested_external_port,
      protocol = "tcp",
    })
    local mapping, map_err = client:map_port("tcp", addr.port, requested_external_port, self.ttl)
    if mapping then
      local ext_port = mapping.external_port or requested_external_port
      local ext_addr = external_addr(external_ip, "tcp", ext_port)
      local info = {
        internal_addr = addr.addr,
        internal_client = addr.ip,
        internal_port = addr.port,
        external_addr = ext_addr,
        external_port = ext_port,
        protocol = "tcp",
        expires = os.time() + (mapping.lifetime or self.ttl),
        verified = self.auto_confirm_address == true,
        status = self.auto_confirm_address and "public" or "unknown",
        source = "nat_pmp",
      }
      if self.host and self.host.address_manager then
        self.host.address_manager:add_public_address_mapping(info)
      end
      self.last_error = nil
      self.mappings["tcp:" .. tostring(addr.port)] = info
      mapped[#mapped + 1] = info
      log.debug("nat-pmp mapping active", {
        gateway = self.gateway,
        internal_client = addr.ip,
        internal_port = addr.port,
        external_addr = ext_addr,
        external_port = ext_port,
      })
      emit_event(self.host, "nat_pmp:mapping:active", info)
      if self.host and type(self.host._emit_self_peer_update_if_changed) == "function" then
        self.host:_emit_self_peer_update_if_changed()
      end
    else
      self.last_error = map_err
      log.debug("nat-pmp mapping failed", {
        gateway = self.gateway,
        internal_client = addr.ip,
        internal_port = addr.port,
        cause = tostring(map_err),
      })
      emit_event(self.host, "nat_pmp:mapping:failed", {
        internal_addr = addr.addr,
        error = map_err,
        error_message = tostring(map_err),
      })
    end
  end

  log.debug("nat-pmp mapping refresh completed", {
    mapped = #mapped,
    candidates = #candidates,
  })
  return mapped
end

--- Start NAT-PMP service and optional self-update hook.
--- true|nil ok
--- table|nil err
function Service:start()
  if self.started then
    return true
  end
  self.started = true
  log.debug("nat-pmp service started", {
    gateway = self.gateway,
    ttl = self.ttl,
  })
  if self.host and type(self.host.on) == "function" then
    self._event_handler = function()
      return self:map_ip_addresses()
    end
    self.host:on("self:peer_updated", self._event_handler)
  end
  local mapped, err = self:map_ip_addresses()
  if not mapped and self.fail_on_start_error then
    return nil, err
  end
  return true
end

--- Stop NAT-PMP service and remove advertised mappings.
--- true
function Service:stop()
  self.started = false
  log.debug("nat-pmp service stopped", {
    mappings = (function()
      local n = 0
      for _ in pairs(self.mappings) do
        n = n + 1
      end
      return n
    end)(),
  })
  if self.host and self._event_handler and type(self.host.off) == "function" then
    self.host:off("self:peer_updated", self._event_handler)
  end
  for _, mapping in pairs(self.mappings) do
    if self.host and self.host.address_manager then
      self.host.address_manager:remove_public_address_mapping(mapping.external_addr)
    end
  end
  self.mappings = {}
  return true
end

--- Return service status snapshot.
--- table
function Service:status()
  return {
    started = self.started,
    mappings = self.mappings,
    last_error = self.last_error,
    gateway = self.gateway,
  }
end

--- Build a NAT-PMP service instance.
-- Common `opts`: `gateway` (required), `client`, `internal_client`,
-- `external_port`, `timeout`, `retries`, `ttl`, `auto_confirm_address`,
-- and `fail_on_start_error`.
-- `opts.gateway` must be a router IP string.
-- `opts.timeout` defaults to `0.25`, `opts.retries` defaults to `6`, `opts.ttl` defaults to `7200`.
--- host table Host instance.
--- opts? table
--- table|nil service
--- table|nil err
---@param host Libp2pHost
---@param opts? Libp2pNatPmpServiceConfig
---@return table service
function M.new(host, opts)
  if type(host) ~= "table" then
    return nil, error_mod.new("input", "nat-pmp service requires host")
  end
  local options = opts or {}
  if type(options.gateway) ~= "string" or options.gateway == "" then
    return nil, error_mod.new("input", "nat-pmp service requires config.gateway")
  end
  return setmetatable({
    host = host,
    client = options.client,
    gateway = options.gateway,
    internal_client = options.internal_client,
    external_port = options.external_port,
    timeout = options.timeout or 0.25,
    retries = options.retries == nil and 6 or options.retries,
    ttl = options.ttl or 7200,
    auto_confirm_address = options.auto_confirm_address == true,
    fail_on_start_error = options.fail_on_start_error == true,
    mappings = {},
    started = false,
  }, Service)
end

M.Service = Service

return M
