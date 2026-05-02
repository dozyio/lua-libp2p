local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log")
local multiaddr = require("lua_libp2p.multiaddr")
local pcp_client = require("lua_libp2p.pcp.client")

local M = {}
M.provides = { "pcp" }
M.requires = {}

local Service = {}
Service.__index = Service

local function is_ipv6(ip)
  return type(ip) == "string" and ip:find(":", 1, true) ~= nil
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
  return { addr = addr, ip = ip, ip_proto = ip_proto, protocol = protocol, port = port }
end

local function is_loopback_ip(ip)
  return type(ip) == "string" and (ip:match("^127%.") ~= nil or ip == "::1")
end

local function is_link_local_ip(ip)
  if type(ip) ~= "string" then
    return false
  end
  if ip:match("^169%.254%.") then
    return true
  end
  local normalized = ip:gsub("%%.+$", ""):lower()
  return normalized:match("^fe8") ~= nil or normalized:match("^fe9") ~= nil
    or normalized:match("^fea") ~= nil or normalized:match("^feb") ~= nil
end

local function external_addr(ip, protocol, port)
  local ip_proto = ip and ip:find(":", 1, true) and "ip6" or "ip4"
  return "/" .. ip_proto .. "/" .. ip .. "/" .. protocol .. "/" .. tostring(port)
end

local function emit_event(host, name, payload)
  if host and type(host.emit) == "function" then
    local ok, err = host:emit(name, payload)
    if not ok then
      log.warn("pcp event handler failed", { event = name, cause = tostring(err) })
    end
  end
  return true
end

local function mapping_key(protocol, ip, port)
  return tostring(protocol) .. ":" .. tostring(ip) .. ":" .. tostring(port)
end

function Service:_client()
  if self.client then
    return self.client
  end
  local c, err = pcp_client.new({
    gateway = self.gateway,
    timeout = self.timeout,
    retries = self.retries,
  })
  if not c then
    return nil, err
  end
  self.client = c
  return c
end

function Service:_eligible_addrs()
  local out = {}
  local gateway_is_v6 = is_ipv6(self.gateway)
  local required_ip_proto = gateway_is_v6 and "ip6" or "ip4"
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
      if parsed.ip_proto ~= required_ip_proto then
        goto continue_addr
      end
      if parsed.ip == "0.0.0.0" or parsed.ip == "::" then
        if self.internal_client and ((required_ip_proto == "ip6" and is_ipv6(self.internal_client)) or (required_ip_proto == "ip4" and not is_ipv6(self.internal_client))) then
          parsed.ip = self.internal_client
        else
          goto continue_addr
        end
      end
      if self.internal_client and parsed.ip ~= self.internal_client then
        goto continue_addr
      end
      if not is_loopback_ip(parsed.ip)
        and not is_link_local_ip(parsed.ip)
      then
        out[#out + 1] = parsed
      end
    end
    ::continue_addr::
  end
  return out
end

function Service:map_ip_addresses()
  local client, client_err = self:_client()
  if not client then
    self.last_error = client_err
    emit_event(self.host, "pcp:mapping:failed", { error = client_err, error_message = tostring(client_err) })
    return nil, client_err
  end
  local candidates = self:_eligible_addrs()
  if #candidates == 0 then
    local err = error_mod.new("state", "no eligible private tcp transport addresses for PCP mapping")
    self.last_error = err
    emit_event(self.host, "pcp:mapping:failed", { error = err, error_message = tostring(err) })
    return {}, nil
  end

  local mapped = {}
  for _, addr in ipairs(candidates) do
    local key = mapping_key("tcp", addr.ip, addr.port)
    if self.mappings[key] then
      goto continue_map
    end
    local requested_external_port = self.external_port or addr.port
    local suggested_external_ip = addr.ip_proto == "ip6" and "::" or "0.0.0.0"
    log.debug("pcp mapping attempt", {
      gateway = self.gateway,
      internal_client = addr.ip,
      internal_port = addr.port,
      protocol = "tcp",
      requested_external_port = requested_external_port,
    })
    local mapping, map_err = client:map_port("tcp", addr.port, requested_external_port, self.ttl, suggested_external_ip, {
      source_ip = addr.ip,
    })
    if mapping then
      local ext_addr = external_addr(mapping.external_ip, "tcp", mapping.external_port)
      local info = {
        internal_addr = addr.addr,
        internal_client = addr.ip,
        internal_port = addr.port,
        external_addr = ext_addr,
        external_port = mapping.external_port,
        protocol = "tcp",
        expires = os.time() + (mapping.lifetime or self.ttl),
        verified = self.auto_confirm_address == true,
        status = self.auto_confirm_address and "public" or "unknown",
        source = "pcp",
      }
      if self.host and self.host.address_manager then
        self.host.address_manager:add_public_address_mapping(info)
      end
      self.last_error = nil
      self.mappings[key] = info
      mapped[#mapped + 1] = info
      log.info("pcp mapping active", {
        gateway = self.gateway,
        internal_client = addr.ip,
        internal_port = addr.port,
        external_addr = ext_addr,
        external_port = mapping.external_port,
      })
      emit_event(self.host, "pcp:mapping:active", info)
      if self.host and type(self.host._emit_self_peer_update_if_changed) == "function" then
        self.host:_emit_self_peer_update_if_changed()
      end
    else
      self.last_error = map_err
      log.warn("pcp mapping failed", {
        gateway = self.gateway,
        internal_client = addr.ip,
        internal_port = addr.port,
        cause = tostring(map_err),
      })
      emit_event(self.host, "pcp:mapping:failed", {
        internal_addr = addr.addr,
        internal_client = addr.ip,
        error = map_err,
        error_message = tostring(map_err),
      })
    end
    ::continue_map::
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
    if self.map_on_self_peer_update ~= false then
      self.host:on("self_peer_update", self._event_handler)
    end
  end

  local function run_initial_map()
    local mapped, err = self:map_ip_addresses()
    if not mapped and self.fail_on_start_error then
      return nil, err
    end
    return true
  end

  if self.initial_map_delay_seconds and self.initial_map_delay_seconds > 0
    and self.host and type(self.host.spawn_task) == "function"
  then
    local delay = self.initial_map_delay_seconds
    local task, task_err = self.host:spawn_task("pcp.initial_map_delay", function(ctx)
      local ok, sleep_err = ctx:sleep(delay)
      if ok == nil and sleep_err then
        return nil, sleep_err
      end
      return run_initial_map()
    end, { service = "pcp" })
    if not task then
      return nil, task_err
    end
    return true
  end

  return run_initial_map()
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

function M.new(host, opts)
  if type(host) ~= "table" then
    return nil, error_mod.new("input", "pcp service requires host")
  end
  local options = opts or {}
  if type(options.gateway) ~= "string" or options.gateway == "" then
    return nil, error_mod.new("input", "pcp service requires config.gateway")
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
    map_on_self_peer_update = options.map_on_self_peer_update ~= false,
    initial_map_delay_seconds = tonumber(options.initial_map_delay_seconds) or 0,
    mappings = {},
    started = false,
  }, Service)
end

M.Service = Service

return M
