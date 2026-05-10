--- PCP mapping service.
-- @module lua_libp2p.pcp.service
local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log").subsystem("pcp")
local multiaddr = require("lua_libp2p.multiaddr")
local os_routing = require("lua_libp2p.os_routing")
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
  return normalized:match("^fe8") ~= nil
    or normalized:match("^fe9") ~= nil
    or normalized:match("^fea") ~= nil
    or normalized:match("^feb") ~= nil
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

function Service:_client_for_gateway(gateway)
  if self.client then
    return self.client
  end
  local c, err = pcp_client.new({
    gateway = gateway,
    timeout = self.timeout,
    retries = self.retries,
  })
  if not c then
    return nil, err
  end
  if gateway == self.gateway then
    self.client = c
  end
  return c
end

function Service:_gateway_candidates()
  local seen = {}
  local out = {}
  local function add(gateway)
    if type(gateway) ~= "string" or gateway == "" or seen[gateway] then
      return
    end
    seen[gateway] = true
    out[#out + 1] = gateway
  end

  if type(self._selected_gateway) == "string" and self._selected_gateway ~= "" then
    add(self._selected_gateway)
  end
  if type(self.gateway) == "string" and self.gateway ~= "" and self.gateway ~= "auto" then
    add(self.gateway)
    return out
  end

  local snapshot, snapshot_err = self.os_routing.snapshot()
  if not snapshot then
    return nil, snapshot_err
  end
  if snapshot.default_route_v6 and snapshot.default_route_v6.gateway then
    add(snapshot.default_route_v6.gateway)
  end
  for _, candidate in ipairs(snapshot.router_candidates_v6 or {}) do
    add(candidate.gateway)
  end
  if snapshot.default_route_v4 and snapshot.default_route_v4.gateway then
    add(snapshot.default_route_v4.gateway)
  end
  add("2001:1::1")
  add("192.0.0.9")

  if #out == 0 then
    return nil, error_mod.new("state", "no PCP gateway candidates available")
  end
  return out
end

function Service:_eligible_addrs(gateway)
  local out = {}
  local gateway_is_v6 = is_ipv6(gateway)
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
        if
          self.internal_client
          and (
            (required_ip_proto == "ip6" and is_ipv6(self.internal_client))
            or (required_ip_proto == "ip4" and not is_ipv6(self.internal_client))
          )
        then
          parsed.ip = self.internal_client
        else
          goto continue_addr
        end
      end
      if self.internal_client and parsed.ip ~= self.internal_client then
        goto continue_addr
      end
      if not is_loopback_ip(parsed.ip) and not is_link_local_ip(parsed.ip) then
        out[#out + 1] = parsed
      end
    end
    ::continue_addr::
  end
  log.debug("pcp eligible addresses selected", {
    candidates = #out,
    gateway = gateway,
  })
  return out
end

--- Create/refresh PCP mappings for eligible listen addresses.
-- @treturn table|nil mappings
-- @treturn[opt] table err
function Service:map_ip_addresses()
  if self._mapping_in_progress then
    log.debug("pcp mapping refresh skipped", {
      gateway = self.gateway,
      reason = "mapping_in_progress",
    })
    return {}, nil
  end
  self._mapping_in_progress = true
  log.debug("pcp mapping refresh started", {
    gateway = self.gateway,
    ttl = self.ttl,
  })

  local gateways, gateways_err = self:_gateway_candidates()
  if not gateways then
    self._mapping_in_progress = false
    self.last_error = gateways_err
    emit_event(self.host, "pcp:mapping:failed", { error = gateways_err, error_message = tostring(gateways_err) })
    return nil, gateways_err
  end

  local mapped = {}
  local last_err = nil
  local had_candidates = false
  for _, gateway in ipairs(gateways) do
    local candidates = self:_eligible_addrs(gateway)
    if #candidates == 0 then
      goto continue_gateway
    end
    had_candidates = true
    local client, client_err = self:_client_for_gateway(gateway)
    if not client then
      last_err = client_err
      goto continue_gateway
    end
    for _, addr in ipairs(candidates) do
      local key = mapping_key("tcp", addr.ip, addr.port)
      if self.mappings[key] then
        log.debug("pcp mapping skipped", {
          gateway = gateway,
          internal_client = addr.ip,
          internal_port = addr.port,
          reason = "already_mapped",
        })
        goto continue_map
      end
      local requested_external_port = self.external_port or addr.port
      local suggested_external_ip = addr.ip_proto == "ip6" and "::" or "0.0.0.0"
      log.debug("pcp mapping attempt", {
        gateway = gateway,
        internal_client = addr.ip,
        internal_port = addr.port,
        protocol = "tcp",
        requested_external_port = requested_external_port,
      })
      local mapping, map_err =
        client:map_port("tcp", addr.port, requested_external_port, self.ttl, suggested_external_ip, {
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
        self._selected_gateway = gateway
        self.mappings[key] = info
        mapped[#mapped + 1] = info
        log.info("pcp mapping active", {
          gateway = gateway,
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
        last_err = map_err
        log.warn("pcp mapping failed", {
          gateway = gateway,
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
    ::continue_gateway::
  end

  if not had_candidates then
    local err = error_mod.new("state", "no eligible private tcp transport addresses for PCP mapping")
    self.last_error = err
    log.debug("pcp mapping refresh skipped", {
      gateway = self.gateway,
      reason = "no_eligible_addresses",
    })
    emit_event(self.host, "pcp:mapping:failed", { error = err, error_message = tostring(err) })
    self._mapping_in_progress = false
    return {}, nil
  end
  if #mapped == 0 and last_err ~= nil then
    self.last_error = last_err
  end

  log.debug("pcp mapping refresh completed", {
    gateway = self.gateway,
    mapped = #mapped,
    candidates = #gateways,
  })
  self._mapping_in_progress = false
  return mapped
end

--- Start PCP service and optional self-update hook.
-- @treturn true|nil ok
-- @treturn[opt] table err
function Service:start()
  if self.started then
    return true
  end
  self.started = true
  log.debug("pcp service started", {
    gateway = self.gateway,
    ttl = self.ttl,
    map_on_self_peer_update = self.map_on_self_peer_update,
    initial_map_delay_seconds = self.initial_map_delay_seconds,
  })
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

  if self.host and type(self.host.spawn_task) == "function" then
    local delay = tonumber(self.initial_map_delay_seconds) or 0
    local task, task_err = self.host:spawn_task("pcp.initial_map", function(ctx)
      log.debug("pcp initial mapping delayed", {
        delay_seconds = delay,
      })
      if delay > 0 then
        local ok, sleep_err = ctx:sleep(delay)
        if ok == nil and sleep_err then
          return nil, sleep_err
        end
      end
      return run_initial_map()
    end, { service = "pcp" })
    if not task then
      return nil, task_err
    end
    self._initial_map_task = task
    return true
  end

  return run_initial_map()
end

--- Stop PCP service and remove advertised mappings.
-- @treturn true
function Service:stop()
  self.started = false
  if self._initial_map_task and self.host and type(self.host.cancel_task) == "function" then
    self.host:cancel_task(self._initial_map_task.id)
  end
  self._initial_map_task = nil
  log.debug("pcp service stopped", {
    mappings = (function()
      local n = 0
      for _ in pairs(self.mappings) do
        n = n + 1
      end
      return n
    end)(),
  })
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

--- Build a PCP service instance.
-- Common `opts`: `gateway` (optional, defaults to `"auto"`), `client`, `internal_client`,
-- `external_port`, `timeout`, `retries`, `ttl`, `auto_confirm_address`,
-- `fail_on_start_error`, `map_on_self_peer_update`, and `initial_map_delay_seconds`.
-- `opts.gateway` may be a router IP string or `"auto"`.
-- `opts.timeout` defaults to `0.25`, `opts.retries` defaults to `6`, `opts.ttl` defaults to `7200`.
-- @tparam table host Host instance.
-- @tparam[opt] table opts
-- @treturn table|nil service
-- @treturn[opt] table err
function M.new(host, opts)
  if type(host) ~= "table" then
    return nil, error_mod.new("input", "pcp service requires host")
  end
  local options = opts or {}
  return setmetatable({
    host = host,
    client = options.client,
    gateway = options.gateway or "auto",
    os_routing = options.os_routing or os_routing,
    _selected_gateway = nil,
    internal_client = options.internal_client,
    external_port = options.external_port,
    timeout = options.timeout or 0.25,
    retries = options.retries == nil and 6 or options.retries,
    ttl = options.ttl or 7200,
    auto_confirm_address = options.auto_confirm_address == true,
    fail_on_start_error = options.fail_on_start_error == true,
    map_on_self_peer_update = options.map_on_self_peer_update ~= false,
    initial_map_delay_seconds = tonumber(options.initial_map_delay_seconds) or 0,
    _initial_map_task = nil,
    mappings = {},
    started = false,
  }, Service)
end

M.Service = Service

return M
