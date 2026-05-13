--- Multicast DNS peer discovery service for LAN peer discovery.
---
--- This service advertises reachable host addresses as `dnsaddr=` TXT records
--- and emits `peer_discovered` events when nearby libp2p peers are found.
local dns = require("lua_libp2p.peer_discovery_mdns.dns")
local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log").subsystem("mdns")
local multiaddr = require("lua_libp2p.multiformats.multiaddr")
local table_utils = require("lua_libp2p.util.tables")

local M = {
  provides = { "peer_discovery_mdns" },
}

M.SERVICE_NAME = "_p2p._udp.local"
M.META_QUERY = "_services._dns-sd._udp.local"
M.MULTICAST_ADDR = "224.0.0.251"
M.MULTICAST_ADDR6 = "ff02::fb"
M.MULTICAST_PORT = 5353
M.DNSADDR_PREFIX = "dnsaddr="

---@class Libp2pMdnsConfig
---@field service_name? string mDNS service name. Default `_p2p._udp.local`.
---@field peer_name? string DNS-SD instance label. Random 32-byte label when omitted.
---@field peer_name_length? integer Random peer name length when `peer_name` is omitted.
---@field multicast_addr? string IPv4 multicast group. Default `224.0.0.251`.
---@field multicast_addr6? string IPv6 multicast group. Default `ff02::fb`.
---@field port? integer UDP port. Default `5353`.
---@field bind_addr? string IPv4 bind address. Default `0.0.0.0`.
---@field bind_addr6? string IPv6 bind address. Default `::`.
---@field interval? number Query interval in seconds. Default `10`; `0` disables periodic queries.
---@field peer_ttl? number Discovered peer TTL in seconds. Default `120`.
---@field broadcast? boolean Whether to answer mDNS queries. Default `true`.
---@field allow_public_addrs? boolean Whether to advertise public IP addresses. Default `false`.
---@field ipv4? boolean Whether to open IPv4 mDNS socket. Default `true`.
---@field ipv6? boolean Whether to open IPv6 mDNS socket. Default `true`.
---@field require_membership? boolean Whether multicast membership failure is fatal for a socket. Default `true`.
---@field query_on_start? boolean Whether to send an immediate query when started. Default `true`.
---@field socket_factory? fun(service: Libp2pMdnsService): table Test hook returning socket-like object.

---@class Libp2pMdnsService
---@field host table|nil
---@field service_name string
---@field peer_name string
---@field multicast_addr string
---@field multicast_addr6 string
---@field port integer
---@field bind_addr string
---@field bind_addr6 string
---@field interval number
---@field peer_ttl number
---@field broadcast boolean
---@field allow_public_addrs boolean
---@field ipv4 boolean
---@field ipv6 boolean
---@field require_membership boolean
---@field query_on_start boolean
---@field started boolean

local Mdns = {}
Mdns.__index = Mdns

local copy_list = table_utils.copy_list

local function lower(value)
  return tostring(value or ""):lower()
end

local function has_suffix(value, suffix)
  value = lower(value)
  suffix = lower(suffix)
  return value:sub(-#suffix) == suffix
end

local function random_peer_name(length)
  local alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
  local out = {}
  for i = 1, length do
    local idx = math.random(1, #alphabet)
    out[i] = alphabet:sub(idx, idx)
  end
  return table.concat(out)
end

local function extract_peer_id(addr)
  local parsed = multiaddr.parse(addr)
  if not parsed then
    return nil
  end
  for i = #parsed.components, 1, -1 do
    local component = parsed.components[i]
    if component.protocol == "p2p" then
      return component.value
    end
  end
  return nil
end

local function ip4_bytes(value)
  local a, b, c, d = tostring(value or ""):match("^(%d+)%.(%d+)%.(%d+)%.(%d+)$")
  a, b, c, d = tonumber(a), tonumber(b), tonumber(c), tonumber(d)
  if not a or not b or not c or not d then
    return nil
  end
  if a > 255 or b > 255 or c > 255 or d > 255 then
    return nil
  end
  return string.char(a, b, c, d)
end

local function split_colon_groups(value)
  local out = {}
  if value == "" then
    return out
  end
  for group in value:gmatch("[^:]+") do
    out[#out + 1] = group
  end
  return out
end

local function ip6_groups(value)
  local text = tostring(value or ""):lower():gsub("%%.+$", "")
  if text == "" or not text:find(":", 1, true) then
    return nil
  end
  local first_double = text:find("::", 1, true)
  if first_double and text:find("::", first_double + 2, true) then
    return nil
  end
  if text:find("%.", 1, false) then
    local prefix, ipv4 = text:match("^(.*:)([^:]+%.%d+%.%d+%.%d+)$")
    local bytes = ip4_bytes(ipv4)
    if not prefix or not bytes then
      return nil
    end
    local a, b, c, d = bytes:byte(1, 4)
    text = prefix .. string.format("%x:%x", a * 256 + b, c * 256 + d)
  end

  local left, right = text:match("^(.-)::(.-)$")
  local groups = {}
  if left ~= nil then
    local left_groups = split_colon_groups(left)
    local right_groups = split_colon_groups(right)
    if #left_groups + #right_groups > 8 then
      return nil
    end
    for _, group in ipairs(left_groups) do
      groups[#groups + 1] = group
    end
    for _ = 1, 8 - #left_groups - #right_groups do
      groups[#groups + 1] = "0"
    end
    for _, group in ipairs(right_groups) do
      groups[#groups + 1] = group
    end
  else
    groups = split_colon_groups(text)
  end
  if #groups ~= 8 then
    return nil
  end
  local out = {}
  for i, group in ipairs(groups) do
    if group == "" or #group > 4 or not group:match("^[%x]+$") then
      return nil
    end
    local n = tonumber(group, 16)
    if not n or n < 0 or n > 0xffff then
      return nil
    end
    out[i] = n
  end
  return out
end

local function ip6_bytes(value)
  local groups = ip6_groups(value)
  if not groups then
    return nil
  end
  local out = {}
  for i, n in ipairs(groups) do
    out[(i - 1) * 2 + 1] = string.char((n >> 8) & 0xff)
    out[(i - 1) * 2 + 2] = string.char(n & 0xff)
  end
  return table.concat(out)
end

local function srv_rdata(port, target)
  local encoded_target, target_err = dns.encode_name(target)
  if not encoded_target then
    return nil, target_err
  end
  local n = tonumber(port) or 4001
  return "\0\0\0\0" .. string.char((n >> 8) & 0xff, n & 0xff) .. encoded_target
end

local function first_ip4_addr(addrs)
  for _, addr in ipairs(addrs or {}) do
    local parsed = multiaddr.parse(addr)
    if parsed then
      for _, component in ipairs(parsed.components or {}) do
        if component.protocol == "ip4" then
          local bytes = ip4_bytes(component.value)
          if bytes then
            return component.value, bytes
          end
        end
      end
    end
  end
  return nil
end

local function first_ip6_addr(addrs)
  for _, addr in ipairs(addrs or {}) do
    local parsed = multiaddr.parse(addr)
    if parsed then
      for _, component in ipairs(parsed.components or {}) do
        if component.protocol == "ip6" then
          local bytes = ip6_bytes(component.value)
          if bytes then
            return component.value, bytes
          end
        end
      end
    end
  end
  return nil
end

local function contains_unsuitable_protocol(parsed)
  for _, component in ipairs(parsed.components or {}) do
    local protocol = component.protocol
    if
      protocol == "p2p-circuit"
      or protocol == "ws"
      or protocol == "wss"
      or protocol == "webtransport"
      or protocol == "webrtc"
      or protocol == "webrtc-direct"
      or protocol == "p2p-webrtc-direct"
    then
      return true
    end
  end
  return false
end

function M.is_suitable_addr(addr, opts)
  local parsed = multiaddr.parse(addr)
  if not parsed or type(parsed.components) ~= "table" or #parsed.components < 2 then
    return false
  end
  if not extract_peer_id(addr) then
    return false
  end
  if contains_unsuitable_protocol(parsed) then
    return false
  end

  local first = parsed.components[1]
  if first.protocol == "ip4" or first.protocol == "ip6" then
    if opts and opts.allow_public_addrs then
      return true
    end
    return multiaddr.is_private_addr(parsed)
  end
  if first.protocol == "dns" or first.protocol == "dns4" or first.protocol == "dns6" or first.protocol == "dnsaddr" then
    return has_suffix(first.value, ".local")
  end
  return false
end

local function add_unique(out, seen, value)
  if type(value) == "string" and value ~= "" and not seen[value] then
    seen[value] = true
    out[#out + 1] = value
  end
end

local function host_peer_id(host)
  if not (host and type(host.peer_id) == "function") then
    return nil
  end
  local id = host:peer_id()
  return type(id) == "table" and id.id or nil
end

function M.advertise_addrs(host, opts)
  local addrs = {}
  if host and type(host.get_multiaddrs) == "function" then
    addrs = host:get_multiaddrs() or {}
  end
  local out = {}
  local seen = {}
  for _, addr in ipairs(addrs) do
    local txt = M.DNSADDR_PREFIX .. tostring(addr)
    if #txt <= 255 and M.is_suitable_addr(addr, opts) then
      add_unique(out, seen, addr)
    end
  end
  return out
end

function M.build_query(service_name)
  return dns.encode_message({
    questions = {
      { name = service_name or M.SERVICE_NAME, type = dns.TYPE_PTR, class = dns.CLASS_IN },
    },
  })
end

function M.build_response(peer_name, service_name, addrs)
  service_name = service_name or M.SERVICE_NAME
  local instance_name = peer_name .. "." .. service_name
  local target_name = peer_name .. ".local"
  local additionals = {}
  local srv, srv_err = srv_rdata(4001, target_name)
  if not srv then
    return nil, srv_err
  end
  additionals[#additionals + 1] = {
    name = instance_name,
    type = dns.TYPE_SRV,
    class = dns.CLASS_IN,
    ttl = 120,
    rdata = srv,
  }
  for _, addr in ipairs(addrs or {}) do
    local txt = M.DNSADDR_PREFIX .. addr
    if #txt <= 255 then
      additionals[#additionals + 1] = {
        name = instance_name,
        type = dns.TYPE_TXT,
        class = dns.CLASS_IN,
        ttl = 120,
        data = txt,
      }
    end
  end
  local _, ip4 = first_ip4_addr(addrs)
  if ip4 then
    additionals[#additionals + 1] = {
      name = target_name,
      type = dns.TYPE_A,
      class = dns.CLASS_IN,
      ttl = 120,
      rdata = ip4,
    }
  end
  local _, ip6 = first_ip6_addr(addrs)
  if ip6 then
    additionals[#additionals + 1] = {
      name = target_name,
      type = dns.TYPE_AAAA,
      class = dns.CLASS_IN,
      ttl = 120,
      rdata = ip6,
    }
  end
  return dns.encode_message({
    response = true,
    answers = {
      {
        name = service_name,
        type = dns.TYPE_PTR,
        class = dns.CLASS_IN,
        ttl = 120,
        data = instance_name,
      },
    },
    additionals = additionals,
  })
end

function M.build_meta_response(service_name)
  return dns.encode_message({
    response = true,
    answers = {
      {
        name = M.META_QUERY,
        type = dns.TYPE_PTR,
        class = dns.CLASS_IN,
        ttl = 120,
        data = service_name or M.SERVICE_NAME,
      },
    },
  })
end

local function records_from(message)
  local out = {}
  for _, list in ipairs({ message.answers or {}, message.additionals or {} }) do
    for _, record in ipairs(list) do
      out[#out + 1] = record
    end
  end
  return out
end

function M.parse_peers(message, opts)
  local options = opts or {}
  local service_name = options.service_name or M.SERVICE_NAME
  local local_peer_id = options.local_peer_id
  local out = {}
  local by_peer = {}
  local seen_addrs = {}

  local has_service_ptr = false
  for _, record in ipairs(message.answers or {}) do
    if record.type == dns.TYPE_PTR and lower(record.name) == lower(service_name) then
      has_service_ptr = true
      break
    end
  end
  if not has_service_ptr then
    return out
  end

  for _, record in ipairs(records_from(message)) do
    if record.type == dns.TYPE_TXT and type(record.data) == "table" then
      for _, value in ipairs(record.data) do
        if type(value) == "string" and value:sub(1, #M.DNSADDR_PREFIX) == M.DNSADDR_PREFIX then
          local addr = value:sub(#M.DNSADDR_PREFIX + 1)
          local peer_id = extract_peer_id(addr)
          if peer_id and peer_id ~= local_peer_id and multiaddr.parse(addr) then
            local entry = by_peer[peer_id]
            if not entry then
              entry = { peer_id = peer_id, addrs = {}, source = "mdns" }
              by_peer[peer_id] = entry
              out[#out + 1] = entry
            end
            seen_addrs[peer_id] = seen_addrs[peer_id] or {}
            add_unique(entry.addrs, seen_addrs[peer_id], addr)
          end
        end
      end
    end
  end
  return out
end

local function create_luv_socket(service, family)
  local ok_luv, uv = pcall(require, "luv")
  if not ok_luv then
    return nil, error_mod.new("unsupported", "mdns requires 'luv' module")
  end
  local udp, udp_err
  if family == "ip6" then
    udp, udp_err = uv.new_udp("inet6")
  else
    udp, udp_err = uv.new_udp()
  end
  if not udp then
    return nil, error_mod.new("io", "failed to create mdns udp socket", { cause = udp_err })
  end
  local bind_addr = family == "ip6" and service.bind_addr6 or service.bind_addr
  local multicast_addr = family == "ip6" and service.multicast_addr6 or service.multicast_addr
  local bind_ok, bind_err = pcall(function()
    return udp:bind(bind_addr, service.port, { reuseaddr = true })
  end)
  if not bind_ok or bind_err == nil then
    pcall(function()
      udp:close()
    end)
    return nil, error_mod.new("io", "failed to bind mdns udp socket", { cause = bind_err })
  end
  local membership_ok, membership_err = pcall(function()
    return udp:set_membership(multicast_addr, nil, "join")
  end)
  if (not membership_ok or membership_err == nil) and service.require_membership ~= false then
    pcall(function()
      udp:close()
    end)
    return nil, error_mod.new("io", "failed to join mdns multicast group", { cause = membership_err })
  end
  pcall(function()
    udp:set_multicast_loop(true)
  end)
  pcall(function()
    udp:set_multicast_ttl(255)
  end)
  local recv_ok, recv_err = pcall(function()
    return udp:recv_start(function(err, data, addr)
      if err then
        log.debug("mdns recv failed", { cause = tostring(err) })
        return
      end
      if data then
        service:_handle_packet(data, addr)
      end
    end)
  end)
  if not recv_ok or recv_err == nil then
    pcall(function()
      udp:close()
    end)
    return nil, error_mod.new("io", "failed to start mdns recv", { cause = recv_err })
  end
  return {
    family = family or "ip4",
    send = function(_, packet)
      local ok, err = pcall(function()
        return udp:send(packet, multicast_addr, service.port)
      end)
      if not ok then
        return nil, err
      end
      return true
    end,
    close = function()
      pcall(function()
        udp:recv_stop()
      end)
      pcall(function()
        udp:close()
      end)
      return true
    end,
  }
end

local function create_luv_sockets(service)
  local sockets = {}
  local errors = {}
  if service.ipv4 then
    local socket, socket_err = create_luv_socket(service, "ip4")
    if socket then
      sockets[#sockets + 1] = socket
    else
      errors[#errors + 1] = socket_err
    end
  end
  if service.ipv6 then
    local socket, socket_err = create_luv_socket(service, "ip6")
    if socket then
      sockets[#sockets + 1] = socket
    else
      log.debug("ipv6 mdns socket unavailable", { cause = tostring(socket_err) })
      errors[#errors + 1] = socket_err
    end
  end
  if #sockets == 0 then
    return nil, errors[1] or error_mod.new("io", "failed to open mdns sockets")
  end
  return {
    sockets = sockets,
    send = function(self, packet)
      local sent = false
      local last_err = nil
      for _, socket in ipairs(self.sockets) do
        local ok, err = socket:send(packet)
        if ok then
          sent = true
        else
          last_err = err
        end
      end
      if not sent then
        return nil, last_err or error_mod.new("io", "failed to send mdns packet")
      end
      return true
    end,
    close = function(self)
      for _, socket in ipairs(self.sockets) do
        if type(socket.close) == "function" then
          socket:close()
        end
      end
      return true
    end,
  }
end

function Mdns:_open()
  if self.socket then
    return true
  end
  local socket = self.socket_factory and self.socket_factory(self) or nil
  if not socket then
    local socket_err
    socket, socket_err = create_luv_sockets(self)
    if not socket then
      return nil, socket_err
    end
  end
  self.socket = socket
  if self.query_on_start ~= false then
    local queried, query_err = self:query()
    if not queried then
      self:stop()
      return nil, query_err
    end
  end
  if self.interval and self.interval > 0 and self.timer == nil then
    local ok_luv, uv = pcall(require, "luv")
    if ok_luv then
      local timer, timer_err = uv.new_timer()
      if not timer then
        self:stop()
        return nil, error_mod.new("io", "failed to create mdns query timer", { cause = timer_err })
      end
      local interval_ms = math.max(1, math.floor(self.interval * 1000))
      local ok, start_err = timer:start(interval_ms, interval_ms, function()
        self:query()
      end)
      if not ok then
        pcall(function()
          timer:close()
        end)
        self:stop()
        return nil, error_mod.new("io", "failed to start mdns query timer", { cause = start_err })
      end
      self.timer = timer
    end
  end
  return true
end

function Mdns:_send(packet)
  if not self.socket or type(self.socket.send) ~= "function" then
    return nil, error_mod.new("state", "mdns socket is not started")
  end
  return self.socket:send(packet, self.multicast_addr, self.port)
end

function Mdns:query()
  local packet, packet_err = M.build_query(self.service_name)
  if not packet then
    return nil, packet_err
  end
  return self:_send(packet)
end

function Mdns:_respond()
  if not self.broadcast then
    return true
  end
  local addrs = M.advertise_addrs(self.host, { allow_public_addrs = self.allow_public_addrs })
  if #addrs == 0 then
    return true
  end
  local packet, packet_err = M.build_response(self.peer_name, self.service_name, addrs)
  if not packet then
    return nil, packet_err
  end
  return self:_send(packet)
end

function Mdns:_respond_meta()
  local packet, packet_err = M.build_meta_response(self.service_name)
  if not packet then
    return nil, packet_err
  end
  return self:_send(packet)
end

function Mdns:_handle_packet(packet, remote)
  local message, decode_err = dns.decode_message(packet)
  if not message then
    log.debug("mdns packet decode failed", { cause = tostring(decode_err) })
    return true
  end

  for _, question in ipairs(message.questions or {}) do
    if question.type == dns.TYPE_PTR and lower(question.name) == lower(self.service_name) then
      local ok, err = self:_respond()
      if not ok then
        log.debug("mdns response failed", { cause = tostring(err) })
      end
    elseif question.type == dns.TYPE_PTR and lower(question.name) == lower(M.META_QUERY) then
      local ok, err = self:_respond_meta()
      if not ok then
        log.debug("mdns meta response failed", { cause = tostring(err) })
      end
    end
  end

  local peers = M.parse_peers(message, {
    service_name = self.service_name,
    local_peer_id = host_peer_id(self.host),
  })
  for _, peer in ipairs(peers) do
    self:_peer_found(peer, remote)
  end
  return true
end

function Mdns:_peer_found(peer, remote)
  if type(peer) ~= "table" or type(peer.peer_id) ~= "string" or #peer.addrs == 0 then
    return true
  end
  self._cache[peer.peer_id] = {
    peer_id = peer.peer_id,
    addrs = copy_list(peer.addrs),
    source = "mdns",
    last_seen = os.time(),
  }
  if self.host and self.host.peerstore and type(self.host.peerstore.merge) == "function" then
    self.host.peerstore:merge(peer.peer_id, {
      addrs = peer.addrs,
      metadata = { mdns = true },
    }, { ttl = self.peer_ttl })
  end
  if self.host and type(self.host.emit) == "function" then
    self.host:emit("peer_discovered", {
      peer_id = peer.peer_id,
      addrs = copy_list(peer.addrs),
      source = "mdns",
      remote = remote,
    })
  end
  return true
end

function Mdns:discover()
  local out = {}
  local now = os.time()
  for peer_id, peer in pairs(self._cache) do
    if not self.peer_ttl or now - peer.last_seen <= self.peer_ttl then
      out[#out + 1] = {
        peer_id = peer_id,
        addrs = copy_list(peer.addrs),
        source = "mdns",
      }
    end
  end
  table.sort(out, function(a, b)
    return a.peer_id < b.peer_id
  end)
  return out
end

function Mdns:start()
  if self.started then
    return true
  end
  self.started = true
  if not self.host or self.host._running then
    local opened, open_err = self:_open()
    if not opened then
      self.started = false
      return nil, open_err
    end
  end
  log.info("mdns discovery service started", { service_name = self.service_name })
  return true
end

function Mdns:on_host_started()
  if not self.started then
    return true
  end
  return self:_open()
end

function Mdns:stop()
  if self.timer then
    pcall(function()
      self.timer:stop()
      self.timer:close()
    end)
    self.timer = nil
  end
  if self.socket and type(self.socket.close) == "function" then
    self.socket:close()
  end
  self.socket = nil
  self.started = false
  return true
end

function Mdns:status()
  return {
    started = self.started == true,
    service_name = self.service_name,
    peer_name = self.peer_name,
    ipv4 = self.ipv4,
    ipv6 = self.ipv6,
    cached_peers = #self:discover(),
  }
end

---@param host table|nil
---@param opts? Libp2pMdnsConfig
---@return Libp2pMdnsService|nil service
---@return table|nil err
function M.new(host, opts)
  local options = opts or {}
  local peer_name = options.peer_name or random_peer_name(options.peer_name_length or 32)
  if type(peer_name) ~= "string" or peer_name == "" or #peer_name > 63 then
    return nil, error_mod.new("input", "mdns peer_name must be 1-63 bytes")
  end
  return setmetatable({
    host = host,
    service_name = options.service_name or M.SERVICE_NAME,
    peer_name = peer_name,
    multicast_addr = options.multicast_addr or M.MULTICAST_ADDR,
    multicast_addr6 = options.multicast_addr6 or M.MULTICAST_ADDR6,
    port = options.port or M.MULTICAST_PORT,
    bind_addr = options.bind_addr or "0.0.0.0",
    bind_addr6 = options.bind_addr6 or "::",
    interval = options.interval == nil and 10 or options.interval,
    peer_ttl = options.peer_ttl == nil and 120 or options.peer_ttl,
    broadcast = options.broadcast ~= false,
    allow_public_addrs = options.allow_public_addrs == true,
    ipv4 = options.ipv4 ~= false,
    ipv6 = options.ipv6 ~= false,
    require_membership = options.require_membership ~= false,
    query_on_start = options.query_on_start ~= false,
    socket_factory = options.socket_factory,
    socket = nil,
    timer = nil,
    started = false,
    _cache = {},
  }, Mdns)
end

M.Mdns = Mdns
M.dns = dns

return M
