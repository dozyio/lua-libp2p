--- PCP client.
---@class Libp2pPcpClientConfig
---@field gateway? string Gateway IP address.
---@field timeout? number UDP request timeout.
---@field retries? integer Request retry count.
---@field socket_factory? function Test/custom UDP socket factory.

---@class Libp2pPcpClient
---@field map fun(self: Libp2pPcpClient, opts: table): table|nil, table|nil
---@field close fun(self: Libp2pPcpClient): true

local socket = require("socket")

local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log").subsystem("pcp")

local M = {}

M.VERSION = 2
M.PORT = 5351

M.OP = {
  ANNOUNCE = 0,
  MAP = 1,
  PEER = 2,
}

local function u16be(n)
  local hi = math.floor(n / 256) % 256
  local lo = n % 256
  return string.char(hi, lo)
end

local function u32be(n)
  local b1 = math.floor(n / 16777216) % 256
  local b2 = math.floor(n / 65536) % 256
  local b3 = math.floor(n / 256) % 256
  local b4 = n % 256
  return string.char(b1, b2, b3, b4)
end

local function be16(data, idx)
  local a, b = data:byte(idx, idx + 1)
  return a * 256 + b
end

local function be32(data, idx)
  local a, b, c, d = data:byte(idx, idx + 3)
  return (((a * 256 + b) * 256 + c) * 256 + d)
end

local function random_bytes(n)
  local ok_sodium, sodium = pcall(require, "luasodium")
  if ok_sodium and type(sodium.randombytes_buf) == "function" then
    return sodium.randombytes_buf(n)
  end
  local out = {}
  for i = 1, n do
    out[i] = string.char(math.random(0, 255))
  end
  return table.concat(out)
end

local function ipv4_mapped(ipv4)
  local a, b, c, d = tostring(ipv4):match("^(%d+)%.(%d+)%.(%d+)%.(%d+)$")
  if not a then
    return nil
  end
  return string.rep("\0", 10) .. "\255\255" .. string.char(tonumber(a), tonumber(b), tonumber(c), tonumber(d))
end

local function is_ipv6(ip)
  return type(ip) == "string" and ip:find(":", 1, true) ~= nil
end

local function parse_ipv6(ip)
  if type(ip) ~= "string" or ip == "" then
    return nil
  end
  local value = ip
  local ipv4_tail = value:match("(%d+%.%d+%.%d+%.%d+)$")
  if ipv4_tail then
    local a, b, c, d = ipv4_tail:match("^(%d+)%.(%d+)%.(%d+)%.(%d+)$")
    if not a then
      return nil
    end
    local hi = tonumber(a) * 256 + tonumber(b)
    local lo = tonumber(c) * 256 + tonumber(d)
    value = value:gsub("%d+%.%d+%.%d+%.%d+$", string.format("%x:%x", hi, lo))
  end

  local head, tail = value:match("^(.-)::(.-)$")
  local groups = {}
  local function add_part(part)
    if part == "" then
      return true
    end
    for g in part:gmatch("[^:]+") do
      if #g > 4 or g:match("[^0-9a-fA-F]") then
        return nil
      end
      groups[#groups + 1] = tonumber(g, 16)
    end
    return true
  end

  if head then
    if not add_part(head) then
      return nil
    end
    local tail_groups = {}
    if tail ~= "" then
      for g in tail:gmatch("[^:]+") do
        if #g > 4 or g:match("[^0-9a-fA-F]") then
          return nil
        end
        tail_groups[#tail_groups + 1] = tonumber(g, 16)
      end
    end
    local missing = 8 - (#groups + #tail_groups)
    if missing < 0 then
      return nil
    end
    for _ = 1, missing do
      groups[#groups + 1] = 0
    end
    for _, g in ipairs(tail_groups) do
      groups[#groups + 1] = g
    end
  else
    if not add_part(value) then
      return nil
    end
    if #groups ~= 8 then
      return nil
    end
  end

  if #groups ~= 8 then
    return nil
  end
  local out = {}
  for _, g in ipairs(groups) do
    out[#out + 1] = string.char(math.floor(g / 256) % 256, g % 256)
  end
  return table.concat(out)
end

local function ip_to_16bytes(ip)
  if is_ipv6(ip) then
    return parse_ipv6(ip)
  end
  return ipv4_mapped(ip)
end

local function bytes16_to_ip(addr)
  if #addr ~= 16 then
    return nil
  end
  local v4mapped = true
  for i = 1, 10 do
    if addr:byte(i) ~= 0 then
      v4mapped = false
      break
    end
  end
  if v4mapped and addr:byte(11) == 255 and addr:byte(12) == 255 then
    local a, b, c, d = addr:byte(13, 16)
    return string.format("%d.%d.%d.%d", a, b, c, d)
  end
  local groups = {}
  for i = 1, 16, 2 do
    local hi, lo = addr:byte(i, i + 1)
    groups[#groups + 1] = string.format("%x", hi * 256 + lo)
  end
  return table.concat(groups, ":")
end

local Client = {}
Client.__index = Client

--- Internal PCP request/response exchange.
-- `opts.gateway`, `opts.port`, `opts.timeout`, `opts.retries`, and
-- `opts.source_ip` override client defaults.
function Client:_exchange(op, payload, lifetime, opts)
  local options = opts or {}
  local retries = self.retries or 6
  local timeout = self.timeout or 0.25
  local client_ip = options.client_ip or options.source_ip
  local client_ip_bytes = ip_to_16bytes(client_ip)
  if not client_ip_bytes then
    return nil, error_mod.new("input", "pcp client_ip/source_ip must be ipv4 or ipv6")
  end
  local request = string.char(M.VERSION, op)
    .. string.rep("\0", 2)
    .. u32be(tonumber(lifetime) or 0)
    .. client_ip_bytes
    .. payload

  local use_v6 = is_ipv6(options.gateway or self.gateway) or is_ipv6(options.source_ip)
  local udp_ctor = use_v6 and socket.udp6 or socket.udp
  if type(udp_ctor) ~= "function" then
    return nil, error_mod.new("unsupported", "pcp ipv6 socket is not supported by current luasocket build")
  end
  local udp, udp_err = udp_ctor()
  if not udp then
    return nil, error_mod.new("io", "pcp udp socket failed", { cause = udp_err })
  end
  if options.source_ip then
    local bind_ok, bind_err = udp:setsockname(options.source_ip, 0)
    if not bind_ok then
      udp:close()
      return nil, error_mod.new("io", "pcp bind failed", { source_ip = options.source_ip, cause = bind_err })
    end
  end

  log.debug("pcp request begin", {
    gateway = options.gateway or self.gateway,
    gateway_port = self.port,
    opcode = op,
    source_ip = options.source_ip,
    socket_family = use_v6 and "udp6" or "udp4",
  })

  local try = 0
  while try <= retries do
    local ok, send_err = udp:sendto(request, options.gateway or self.gateway, self.port)
    if not ok then
      udp:close()
      log.debug("pcp request failed", {
        gateway = options.gateway or self.gateway,
        gateway_port = self.port,
        opcode = op,
        stage = "send",
        attempt = try + 1,
        cause = tostring(send_err),
      })
      return nil, error_mod.new("io", "pcp send failed", { cause = send_err })
    end
    udp:settimeout(timeout)
    local data, from_ip, from_port = udp:receivefrom()
    if data then
      udp:close()
      if from_ip ~= (options.gateway or self.gateway) or tonumber(from_port) ~= tonumber(self.port) then
        return nil,
          error_mod.new("protocol", "pcp response from unexpected source", {
            from_ip = from_ip,
            from_port = from_port,
            expected_ip = options.gateway or self.gateway,
            expected_port = self.port,
          })
      end
      if #data < 24 then
        return nil, error_mod.new("protocol", "pcp response too short")
      end
      local version = data:byte(1)
      local rop = data:byte(2)
      local result = be16(data, 4)
      local epoch = be32(data, 9)
      if version ~= M.VERSION then
        return nil, error_mod.new("protocol", "pcp unsupported response version", { version = version })
      end
      if rop ~= (128 + op) then
        return nil, error_mod.new("protocol", "pcp unexpected opcode", { opcode = rop, expected = 128 + op })
      end
      if result ~= 0 then
        log.debug("pcp request failed", {
          gateway = options.gateway or self.gateway,
          gateway_port = self.port,
          opcode = op,
          stage = "response",
          result = result,
          epoch = epoch,
        })
        return nil, error_mod.new("protocol", "pcp non-success result", { result = result, epoch = epoch })
      end
      log.debug("pcp request success", {
        gateway = options.gateway or self.gateway,
        gateway_port = self.port,
        opcode = op,
        source_ip = options.source_ip,
        epoch = epoch,
      })
      return { data = data, epoch = epoch }
    end
    log.debug("pcp request retry", {
      gateway = options.gateway or self.gateway,
      gateway_port = self.port,
      opcode = op,
      source_ip = options.source_ip,
      attempt = try + 1,
      timeout = timeout,
    })
    try = try + 1
    timeout = timeout * 2
  end

  udp:close()
  log.debug("pcp request timed out", {
    gateway = options.gateway or self.gateway,
    gateway_port = self.port,
    opcode = op,
    attempts = retries + 1,
  })
  return nil,
    error_mod.new("timeout", "pcp request timed out", { gateway = options.gateway or self.gateway, opcode = op })
end

--- Request PCP MAP mapping.
-- `opts.gateway` overrides configured gateway.
-- `opts.source_ip` pins local source address for UDP socket binding.
--- protocol string `tcp|udp`.
--- internal_port number
--- suggested_external_port? number
--- lifetime? number
--- suggested_external_ip? string
--- opts? table
--- table|nil mapping
--- table|nil err
function Client:map_port(protocol, internal_port, suggested_external_port, lifetime, suggested_external_ip, opts)
  local proto_num
  if protocol == "tcp" then
    proto_num = 6
  elseif protocol == "udp" then
    proto_num = 17
  else
    return nil, error_mod.new("input", "pcp protocol must be tcp or udp")
  end

  local iport = tonumber(internal_port)
  local eport = tonumber(suggested_external_port or internal_port)
  local ttl = tonumber(lifetime or 7200)
  if not iport or iport < 1 or iport > 65535 then
    return nil, error_mod.new("input", "pcp invalid internal port")
  end

  local nonce = random_bytes(12)
  local ext_ip_bytes = ip_to_16bytes(suggested_external_ip or "0.0.0.0")
  if not ext_ip_bytes then
    return nil, error_mod.new("input", "pcp suggested_external_ip must be ipv4 or ipv6")
  end

  local payload = nonce .. string.char(proto_num) .. string.rep("\0", 3) .. u16be(iport) .. u16be(eport) .. ext_ip_bytes

  log.debug("pcp map request started", {
    gateway = opts and opts.gateway or self.gateway,
    protocol = protocol,
    internal_port = iport,
    suggested_external_port = eport,
    lifetime = ttl,
    suggested_external_ip = suggested_external_ip,
  })
  local resp, err = self:_exchange(M.OP.MAP, payload, ttl, opts)
  if not resp then
    return nil, err
  end
  local data = resp.data
  if #data < 60 then
    return nil, error_mod.new("protocol", "pcp map response too short")
  end
  local out_nonce = data:sub(25, 36)
  if out_nonce ~= nonce then
    return nil, error_mod.new("protocol", "pcp nonce mismatch")
  end
  local assigned_external_port = be16(data, 43)
  local addr = data:sub(45, 60)

  local mapping = {
    protocol = protocol,
    internal_port = iport,
    external_port = assigned_external_port,
    lifetime = be32(data, 5),
    epoch = resp.epoch,
    external_ip = bytes16_to_ip(addr),
  }
  log.debug("pcp map request completed", {
    gateway = opts and opts.gateway or self.gateway,
    protocol = mapping.protocol,
    internal_port = mapping.internal_port,
    external_port = mapping.external_port,
    external_ip = mapping.external_ip,
    lifetime = mapping.lifetime,
    epoch = mapping.epoch,
  })
  return mapping
end

function Client:unmap_port(protocol, internal_port, suggested_external_port)
  return self:map_port(protocol, internal_port, suggested_external_port, 0)
end

--- Construct a PCP client instance.
-- `opts` includes `gateway` (required), `port`, `timeout`, and `retries`.
---@param opts? Libp2pPcpClientConfig
---@return Libp2pPcpClient client
function M.new(opts)
  local options = opts or {}
  local gateway = options.gateway
  if type(gateway) ~= "string" or gateway == "" then
    return nil, error_mod.new("input", "pcp gateway is required (e.g. 192.168.1.254)")
  end
  return setmetatable({
    gateway = gateway,
    port = options.port or M.PORT,
    timeout = options.timeout or 0.25,
    retries = options.retries == nil and 6 or options.retries,
  }, Client)
end

M.Client = Client

return M
