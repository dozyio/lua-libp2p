local socket = require("socket")

local error_mod = require("lua_libp2p.error")

local M = {}

M.PORT = 5351
M.VERSION = 0

M.OP = {
  EXTERNAL_ADDRESS = 0,
  MAP_UDP = 1,
  MAP_TCP = 2,
}

M.RESULT = {
  SUCCESS = 0,
  UNSUPPORTED_VERSION = 1,
  NOT_AUTHORIZED = 2,
  NETWORK_FAILURE = 3,
  OUT_OF_RESOURCES = 4,
  UNSUPPORTED_OPCODE = 5,
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

local function recv_exact(udp, timeout)
  udp:settimeout(timeout)
  local data, ip, port = udp:receivefrom()
  if not data then
    return nil, nil, nil
  end
  return data, ip, port
end

local function op_from_protocol(protocol)
  if protocol == "udp" then
    return M.OP.MAP_UDP
  end
  if protocol == "tcp" then
    return M.OP.MAP_TCP
  end
  return nil
end

local Client = {}
Client.__index = Client

function Client:_exchange(op, payload)
  local retries = self.retries or 6
  local timeout = self.timeout or 0.25

  local request = string.char(M.VERSION, op) .. payload

  local udp, udp_err = socket.udp()
  if not udp then
    return nil, error_mod.new("io", "nat-pmp udp socket failed", { cause = udp_err })
  end

  local try = 0
  while try <= retries do
    local ok, send_err = udp:sendto(request, self.gateway, self.port)
    if not ok then
      udp:close()
      return nil, error_mod.new("io", "nat-pmp send failed", { cause = send_err })
    end

    local data = recv_exact(udp, timeout)
    if data then
      udp:close()
      if #data < 8 then
        return nil, error_mod.new("protocol", "nat-pmp response too short")
      end
      local version = data:byte(1)
      local rop = data:byte(2)
      local result = be16(data, 3)
      local epoch = be32(data, 5)
      if version ~= M.VERSION then
        return nil, error_mod.new("protocol", "nat-pmp unsupported response version", { version = version })
      end
      if rop ~= (128 + op) then
        return nil, error_mod.new("protocol", "nat-pmp unexpected opcode", { opcode = rop, expected = 128 + op })
      end
      if result ~= M.RESULT.SUCCESS then
        return nil, error_mod.new("protocol", "nat-pmp non-success result", {
          result = result,
          epoch = epoch,
        })
      end
      return {
        data = data,
        epoch = epoch,
      }
    end

    try = try + 1
    timeout = timeout * 2
  end

  udp:close()
  return nil, error_mod.new("timeout", "nat-pmp request timed out", {
    gateway = self.gateway,
    opcode = op,
  })
end

function Client:get_external_address()
  local resp, err = self:_exchange(M.OP.EXTERNAL_ADDRESS, "")
  if not resp then
    return nil, err
  end
  local data = resp.data
  if #data < 12 then
    return nil, error_mod.new("protocol", "nat-pmp external address response too short")
  end
  local a, b, c, d = data:byte(9, 12)
  return string.format("%d.%d.%d.%d", a, b, c, d), {
    epoch = resp.epoch,
  }
end

function Client:map_port(protocol, internal_port, external_port, lifetime)
  local op = op_from_protocol(protocol)
  if not op then
    return nil, error_mod.new("input", "nat-pmp protocol must be tcp or udp")
  end
  local iport = tonumber(internal_port)
  local eport = tonumber(external_port or internal_port)
  local ttl = tonumber(lifetime or 7200)
  if not iport or iport < 1 or iport > 65535 then
    return nil, error_mod.new("input", "nat-pmp invalid internal port")
  end
  if not eport or eport < 0 or eport > 65535 then
    return nil, error_mod.new("input", "nat-pmp invalid external port")
  end
  if not ttl or ttl < 0 then
    return nil, error_mod.new("input", "nat-pmp invalid lifetime")
  end

  local payload = u16be(0) .. u16be(iport) .. u16be(eport) .. u32be(ttl)
  local resp, err = self:_exchange(op, payload)
  if not resp then
    return nil, err
  end
  local data = resp.data
  if #data < 16 then
    return nil, error_mod.new("protocol", "nat-pmp map response too short")
  end

  return {
    protocol = protocol,
    internal_port = be16(data, 9),
    external_port = be16(data, 11),
    lifetime = be32(data, 13),
    epoch = resp.epoch,
  }
end

function Client:unmap_port(protocol, internal_port, external_port)
  return self:map_port(protocol, internal_port, external_port, 0)
end

function M.new(opts)
  local options = opts or {}
  local gateway = options.gateway
  if type(gateway) ~= "string" or gateway == "" then
    return nil, error_mod.new("input", "nat-pmp gateway is required (e.g. 192.168.1.254)")
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
