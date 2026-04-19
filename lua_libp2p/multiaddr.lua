local error_mod = require("lua_libp2p.error")
local peerid = require("lua_libp2p.peerid")

local M = {}

local function is_decimal_integer(text)
  if type(text) ~= "string" or text == "" then
    return false
  end
  if not text:match("^%d+$") then
    return false
  end
  if #text > 1 and text:sub(1, 1) == "0" then
    return false
  end
  return true
end

local function validate_ip4(value)
  local a, b, c, d = value:match("^(%d+)%.(%d+)%.(%d+)%.(%d+)$")
  if not a then
    return nil, "invalid ip4 address"
  end
  local parts = { tonumber(a), tonumber(b), tonumber(c), tonumber(d) }
  for i = 1, 4 do
    if parts[i] < 0 or parts[i] > 255 then
      return nil, "invalid ip4 address"
    end
  end
  return true
end

local function validate_ip6(value)
  if not value:match(":") then
    return nil, "invalid ip6 address"
  end
  if not value:match("^[%x:%.]+$") then
    return nil, "invalid ip6 address"
  end
  return true
end

local function validate_dns(value)
  if value == "" or #value > 253 then
    return nil, "invalid dns name"
  end
  if value:sub(1, 1) == "." or value:sub(-1) == "." then
    return nil, "invalid dns name"
  end
  if value:find("%.%.") then
    return nil, "invalid dns name"
  end
  for label in value:gmatch("[^%.]+") do
    if #label > 63 then
      return nil, "invalid dns name"
    end
    if label:sub(1, 1) == "-" or label:sub(-1) == "-" then
      return nil, "invalid dns name"
    end
    if not label:match("^[%w-]+$") then
      return nil, "invalid dns name"
    end
  end
  return true
end

local function validate_port(value)
  if not is_decimal_integer(value) then
    return nil, "invalid tcp/udp port"
  end
  local port = tonumber(value)
  if port < 0 or port > 65535 then
    return nil, "invalid tcp/udp port"
  end
  return true
end

local function validate_non_empty(value)
  if value == "" then
    return nil, "value must not be empty"
  end
  return true
end

local function validate_peer_id(value)
  if value == "" then
    return nil, "peer id value must not be empty"
  end
  local parsed = peerid.parse(value)
  if not parsed then
    return nil, "invalid peer id"
  end
  return true
end

local PROTOCOLS = {
  ip4 = { has_value = true, validate = validate_ip4 },
  ip6 = { has_value = true, validate = validate_ip6 },
  dns = { has_value = true, validate = validate_dns },
  dns4 = { has_value = true, validate = validate_dns },
  dns6 = { has_value = true, validate = validate_dns },
  dnsaddr = { has_value = true, validate = validate_dns },
  tcp = { has_value = true, validate = validate_port },
  udp = { has_value = true, validate = validate_port },
  p2p = { has_value = true, validate = validate_peer_id },
  ipfs = { has_value = true, validate = validate_peer_id },
  unix = { has_value = true, path = true, validate = validate_non_empty },
  sni = { has_value = true, validate = validate_dns },
  http = { has_value = false },
  ["http-path"] = { has_value = true, validate = validate_non_empty },
  https = { has_value = false },
  ws = { has_value = false },
  wss = { has_value = false },
  tls = { has_value = false },
  noise = { has_value = false },
  utp = { has_value = false },
  udt = { has_value = false },
  quic = { has_value = false },
  ["quic-v1"] = { has_value = false },
  webtransport = { has_value = false },
  webrtc = { has_value = false },
  ["p2p-webrtc-direct"] = { has_value = false },
  ["p2p-webrtc-star"] = { has_value = false },
  ["p2p-websocket-star"] = { has_value = false },
  ["p2p-stardust"] = { has_value = false },
  ["webrtc-direct"] = { has_value = false },
  plaintextv2 = { has_value = false },
  ["p2p-circuit"] = { has_value = false },
  memory = { has_value = true, validate = validate_non_empty },
}

local function split_path(path)
  local out = {}
  for token in path:gmatch("[^/]+") do
    out[#out + 1] = token
  end
  return out
end

local function normalize_text(text)
  local normalized = text:gsub("//+", "/")
  if normalized ~= "/" and normalized:sub(-1) == "/" then
    normalized = normalized:sub(1, -2)
  end
  return normalized
end

function M.parse(text)
  if type(text) ~= "string" then
    return nil, error_mod.new("input", "multiaddr must be a string")
  end

  local normalized = normalize_text(text)
  if normalized == "" or normalized == "/" then
    return nil, error_mod.new("input", "empty multiaddr is invalid")
  end

  if normalized:sub(1, 1) ~= "/" then
    return nil, error_mod.new("input", "multiaddr must begin with '/'")
  end

  local parts = split_path(normalized)
  local components = {}
  local i = 1
  while i <= #parts do
    local protocol = parts[i]
    if protocol == "ipfs" then
      protocol = "p2p"
    end
    local spec = PROTOCOLS[protocol]
    if not spec then
      return nil, error_mod.new("input", "unsupported multiaddr protocol", { protocol = protocol })
    end

    local value
    if spec.has_value then
      i = i + 1
      if i > #parts then
        return nil, error_mod.new("input", "missing protocol value", { protocol = protocol })
      end
      if spec.path then
        value = table.concat(parts, "/", i)
        i = #parts
      else
        value = parts[i]
      end

      if spec.validate then
        local ok, reason = spec.validate(value)
        if not ok then
          return nil, error_mod.new("input", "invalid protocol value", {
            protocol = protocol,
            value = value,
            reason = reason,
          })
        end
      end
    end

    components[#components + 1] = {
      protocol = protocol,
      value = value,
    }
    i = i + 1
  end

  return {
    text = normalized,
    components = components,
  }
end

function M.format(addr)
  if type(addr) ~= "table" or type(addr.components) ~= "table" then
    return nil, error_mod.new("input", "multiaddr object must contain components")
  end
  if #addr.components == 0 then
    return nil, error_mod.new("input", "empty multiaddr is invalid")
  end

  local out = {}
  for _, component in ipairs(addr.components) do
    local protocol = component.protocol
    local spec = PROTOCOLS[protocol]
    if not spec then
      return nil, error_mod.new("input", "unsupported multiaddr protocol", { protocol = protocol })
    end
    out[#out + 1] = protocol
    if spec.has_value then
      if type(component.value) ~= "string" then
        return nil, error_mod.new("input", "missing protocol value", { protocol = protocol })
      end
      if spec.validate then
        local ok, reason = spec.validate(component.value)
        if not ok then
          return nil, error_mod.new("input", "invalid protocol value", {
            protocol = protocol,
            value = component.value,
            reason = reason,
          })
        end
      end
      out[#out + 1] = component.value
    end
  end

  return "/" .. table.concat(out, "/")
end

function M.encapsulate(base, extra)
  local left, left_err
  if type(base) == "string" then
    left, left_err = M.parse(base)
  else
    left = base
  end
  if not left then
    return nil, left_err or error_mod.new("input", "invalid base multiaddr")
  end
  local right, right_err
  if type(extra) == "string" then
    right, right_err = M.parse(extra)
  else
    right = extra
  end
  if not right then
    return nil, right_err or error_mod.new("input", "invalid encapsulated multiaddr")
  end

  local components = {}
  for _, c in ipairs(left.components) do
    components[#components + 1] = { protocol = c.protocol, value = c.value }
  end
  for _, c in ipairs(right.components) do
    components[#components + 1] = { protocol = c.protocol, value = c.value }
  end

  return M.format({ components = components })
end

function M.decapsulate(base, suffix)
  local left, left_err = M.parse(base)
  if not left then
    return nil, left_err
  end
  local right, right_err = M.parse(suffix)
  if not right then
    return nil, right_err
  end

  if #right.components > #left.components then
    return nil, error_mod.new("input", "suffix not found in multiaddr")
  end

  local offset = #left.components - #right.components
  for i = 1, #right.components do
    local a = left.components[offset + i]
    local b = right.components[i]
    if a.protocol ~= b.protocol or a.value ~= b.value then
      return nil, error_mod.new("input", "suffix not found in multiaddr")
    end
  end

  local kept = {}
  for i = 1, offset do
    local c = left.components[i]
    kept[#kept + 1] = { protocol = c.protocol, value = c.value }
  end

  return M.format({ components = kept })
end

function M.to_tcp_endpoint(input)
  local addr = input
  if type(addr) == "string" then
    local parsed, parse_err = M.parse(addr)
    if not parsed then
      return nil, parse_err
    end
    addr = parsed
  end

  if type(addr) ~= "table" or type(addr.components) ~= "table" then
    return nil, error_mod.new("input", "invalid multiaddr object")
  end
  if #addr.components < 2 then
    return nil, error_mod.new("input", "multiaddr must include host and tcp")
  end

  local host_part = addr.components[1]
  local tcp_part = addr.components[2]
  if tcp_part.protocol ~= "tcp" then
    return nil, error_mod.new("input", "multiaddr must include /tcp as second component")
  end

  if host_part.protocol ~= "ip4" and host_part.protocol ~= "dns" and host_part.protocol ~= "dns4" and host_part.protocol ~= "dns6" then
    return nil, error_mod.new("input", "unsupported tcp host protocol", { protocol = host_part.protocol })
  end

  local port = tonumber(tcp_part.value)
  if not port then
    return nil, error_mod.new("input", "invalid tcp port", { value = tcp_part.value })
  end

  return {
    host = host_part.value,
    host_protocol = host_part.protocol,
    port = port,
  }
end

M.PROTOCOLS = PROTOCOLS

return M
