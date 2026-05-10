--- Multiaddr parsing, formatting, and helpers.
-- @module lua_libp2p.multiaddr
local error_mod = require("lua_libp2p.error")
local peerid = require("lua_libp2p.peerid")
local varint = require("lua_libp2p.multiformats.varint")

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

local function parse_ip4_parts(value)
  local a, b, c, d = tostring(value or ""):match("^(%d+)%.(%d+)%.(%d+)%.(%d+)$")
  if not a then
    return nil
  end
  local parts = { tonumber(a), tonumber(b), tonumber(c), tonumber(d) }
  for i = 1, 4 do
    if not parts[i] or parts[i] < 0 or parts[i] > 255 then
      return nil
    end
  end
  return parts
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

local function parse_ip6_groups(value)
  local text = tostring(value or ""):lower():gsub("%%.+$", "")
  if text == "" or not text:find(":", 1, true) then
    return nil
  end
  if text:find("::", 1, true) and text:find("::", text:find("::", 1, true) + 2, true) then
    return nil
  end

  if text:find("%.", 1, false) then
    local prefix, ipv4 = text:match("^(.*:)([^:]+%.%d+%.%d+%.%d+)$")
    if not prefix then
      return nil
    end
    local parts = parse_ip4_parts(ipv4)
    if not parts then
      return nil
    end
    text = prefix .. string.format("%x:%x", parts[1] * 256 + parts[2], parts[3] * 256 + parts[4])
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
    if #groups ~= 8 then
      return nil
    end
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

local function validate_ip6(value)
  if not parse_ip6_groups(value) then
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

local PROTOCOL_CODES = {
  ip4 = 4,
  tcp = 6,
  dns = 53,
  dns4 = 54,
  dns6 = 55,
  dnsaddr = 56,
  udp = 273,
  ip6 = 41,
  p2p = 421,
  http = 480,
  ["http-path"] = 481,
  https = 443,
  tls = 448,
  sni = 449,
  noise = 454,
  ws = 477,
  wss = 478,
  plaintextv2 = 7367777,
  ["webrtc-direct"] = 280,
  webrtc = 281,
  ["p2p-circuit"] = 290,
  utp = 302,
  udt = 301,
  quic = 460,
  ["quic-v1"] = 461,
  webtransport = 465,
  ["p2p-webrtc-direct"] = 276,
  ["p2p-webrtc-star"] = 275,
  ["p2p-websocket-star"] = 479,
  ["p2p-stardust"] = 277,
  unix = 400,
  memory = 777,
}

local CODE_TO_PROTOCOL = {}
for name, code in pairs(PROTOCOL_CODES) do
  CODE_TO_PROTOCOL[code] = name
end

local ENCODING_KIND = {
  ip4 = "ip4",
  ip6 = "ip6",
  tcp = "port",
  udp = "port",
  dns = "string",
  dns4 = "string",
  dns6 = "string",
  dnsaddr = "string",
  sni = "string",
  p2p = "peerid",
  unix = "string",
  ["http-path"] = "string",
  memory = "string",
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

--- Parse a multiaddr string into components.
-- @tparam string text Multiaddr text.
-- @treturn table|nil parsed
-- @treturn[opt] table err
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
          return nil,
            error_mod.new("input", "invalid protocol value", {
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

--- Format a parsed multiaddr back to text.
-- @tparam table|string addr Parsed object or address string.
-- @treturn string|nil text
-- @treturn[opt] table err
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
          return nil,
            error_mod.new("input", "invalid protocol value", {
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

--- Encapsulate one multiaddr inside another.
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

--- Remove a trailing suffix multiaddr.
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

local function parse_input(input)
  if type(input) == "string" then
    return M.parse(input)
  end
  if type(input) == "table" and type(input.components) == "table" then
    return input
  end
  return nil, error_mod.new("input", "invalid multiaddr object")
end

local function copy_components(components, first, last)
  local out = {}
  for i = first or 1, last or #components do
    local c = components[i]
    out[#out + 1] = { protocol = c.protocol, value = c.value }
  end
  return out
end

--- Extract relay components from a relay destination address.
function M.relay_info(input)
  local addr, parse_err = parse_input(input)
  if not addr then
    return nil, parse_err
  end

  local circuit_index = nil
  for i, component in ipairs(addr.components) do
    if component.protocol == "p2p-circuit" then
      circuit_index = i
      break
    end
  end
  if not circuit_index then
    return nil, error_mod.new("input", "multiaddr does not include /p2p-circuit")
  end

  local relay_peer_index = nil
  for i = circuit_index - 1, 1, -1 do
    if addr.components[i].protocol == "p2p" then
      relay_peer_index = i
      break
    end
  end
  if not relay_peer_index then
    return nil, error_mod.new("input", "relay multiaddr must include relay /p2p peer id before /p2p-circuit")
  end

  local destination_peer_id = nil
  if addr.components[circuit_index + 1] and addr.components[circuit_index + 1].protocol == "p2p" then
    destination_peer_id = addr.components[circuit_index + 1].value
  end

  local relay_addr, relay_addr_err = M.format({ components = copy_components(addr.components, 1, relay_peer_index) })
  if not relay_addr then
    return nil, relay_addr_err
  end
  local circuit_addr, circuit_addr_err = M.format({ components = copy_components(addr.components, 1, circuit_index) })
  if not circuit_addr then
    return nil, circuit_addr_err
  end

  return {
    relay_peer_id = addr.components[relay_peer_index].value,
    relay_addr = relay_addr,
    circuit_addr = circuit_addr,
    destination_peer_id = destination_peer_id,
    circuit_index = circuit_index,
  }
end

--- Check whether an address is a relay (`/p2p-circuit`) address.
function M.is_relay_addr(input)
  return M.relay_info(input) ~= nil
end

--- Build a relay reservation address from a relay peer address.
function M.relay_reservation_addr(relay_addr)
  local info = M.relay_info(relay_addr)
  if info then
    return info.circuit_addr
  end
  return M.encapsulate(relay_addr, "/p2p-circuit")
end

--- Build a relay destination address for a target peer.
function M.relay_destination_addr(relay_addr, destination_peer_id)
  if type(destination_peer_id) ~= "string" or destination_peer_id == "" then
    return nil, error_mod.new("input", "destination peer id must be non-empty")
  end
  local ok, reason = validate_peer_id(destination_peer_id)
  if not ok then
    return nil, error_mod.new("input", "invalid destination peer id", { reason = reason })
  end
  local reservation, reservation_err = M.relay_reservation_addr(relay_addr)
  if not reservation then
    return nil, reservation_err
  end
  local info = M.relay_info(reservation)
  if info and info.destination_peer_id then
    reservation = info.circuit_addr
  end
  return M.encapsulate(reservation, "/p2p/" .. destination_peer_id)
end

--- Convert multiaddr into `{ host, port }` TCP endpoint.
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
  if
    host_part.protocol ~= "ip4"
    and host_part.protocol ~= "ip6"
    and host_part.protocol ~= "dns"
    and host_part.protocol ~= "dns4"
    and host_part.protocol ~= "dns6"
  then
    return nil, error_mod.new("input", "unsupported tcp host protocol", { protocol = host_part.protocol })
  end

  local tcp_part = nil
  for i = 2, #addr.components do
    local component = addr.components[i]
    if component.protocol == "tcp" then
      tcp_part = component
      break
    end
    if
      component.protocol == "udp"
      or component.protocol == "quic"
      or component.protocol == "quic-v1"
      or component.protocol == "ws"
      or component.protocol == "wss"
    then
      return nil, error_mod.new("input", "unsupported tcp multiaddr transport", { protocol = component.protocol })
    end
  end
  if not tcp_part then
    return nil, error_mod.new("input", "multiaddr must include /tcp")
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

local function first_host_component(input)
  local addr = input
  if type(addr) == "string" then
    local parsed = M.parse(addr)
    if not parsed then
      return nil
    end
    addr = parsed
  end
  if type(addr) ~= "table" or type(addr.components) ~= "table" then
    return nil
  end
  for _, component in ipairs(addr.components) do
    if
      component.protocol == "ip4"
      or component.protocol == "ip6"
      or component.protocol == "dns"
      or component.protocol == "dns4"
      or component.protocol == "dns6"
    then
      return component
    end
  end
  return nil
end

--- Return true when address is private/loopback/link-local.
function M.is_private_addr(input)
  local host = first_host_component(input)
  if not host then
    return false
  end
  if host.protocol == "ip4" then
    local parts = parse_ip4_parts(host.value)
    if not parts then
      return false
    end
    local a, b = parts[1], parts[2]
    if a == 10 or a == 127 or a == 0 or a >= 224 then
      return true
    end
    if a == 172 and b >= 16 and b <= 31 then
      return true
    end
    if a == 192 and b == 168 then
      return true
    end
    if a == 169 and b == 254 then
      return true
    end
    if a == 100 and b >= 64 and b <= 127 then
      return true
    end
    return false
  end
  if host.protocol == "ip6" then
    local value = string.lower(tostring(host.value or ""))
    if value == "::" or value == "::1" then
      return true
    end
    local groups = parse_ip6_groups(value)
    if groups then
      local all_zero = true
      for i = 1, 8 do
        if groups[i] ~= 0 then
          all_zero = false
          break
        end
      end
      if all_zero then
        return true
      end
      local loopback = true
      for i = 1, 7 do
        if groups[i] ~= 0 then
          loopback = false
          break
        end
      end
      if loopback and groups[8] == 1 then
        return true
      end
    end
    return value:match("^fc") ~= nil
      or value:match("^fd") ~= nil
      or value:match("^fe8") ~= nil
      or value:match("^fe9") ~= nil
      or value:match("^fea") ~= nil
      or value:match("^feb") ~= nil
  end
  local dns_name = string.lower(tostring(host.value or ""))
  return dns_name == "localhost" or dns_name:match("%.localhost$") ~= nil or dns_name:match("%.local$") ~= nil
end

--- Return true when address is considered public-routable.
function M.is_public_addr(input)
  local host = first_host_component(input)
  if not host then
    return false
  end
  return not M.is_private_addr(input)
end

local function ip4_to_bytes(ip4)
  local a, b, c, d = ip4:match("^(%d+)%.(%d+)%.(%d+)%.(%d+)$")
  return string.char(tonumber(a), tonumber(b), tonumber(c), tonumber(d))
end

local function ip4_from_bytes(bytes)
  if #bytes ~= 4 then
    return nil, error_mod.new("decode", "ip4 byte length must be 4")
  end
  return string.format("%d.%d.%d.%d", bytes:byte(1), bytes:byte(2), bytes:byte(3), bytes:byte(4))
end

local function ip6_to_bytes(value)
  local groups = parse_ip6_groups(value)
  if not groups then
    return nil, error_mod.new("input", "invalid ip6 address")
  end
  local out = {}
  for _, group in ipairs(groups) do
    out[#out + 1] = string.char(math.floor(group / 256), group % 256)
  end
  return table.concat(out)
end

local function ip6_from_bytes(bytes)
  if #bytes ~= 16 then
    return nil, error_mod.new("decode", "ip6 byte length must be 16")
  end
  local groups = {}
  for i = 1, 16, 2 do
    local a, b = bytes:byte(i), bytes:byte(i + 1)
    groups[#groups + 1] = string.format("%x", a * 256 + b)
  end
  return table.concat(groups, ":")
end

local function port_to_bytes(text)
  local n = tonumber(text)
  return string.char(math.floor(n / 256), n % 256)
end

local function port_from_bytes(bytes)
  if #bytes ~= 2 then
    return nil, error_mod.new("decode", "port byte length must be 2")
  end
  local a, b = bytes:byte(1), bytes:byte(2)
  return tostring(a * 256 + b)
end

local function encode_value(protocol, value)
  local kind = ENCODING_KIND[protocol]
  if kind == nil then
    return ""
  end
  if kind == "ip4" then
    return ip4_to_bytes(value)
  end
  if kind == "ip6" then
    return ip6_to_bytes(value)
  end
  if kind == "port" then
    return port_to_bytes(value)
  end
  if kind == "peerid" then
    local pid, pid_err = peerid.parse(value)
    if not pid then
      return nil, pid_err
    end
    local len, len_err = varint.encode_u64(#pid.bytes)
    if not len then
      return nil, len_err
    end
    return len .. pid.bytes
  end
  if kind == "string" then
    local len, len_err = varint.encode_u64(#value)
    if not len then
      return nil, len_err
    end
    return len .. value
  end
  return nil, error_mod.new("unsupported", "unsupported value encoding kind", { kind = kind })
end

local function decode_value(protocol, bytes, offset)
  local kind = ENCODING_KIND[protocol]
  if kind == nil then
    return nil, offset
  end
  local i = offset

  if kind == "ip4" then
    local finish = i + 3
    if finish > #bytes then
      return nil, nil, error_mod.new("decode", "truncated ip4 value")
    end
    local value, err = ip4_from_bytes(bytes:sub(i, finish))
    if not value then
      return nil, nil, err
    end
    return value, finish + 1
  end
  if kind == "ip6" then
    local finish = i + 15
    if finish > #bytes then
      return nil, nil, error_mod.new("decode", "truncated ip6 value")
    end
    local value, err = ip6_from_bytes(bytes:sub(i, finish))
    if not value then
      return nil, nil, err
    end
    return value, finish + 1
  end
  if kind == "port" then
    local finish = i + 1
    if finish > #bytes then
      return nil, nil, error_mod.new("decode", "truncated port value")
    end
    local value, err = port_from_bytes(bytes:sub(i, finish))
    if not value then
      return nil, nil, err
    end
    return value, finish + 1
  end

  local length, after_len_or_err = varint.decode_u64(bytes, i)
  if not length then
    return nil, nil, after_len_or_err
  end
  local finish = after_len_or_err + length - 1
  if finish > #bytes then
    return nil, nil, error_mod.new("decode", "truncated variable-size value")
  end
  local raw = bytes:sub(after_len_or_err, finish)

  if kind == "peerid" then
    return peerid.to_base58(raw), finish + 1
  end
  return raw, finish + 1
end

--- Encode multiaddr text to binary bytes.
function M.to_bytes(input)
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

  local parts = {}
  for _, component in ipairs(addr.components) do
    local protocol = component.protocol
    local code = PROTOCOL_CODES[protocol]
    if not code then
      return nil, error_mod.new("unsupported", "binary encoding unsupported protocol", { protocol = protocol })
    end

    local vcode, vcode_err = varint.encode_u64(code)
    if not vcode then
      return nil, vcode_err
    end
    parts[#parts + 1] = vcode

    local spec = PROTOCOLS[protocol]
    if spec and spec.has_value then
      local value_bytes, value_err = encode_value(protocol, component.value)
      if not value_bytes then
        return nil, value_err
      end
      parts[#parts + 1] = value_bytes
    end
  end

  return table.concat(parts)
end

--- Decode binary multiaddr bytes to text and components.
function M.from_bytes(bytes)
  if type(bytes) ~= "string" or bytes == "" then
    return nil, error_mod.new("input", "multiaddr bytes must be non-empty")
  end

  local components = {}
  local i = 1
  while i <= #bytes do
    local code, next_i_or_err = varint.decode_u64(bytes, i)
    if not code then
      return nil, next_i_or_err
    end

    local protocol = CODE_TO_PROTOCOL[code]
    if not protocol then
      return nil, error_mod.new("unsupported", "unsupported multiaddr protocol code", { code = code })
    end

    i = next_i_or_err
    local spec = PROTOCOLS[protocol]
    local value
    if spec and spec.has_value then
      local err
      value, i, err = decode_value(protocol, bytes, i)
      if not value then
        return nil, err
      end
    end

    components[#components + 1] = {
      protocol = protocol,
      value = value,
    }
  end

  local text, format_err = M.format({ components = components })
  if not text then
    return nil, format_err
  end

  return {
    text = text,
    components = components,
  }
end

M.PROTOCOLS = PROTOCOLS

return M
