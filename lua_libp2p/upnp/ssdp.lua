local socket = require("socket")

local error_mod = require("lua_libp2p.error")

local M = {}

M.MULTICAST_ADDR = "239.255.255.250"
M.MULTICAST_PORT = 1900
M.SEARCH_TARGETS = {
  "urn:schemas-upnp-org:device:InternetGatewayDevice:2",
  "urn:schemas-upnp-org:device:InternetGatewayDevice:1",
  "urn:schemas-upnp-org:service:WANIPConnection:2",
  "urn:schemas-upnp-org:service:WANIPConnection:1",
  "urn:schemas-upnp-org:service:WANPPPConnection:1",
}

local function trim(value)
  return (tostring(value or ""):gsub("^%s+", ""):gsub("%s+$", ""))
end

function M.parse_response(data)
  if type(data) ~= "string" or data == "" then
    return nil, error_mod.new("decode", "empty SSDP response")
  end
  local headers = {}
  local status = data:match("^([^\r\n]+)") or ""
  for line in data:gmatch("[^\r\n]+") do
    local key, value = line:match("^([^:]+):%s*(.*)$")
    if key then
      headers[string.lower(trim(key))] = trim(value)
    end
  end
  return {
    status = status,
    headers = headers,
    location = headers.location,
    st = headers.st,
    usn = headers.usn,
  }
end

function M.build_search_request(st, opts)
  local options = opts or {}
  local mx = options.mx or 2
  local host = options.host or (M.MULTICAST_ADDR .. ":" .. M.MULTICAST_PORT)
  return table.concat({
    "M-SEARCH * HTTP/1.1",
    "HOST: " .. host,
    "MAN: \"ssdp:discover\"",
    "MX: " .. tostring(mx),
    "ST: " .. st,
    "",
    "",
  }, "\r\n")
end

local function collect_responses(udp, timeout, out, seen)
  local deadline = socket.gettime() + timeout
  while socket.gettime() < deadline do
    udp:settimeout(math.max(deadline - socket.gettime(), 0.05))
    local data = udp:receive()
    if data then
      local response = M.parse_response(data)
      if response and response.location and not seen[response.location] then
        seen[response.location] = true
        out[#out + 1] = response
      end
    end
  end
end

function M.discover(opts)
  local options = opts or {}
  local timeout = options.timeout or 10
  local targets = options.search_targets or M.SEARCH_TARGETS
  local out = {}
  local seen = {}

  local udp, udp_err = socket.udp()
  if not udp then
    return nil, error_mod.new("io", "failed to create SSDP UDP socket", { cause = udp_err })
  end
  udp:settimeout(timeout)
  if options.interface then
    pcall(function()
      udp:setsockname(options.interface, 0)
    end)
  end

  for _, st in ipairs(targets) do
    local ok, send_err = udp:sendto(M.build_search_request(st, options), M.MULTICAST_ADDR, M.MULTICAST_PORT)
    if not ok then
      udp:close()
      return nil, error_mod.new("io", "failed to send SSDP search", { cause = send_err, st = st })
    end
  end
  collect_responses(udp, timeout, out, seen)
  udp:close()
  return out
end

return M
