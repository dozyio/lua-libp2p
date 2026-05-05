--- SSDP discovery helpers.
-- @module lua_libp2p.upnp.ssdp
local socket = require("socket")

local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log").subsystem("upnp")

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

--- Parse a raw SSDP response datagram.
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

--- Build an SSDP M-SEARCH request.
-- `opts.mx` (`number`, default `2`) controls responder delay budget.
-- `opts.host` overrides default multicast host header.
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

local function collect_responses(udp, timeout, out, seen, opts)
  local options = opts or {}
  local deadline = socket.gettime() + timeout
  while socket.gettime() < deadline do
    udp:settimeout(math.max(deadline - socket.gettime(), 0.05))
    local data, ip, port = udp:receivefrom()
    if data then
      if options.debug_raw then
        log.info("upnp ssdp raw response", {
          source_ip = ip,
          source_port = port,
          payload = data,
        })
      end
      local response = M.parse_response(data)
      if response and response.location and not seen[response.location] then
        seen[response.location] = true
        out[#out + 1] = response
      end
    end
  end
end

--- Discover SSDP responders for target service types.
-- `opts.timeout` (`number`, default `10`) receive timeout seconds.
-- `opts.search_targets` overrides target ST list.
-- `opts.interface` binds source interface/address.
-- `opts.max_results` truncates collected responses.
-- `opts.debug_raw=true` logs raw datagrams.
function M.discover(opts)
  local options = opts or {}
  local timeout = options.timeout or 10
  local targets = options.search_targets or M.SEARCH_TARGETS
  local out = {}
  local seen = {}
  log.debug("upnp ssdp discovery started", {
    timeout = timeout,
    targets = #targets,
    interface = options.interface,
  })

  local udp, udp_err = socket.udp()
  if not udp then
    log.debug("upnp ssdp discovery failed", {
      stage = "socket",
      cause = tostring(udp_err),
    })
    return nil, error_mod.new("io", "failed to create SSDP UDP socket", { cause = udp_err })
  end
  udp:settimeout(timeout)
  if options.interface then
    pcall(function()
      udp:setsockname(options.interface, 0)
    end)
  end

  for _, st in ipairs(targets) do
    local payload = M.build_search_request(st, options)
    if options.debug_raw then
      log.info("upnp ssdp raw search", {
        st = st,
        multicast_addr = M.MULTICAST_ADDR,
        multicast_port = M.MULTICAST_PORT,
        payload = payload,
      })
    end
    local ok, send_err = udp:sendto(payload, M.MULTICAST_ADDR, M.MULTICAST_PORT)
    if not ok then
      udp:close()
      log.debug("upnp ssdp search failed", {
        st = st,
        cause = tostring(send_err),
      })
      return nil, error_mod.new("io", "failed to send SSDP search", { cause = send_err, st = st })
    end
    log.debug("upnp ssdp search sent", {
      st = st,
      multicast_addr = M.MULTICAST_ADDR,
      multicast_port = M.MULTICAST_PORT,
    })
  end
  collect_responses(udp, timeout, out, seen, options)
  udp:close()
  log.debug("upnp ssdp discovery completed", {
    responses = #out,
  })
  return out
end

return M
