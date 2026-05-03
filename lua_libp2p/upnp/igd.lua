--- UPnP IGD SOAP client helpers.
-- @module lua_libp2p.upnp.igd
local http = require("socket.http")
local ltn12 = require("ltn12")

local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log")
local ssdp = require("lua_libp2p.upnp.ssdp")

local M = {}

local SERVICE_TYPES = {
  "urn:schemas-upnp-org:service:WANIPConnection:2",
  "urn:schemas-upnp-org:service:WANIPConnection:1",
  "urn:schemas-upnp-org:service:WANPPPConnection:1",
}

local function selected_service_types(opts)
  local options = opts or {}
  if type(options.service_types) == "table" and #options.service_types > 0 then
    return options.service_types
  end
  return SERVICE_TYPES
end

local ACTION_ARG_ORDER = {
  AddPortMapping = {
    "NewRemoteHost",
    "NewExternalPort",
    "NewProtocol",
    "NewInternalPort",
    "NewInternalClient",
    "NewEnabled",
    "NewPortMappingDescription",
    "NewLeaseDuration",
  },
  DeletePortMapping = {
    "NewRemoteHost",
    "NewExternalPort",
    "NewProtocol",
  },
  GetSpecificPortMappingEntry = {
    "NewRemoteHost",
    "NewExternalPort",
    "NewProtocol",
  },
  GetGenericPortMappingEntry = {
    "NewPortMappingIndex",
  },
}

local function trim(value)
  return (tostring(value or ""):gsub("^%s+", ""):gsub("%s+$", ""))
end

local function xml_escape(value)
  return tostring(value or "")
    :gsub("&", "&amp;")
    :gsub("<", "&lt;")
    :gsub(">", "&gt;")
    :gsub('"', "&quot;")
end

local function parse_url(url)
  local scheme, rest = tostring(url or ""):match("^(https?)://(.+)$")
  if not scheme then
    return nil
  end
  local authority, path = rest:match("^([^/]+)(/.*)$")
  if not authority then
    authority, path = rest, "/"
  end
  local host, port = authority:match("^%[([^%]]+)%]:(%d+)$")
  if not host then
    host, port = authority:match("^([^:]+):(%d+)$")
  end
  if not host then
    host = authority:gsub("^%[", ""):gsub("%]$", "")
  end
  return {
    scheme = scheme,
    host = host,
    port = tonumber(port) or (scheme == "https" and 443 or 80),
    path = path,
    base = scheme .. "://" .. authority,
  }
end

local function resolve_url(base_url, maybe_relative)
  if type(maybe_relative) ~= "string" or maybe_relative == "" then
    return nil
  end
  if maybe_relative:match("^https?://") then
    return maybe_relative
  end
  local parsed = parse_url(base_url)
  if not parsed then
    return nil
  end
  if maybe_relative:sub(1, 1) == "/" then
    return parsed.base .. maybe_relative
  end
  local dir = parsed.path:gsub("[^/]*$", "")
  if dir == "" then
    dir = "/"
  end
  return parsed.base .. dir .. maybe_relative
end

local function http_request(url, opts)
  local options = opts or {}
  if options.debug_raw then
    log.info("upnp http request", {
      method = options.method or "GET",
      url = url,
      headers = options.headers,
      body = options.body,
    })
  end
  local chunks = {}
  local ok, code, headers, status = http.request({
    url = url,
    method = options.method or "GET",
    headers = options.headers,
    source = options.body and ltn12.source.string(options.body) or nil,
    sink = ltn12.sink.table(chunks),
  })
  if not ok then
    return nil, error_mod.new("io", "HTTP request failed", { url = url, cause = code })
  end
  code = tonumber(code)
  local body = table.concat(chunks)
  if options.debug_raw then
    log.info("upnp http response", {
      url = url,
      code = code,
      status = status,
      headers = headers,
      body = body,
    })
  end
  if not code or code < 200 or code >= 300 then
    return nil, error_mod.new("protocol", "HTTP request returned non-success status", {
      url = url,
      code = code,
      status = status,
      body = body,
    })
  end
  return body, headers, code
end

local function service_blocks(xml)
  local blocks = {}
  for block in tostring(xml or ""):gmatch("<%s*service%s*>.-<%s*/%s*service%s*>") do
    blocks[#blocks + 1] = block
  end
  return blocks
end

local function descriptor_service_types(xml)
  local out = {}
  for _, block in ipairs(service_blocks(xml)) do
    local service_type = trim(tostring(block):match("<%s*serviceType%s*>(.-)<%s*/%s*serviceType%s*>") or "")
    if service_type ~= "" then
      out[#out + 1] = service_type
    end
  end
  return out
end

local function tag_text(xml, tag)
  return trim(tostring(xml or ""):match("<%s*" .. tag .. "%s*>(.-)<%s*/%s*" .. tag .. "%s*>") or "")
end

--- Parse IGD device descriptor XML and pick WANIP/WANPPP service.
-- `opts.wanppp_only=true` restricts service selection to WANPPPConnection.
-- `opts.debug_raw=true` logs descriptor service candidates.
function M.parse_descriptor(xml, location, opts)
  local wanted_types = selected_service_types(opts)
  if opts and opts.debug_raw then
    log.info("upnp descriptor services discovered", {
      location = location,
      service_types = descriptor_service_types(xml),
      wanted_service_types = wanted_types,
    })
  end
  for _, block in ipairs(service_blocks(xml)) do
    local service_type = tag_text(block, "serviceType")
    for _, wanted in ipairs(wanted_types) do
      if service_type == wanted then
        local control_url = tag_text(block, "controlURL")
        local scpd_url = tag_text(block, "SCPDURL")
        return {
          location = location,
          service_type = service_type,
          control_url = resolve_url(location, control_url),
          scpd_url = resolve_url(location, scpd_url),
        }
      end
    end
  end
  return nil, error_mod.new("protocol", "UPnP descriptor missing WAN connection service")
end

function M.soap_envelope(action, args, service_type)
  local ns = service_type or "%s"
  local values = args or {}
  local parts = {
    '<?xml version="1.0"?>',
    '<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">',
    '<s:Body>',
    '<u:' .. action .. ' xmlns:u="' .. xml_escape(ns) .. '">',
  }
  local used = {}
  for _, name in ipairs(ACTION_ARG_ORDER[action] or {}) do
    if values[name] ~= nil then
      parts[#parts + 1] = "<" .. name .. ">" .. xml_escape(values[name]) .. "</" .. name .. ">"
      used[name] = true
    end
  end
  local extra_names = {}
  for name in pairs(values) do
    if not used[name] then
      extra_names[#extra_names + 1] = name
    end
  end
  table.sort(extra_names)
  for _, name in ipairs(extra_names) do
    local value = values[name]
    parts[#parts + 1] = "<" .. name .. ">" .. xml_escape(value) .. "</" .. name .. ">"
  end
  parts[#parts + 1] = "</u:" .. action .. ">"
  parts[#parts + 1] = "</s:Body>"
  parts[#parts + 1] = "</s:Envelope>"
  return table.concat(parts)
end

local Client = {}
Client.__index = Client

function Client:soap(action, args)
  if not self.control_url or not self.service_type then
    return nil, error_mod.new("state", "UPnP client missing service control URL")
  end
  local body = M.soap_envelope(action, args, self.service_type)
  if self.debug_soap then
    io.stderr:write("\n[upnp soap] POST " .. tostring(self.control_url) .. "\n")
    io.stderr:write("[upnp soap] SOAPAction: \"" .. tostring(self.service_type) .. "#" .. action .. "\"\n")
    io.stderr:write(body .. "\n")
  end
  return http_request(self.control_url, {
    method = "POST",
    body = body,
    debug_raw = self.debug_raw,
    headers = {
      ["Content-Type"] = 'text/xml; charset="utf-8"',
      ["Content-Length"] = tostring(#body),
      ["SOAPAction"] = '"' .. self.service_type .. "#" .. action .. '"',
    },
  })
end

function Client:get_external_ip()
  local body, err = self:soap("GetExternalIPAddress")
  if not body then
    return nil, err
  end
  local ip = tag_text(body, "NewExternalIPAddress")
  if ip == "" then
    return nil, error_mod.new("protocol", "UPnP GetExternalIPAddress response missing IP")
  end
  return ip
end

function Client:add_port_mapping(protocol, internal_port, external_port, internal_client, ttl, description)
  local body, err = self:soap("AddPortMapping", {
    NewRemoteHost = "",
    NewExternalPort = external_port or internal_port,
    NewProtocol = string.upper(protocol),
    NewInternalPort = internal_port,
    NewInternalClient = internal_client,
    NewEnabled = 1,
    NewPortMappingDescription = description or "lua-libp2p",
    NewLeaseDuration = ttl or 720,
  })
  if not body then
    return nil, err
  end
  return {
    protocol = string.lower(protocol),
    internal_port = tonumber(internal_port),
    external_port = tonumber(external_port or internal_port),
    internal_client = internal_client,
  }
end

function Client:get_specific_port_mapping(protocol, external_port)
  local body, err = self:soap("GetSpecificPortMappingEntry", {
    NewRemoteHost = "",
    NewExternalPort = external_port,
    NewProtocol = string.upper(protocol),
  })
  if not body then
    return nil, err
  end
  return {
    protocol = string.lower(protocol),
    external_port = tonumber(external_port),
    internal_client = tag_text(body, "NewInternalClient"),
    internal_port = tonumber(tag_text(body, "NewInternalPort")),
    enabled = tag_text(body, "NewEnabled"),
    description = tag_text(body, "NewPortMappingDescription"),
    lease_duration = tonumber(tag_text(body, "NewLeaseDuration")),
  }
end

function Client:get_generic_port_mapping(index)
  local body, err = self:soap("GetGenericPortMappingEntry", {
    NewPortMappingIndex = index,
  })
  if not body then
    return nil, err
  end
  return {
    index = index,
    remote_host = tag_text(body, "NewRemoteHost"),
    external_port = tonumber(tag_text(body, "NewExternalPort")),
    protocol = tag_text(body, "NewProtocol"),
    internal_port = tonumber(tag_text(body, "NewInternalPort")),
    internal_client = tag_text(body, "NewInternalClient"),
    enabled = tag_text(body, "NewEnabled"),
    description = tag_text(body, "NewPortMappingDescription"),
    lease_duration = tonumber(tag_text(body, "NewLeaseDuration")),
  }
end

--- List IGD port mappings.
-- `opts.max` (`number`, default `64`) limits entry scan count.
-- @tparam[opt] table opts
-- @treturn table mappings
function Client:list_port_mappings(opts)
  local options = opts or {}
  local max = options.max or 64
  local out = {}
  for index = 0, max - 1 do
    local mapping = self:get_generic_port_mapping(index)
    if not mapping then
      break
    end
    out[#out + 1] = mapping
  end
  return out
end

function Client:delete_port_mapping(protocol, external_port)
  local body, err = self:soap("DeletePortMapping", {
    NewRemoteHost = "",
    NewExternalPort = external_port,
    NewProtocol = string.upper(protocol),
  })
  if not body then
    return nil, err
  end
  return true
end

function M.new(service)
  if type(service) ~= "table" then
    return nil, error_mod.new("input", "UPnP IGD client requires service descriptor")
  end
  return setmetatable(service, Client)
end

--- Build IGD client from descriptor URL.
-- `opts.debug_raw` enables raw HTTP capture.
-- Parser options are forwarded to `parse_descriptor`.
-- @tparam string location
-- @tparam[opt] table opts
-- @treturn table|nil client
-- @treturn[opt] table err
function M.from_location(location, opts)
  local body, err = http_request(location, {
    debug_raw = opts and opts.debug_raw,
  })
  if not body then
    return nil, err
  end
  local service, service_err = M.parse_descriptor(body, location, opts)
  if not service then
    return nil, service_err
  end
  return M.new(service)
end

--- Discover first usable IGD via SSDP.
-- `opts` is forwarded to `ssdp.discover(opts)` and `from_location(..., opts)`.
-- Common fields: `max_results`, `mx_seconds`, `timeout`, `debug_raw`.
-- @tparam[opt] table opts
-- @treturn table|nil client
-- @treturn[opt] table err
function M.discover(opts)
  local options = opts or {}
  local responses, err = ssdp.discover(opts)
  if not responses then
    return nil, err
  end
  local last_err
  for _, response in ipairs(responses) do
    local client, client_err = M.from_location(response.location, options)
    if client then
      return client
    end
    last_err = client_err
  end
  return nil, last_err or error_mod.new("not_found", "no UPnP IGD gateway found")
end

M.Client = Client

return M
