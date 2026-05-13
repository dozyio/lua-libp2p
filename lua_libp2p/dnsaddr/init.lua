--- DNSADDR resolver utilities.
---@class Libp2pDnsaddrResolveOptions
---@field max_depth? integer
---@field resolver? fun(domain: string): string[]|nil, table|nil

local error_mod = require("lua_libp2p.error")
local multiaddr = require("lua_libp2p.multiformats.multiaddr")
local socket = require("socket")

local M = {}
M.DEFAULT_MAX_DEPTH = 4

local function read_file(path)
  local f = io.open(path, "rb")
  if not f then
    return nil
  end
  local data = f:read("*a")
  f:close()
  return data
end

local function system_dns_servers()
  local data = read_file("/etc/resolv.conf") or ""
  local out = {}
  local seen = {}
  for line in data:gmatch("[^\r\n]+") do
    local ns = line:match("^%s*nameserver%s+([^%s#;]+)")
    if ns and not seen[ns] then
      seen[ns] = true
      out[#out + 1] = ns
    end
  end
  if #out == 0 then
    out = { "127.0.0.1", "::1" }
  end
  return out
end

local function u16be(n)
  local hi = math.floor(n / 256) % 256
  local lo = n % 256
  return string.char(hi, lo)
end

local function parse_u16be(bytes, offset)
  local a, b = bytes:byte(offset, offset + 1)
  if not a or not b then
    return nil
  end
  return a * 256 + b
end

local function encode_dns_name(name)
  local out = {}
  for label in tostring(name):gmatch("[^.]+") do
    if #label == 0 or #label > 63 then
      return nil, "invalid dns label length"
    end
    out[#out + 1] = string.char(#label)
    out[#out + 1] = label
  end
  out[#out + 1] = "\0"
  return table.concat(out)
end

local function skip_dns_name(packet, offset)
  local i = offset
  while true do
    local len = packet:byte(i)
    if not len then
      return nil
    end
    if len == 0 then
      return i + 1
    end
    if len >= 192 then
      local next_byte = packet:byte(i + 1)
      if not next_byte then
        return nil
      end
      return i + 2
    end
    i = i + 1 + len
  end
end

local function parse_txt_rdata_parts(rdata)
  local parts = {}
  local i = 1
  while i <= #rdata do
    local len = rdata:byte(i)
    if not len then
      break
    end
    local start_i = i + 1
    local end_i = start_i + len - 1
    if end_i > #rdata then
      break
    end
    parts[#parts + 1] = rdata:sub(start_i, end_i)
    i = end_i + 1
  end
  return parts
end

local function resolve_txt_with_udp(domain, nameserver)
  local qname, qname_err = encode_dns_name("_dnsaddr." .. domain)
  if not qname then
    return nil, qname_err
  end

  local txid = math.random(0, 65535)
  local header = table.concat({
    u16be(txid),
    u16be(0x0100),
    u16be(1),
    u16be(0),
    u16be(0),
    u16be(0),
  })
  local query = header .. qname .. u16be(16) .. u16be(1)

  local udp = assert(socket.udp())
  udp:settimeout(3)
  udp:setpeername(nameserver, 53)
  local ok, send_err = udp:send(query)
  if not ok then
    udp:close()
    return nil, send_err or "dns send failed"
  end
  local response, recv_err = udp:receive()
  udp:close()
  if not response then
    return nil, recv_err or "dns receive failed"
  end
  if #response < 12 then
    return nil, "short dns response"
  end
  if parse_u16be(response, 1) ~= txid then
    return nil, "dns txid mismatch"
  end
  local flags = parse_u16be(response, 3)
  local rcode = flags and (flags % 16) or nil
  if rcode ~= 0 then
    return nil, "dns error rcode=" .. tostring(rcode)
  end

  local qdcount = parse_u16be(response, 5) or 0
  local ancount = parse_u16be(response, 7) or 0
  local offset = 13
  for _ = 1, qdcount do
    offset = skip_dns_name(response, offset)
    if not offset or offset + 3 > #response then
      return nil, "malformed dns question"
    end
    offset = offset + 4
  end

  local out = {}
  for _ = 1, ancount do
    offset = skip_dns_name(response, offset)
    if not offset or offset + 9 > #response then
      return nil, "malformed dns answer"
    end
    local rr_type = parse_u16be(response, offset)
    local rdlen = parse_u16be(response, offset + 8)
    if not rr_type or not rdlen then
      return nil, "malformed dns rr"
    end
    local rdata_start = offset + 10
    local rdata_end = rdata_start + rdlen - 1
    if rdata_end > #response then
      return nil, "truncated dns rdata"
    end
    if rr_type == 16 then
      for _, txt in ipairs(parse_txt_rdata_parts(response:sub(rdata_start, rdata_end))) do
        if txt and txt:sub(1, 8) == "dnsaddr=" then
          out[#out + 1] = txt
        end
      end
    end
    offset = rdata_end + 1
  end
  return out
end

---@param domain string
---@return string[]|nil records
---@return table|nil err
function M.default_resolver(domain)
  local last_err = nil
  for _, nameserver in ipairs(system_dns_servers()) do
    local records, err = resolve_txt_with_udp(domain, nameserver)
    if records then
      return records
    end
    last_err = err
  end
  return nil, last_err or "dns lookup failed"
end

local function parse(addr)
  if type(addr) ~= "string" or addr == "" then
    return nil, error_mod.new("input", "multiaddr must be a non-empty string")
  end
  local parsed, parse_err = multiaddr.parse(addr)
  if not parsed then
    return nil, parse_err
  end
  return parsed
end

local function peer_id_component(parsed)
  for _, c in ipairs(parsed.components or {}) do
    if c.protocol == "p2p" and type(c.value) == "string" and c.value ~= "" then
      return c.value
    end
  end
  return nil
end

---@param addr string
---@return boolean
function M.is_dnsaddr(addr)
  local parsed = parse(addr)
  if not parsed then
    return false
  end
  local first = parsed.components and parsed.components[1]
  return first ~= nil and first.protocol == "dnsaddr"
end

---@param addr string
---@return string|nil domain
---@return table|nil err
function M.extract_domain(addr)
  local parsed, parse_err = parse(addr)
  if not parsed then
    return nil, parse_err
  end
  local first = parsed.components and parsed.components[1]
  if not first or first.protocol ~= "dnsaddr" then
    return nil, error_mod.new("input", "multiaddr is not dnsaddr")
  end
  return first.value
end

---@param addr string
---@return string|nil peer_id
function M.extract_peer_id(addr)
  local parsed, parse_err = parse(addr)
  if not parsed then
    return nil, parse_err
  end
  return peer_id_component(parsed)
end

--- Resolve dnsaddr multiaddrs recursively.
-- `opts` requires `resolver(domain)` and supports `max_depth`.
-- Resolver should return a list of TXT record strings.
--- addr string Multiaddr.
--- opts? table
--- table|nil addrs
--- table|nil err
---@param addr string
---@param opts? Libp2pDnsaddrResolveOptions
---@return string[]|nil addrs
---@return table|nil err
function M.resolve(addr, opts)
  local options = opts or {}
  if not M.is_dnsaddr(addr) then
    return { addr }
  end

  local resolver = options.resolver
  if type(resolver) ~= "function" then
    return nil, error_mod.new("input", "dnsaddr resolver function is required")
  end

  local max_depth = options.max_depth or M.DEFAULT_MAX_DEPTH
  if type(max_depth) ~= "number" or max_depth < 0 then
    return nil, error_mod.new("input", "dnsaddr max_depth must be >= 0")
  end

  local seen = {}
  local out = {}
  local out_seen = {}

  local function append_out(candidate)
    if not out_seen[candidate] then
      out_seen[candidate] = true
      out[#out + 1] = candidate
    end
  end

  local function resolve_inner(current, depth)
    if type(current) ~= "string" or current == "" then
      return nil, error_mod.new("input", "dnsaddr candidate must be a non-empty string")
    end
    if seen[current] then
      return true
    end
    seen[current] = true

    if not M.is_dnsaddr(current) then
      append_out(current)
      return true
    end
    if depth > max_depth then
      return nil,
        error_mod.new("decode", "dnsaddr recursion depth exceeded", {
          max_depth = max_depth,
          candidate = current,
        })
    end

    local domain, domain_err = M.extract_domain(current)
    if not domain then
      return nil, domain_err
    end

    local expected_peer_id, peer_err = M.extract_peer_id(current)
    if expected_peer_id == nil and peer_err then
      return nil, peer_err
    end

    local records, resolve_err = resolver(domain)
    if not records then
      return nil, resolve_err or error_mod.new("io", "dnsaddr resolver failed", { domain = domain })
    end
    if type(records) ~= "table" then
      return nil, error_mod.new("input", "dnsaddr resolver must return a list of records")
    end

    for _, raw in ipairs(records) do
      local candidate = raw
      if type(candidate) == "string" and candidate:sub(1, 8) == "dnsaddr=" then
        candidate = candidate:sub(9)
      end
      if type(candidate) == "string" and candidate:sub(1, 1) == "/" then
        local parsed = multiaddr.parse(candidate)
        if parsed then
          if expected_peer_id then
            local candidate_peer = peer_id_component(parsed)
            if candidate_peer ~= expected_peer_id then
              goto continue
            end
          end

          local ok, err = resolve_inner(candidate, depth + 1)
          if not ok then
            return nil, err
          end
        end
      end

      ::continue::
    end

    return true
  end

  local ok, err = resolve_inner(addr, 0)
  if not ok then
    return nil, err
  end

  return out
end

return M
