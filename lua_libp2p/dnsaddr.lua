local error_mod = require("lua_libp2p.error")
local multiaddr = require("lua_libp2p.multiaddr")

local M = {}
M.DEFAULT_MAX_DEPTH = 4

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

function M.is_dnsaddr(addr)
  local parsed = parse(addr)
  if not parsed then
    return false
  end
  local first = parsed.components and parsed.components[1]
  return first ~= nil and first.protocol == "dnsaddr"
end

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

function M.extract_peer_id(addr)
  local parsed, parse_err = parse(addr)
  if not parsed then
    return nil, parse_err
  end
  return peer_id_component(parsed)
end

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
      return nil, error_mod.new("decode", "dnsaddr recursion depth exceeded", {
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
