--- Bootstrap peer discovery source.
-- @module lua_libp2p.peer_discovery_bootstrap
local dnsaddr = require("lua_libp2p.dnsaddr")
local error_mod = require("lua_libp2p.error")
local multiaddr = require("lua_libp2p.multiaddr")
local bootstrap = require("lua_libp2p.bootstrap")
local table_utils = require("lua_libp2p.util.tables")

local M = {}

local BootstrapSource = {}
BootstrapSource.__index = BootstrapSource

local function parse_peer_id(addr)
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

local copy_list = table_utils.copy_list

local function is_dialable_tcp_addr(addr)
  local parsed = multiaddr.parse(addr)
  if not parsed or type(parsed.components) ~= "table" or #parsed.components < 2 then
    return false
  end
  local host_part = parsed.components[1]
  local tcp_part = parsed.components[2]
  if host_part.protocol ~= "ip4" and host_part.protocol ~= "dns" and host_part.protocol ~= "dns4" and host_part.protocol ~= "dns6" then
    return false
  end
  if tcp_part.protocol ~= "tcp" then
    return false
  end
  for i = 3, #parsed.components do
    if parsed.components[i].protocol ~= "p2p" then
      return false
    end
  end
  return true
end

--- Discover peers from bootstrap list.
-- `opts.dnsaddr_resolver` overrides resolver function.
-- `opts.dialable_only` (`boolean`) filters non-TCP candidates.
-- `opts.ignore_resolve_errors` (`boolean`) skips bad records.
-- @tparam[opt] table opts
-- @treturn table|nil peers
-- @treturn[opt] table err
function BootstrapSource:discover(opts)
  local options = opts or {}
  local resolver = options.dnsaddr_resolver or self.dnsaddr_resolver
  local dialable_only = not not (options.dialable_only or self.dialable_only)
  local ignore_resolve_errors = not not (options.ignore_resolve_errors or self.ignore_resolve_errors)
  local addr_filter = options.addr_filter

  local out = {}

  for _, addr in ipairs(self.list) do
    local resolved, resolve_err = dnsaddr.resolve(addr, { resolver = resolver })
    if not resolved then
      if ignore_resolve_errors then
        goto continue
      end
      return nil, resolve_err
    end

    for _, candidate in ipairs(resolved) do
      local dialable = true
      if dialable_only then
        if type(addr_filter) == "function" then
          dialable = addr_filter(candidate) ~= false
        else
          dialable = is_dialable_tcp_addr(candidate)
        end
      end
      if dialable then
        out[#out + 1] = {
          peer_id = parse_peer_id(candidate),
          addrs = { candidate },
          source = "bootstrap",
        }
      end
    end

    ::continue::
  end

  return out
end

--- Construct bootstrap discovery source.
-- `opts.list` (`table<string>`) bootstrap multiaddrs (defaults to built-in set).
-- `opts.dnsaddr_resolver` (`function`) resolver for dnsaddr records.
-- `opts.dialable_only` (`boolean`) keeps only dialable TCP addrs.
-- `opts.ignore_resolve_errors` (`boolean`) suppresses resolver failures.
-- @tparam[opt] table opts
-- @treturn table|nil source
-- @treturn[opt] table err
function M.new(opts)
  local options = opts or {}
  local list = options.list
  if list == nil then
    list = bootstrap.DEFAULT_BOOTSTRAPPERS
  end
  if type(list) ~= "table" then
    return nil, error_mod.new("input", "bootstrap source list must be provided")
  end

  local resolved_opts = {
    list = copy_list(list),
    dnsaddr_resolver = options.dnsaddr_resolver or dnsaddr.default_resolver,
    dialable_only = not not options.dialable_only,
    ignore_resolve_errors = not not options.ignore_resolve_errors,
    dial_on_start = options.dial_on_start,
    timeout = options.timeout,
    delay = options.delay,
    tag_name = options.tag_name,
    tag_value = options.tag_value,
    tag_ttl = options.tag_ttl,
  }

  return setmetatable({
    list = copy_list(resolved_opts.list),
    dnsaddr_resolver = resolved_opts.dnsaddr_resolver,
    dialable_only = resolved_opts.dialable_only,
    ignore_resolve_errors = resolved_opts.ignore_resolve_errors,
    _bootstrap_config = resolved_opts,
  }, BootstrapSource)
end

M.BootstrapSource = BootstrapSource

return M
