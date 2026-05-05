--- KAD-DHT bootstrap discovery and dial workflows.
-- @module lua_libp2p.kad_dht.bootstrap
local bootstrap_defaults = require("lua_libp2p.bootstrap")
local discovery = require("lua_libp2p.discovery")
local discovery_bootstrap = require("lua_libp2p.peer_discovery_bootstrap")
local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log").subsystem("kad_dht")
local multiaddr = require("lua_libp2p.multiaddr")

local M = {}

function M.default_peer_discovery(opts)
  local options = opts or {}
  local bootstrap_source, bootstrap_err = discovery_bootstrap.new({
    list = options.bootstrappers or bootstrap_defaults.DEFAULT_BOOTSTRAPPERS,
    dnsaddr_resolver = options.dnsaddr_resolver,
    dialable_only = options.dialable_only,
    ignore_resolve_errors = options.ignore_resolve_errors,
  })
  if not bootstrap_source then return nil, bootstrap_err end
  return discovery.new({ sources = { bootstrap_source } })
end

function M.resolve_bootstrap_addrs(addrs, opts)
  local options = opts or {}
  if type(addrs) ~= "table" then
    return nil, error_mod.new("input", "bootstrap addrs must be a list")
  end
  local bootstrap_source, bootstrap_err = discovery_bootstrap.new({
    list = addrs,
    dnsaddr_resolver = options.dnsaddr_resolver,
    ignore_resolve_errors = options.ignore_resolve_errors,
  })
  if not bootstrap_source then return nil, bootstrap_err end
  local peers, peers_err = bootstrap_source:discover({
    dialable_only = false,
    ignore_resolve_errors = options.ignore_resolve_errors,
    dnsaddr_resolver = options.dnsaddr_resolver,
  })
  if not peers then return nil, peers_err end
  local out = {}
  for _, peer in ipairs(peers) do
    if type(peer.addrs) == "table" and type(peer.addrs[1]) == "string" then
      out[#out + 1] = peer.addrs[1]
    end
  end
  return out
end

local function extract_peer_id_from_multiaddr(addr)
  local parsed = multiaddr.parse(addr)
  if not parsed then return nil end
  for i = #parsed.components, 1, -1 do
    local c = parsed.components[i]
    if c.protocol == "p2p" and c.value then return c.value end
  end
  return nil
end

function M.bootstrap_targets(dht, opts)
  local options = opts or {}
  local discoverer = options.peer_discovery or dht.peer_discovery
  if not discoverer then
    local discover_err
    discoverer, discover_err = M.default_peer_discovery({
      bootstrappers = options.bootstrappers or dht.bootstrappers or bootstrap_defaults.DEFAULT_BOOTSTRAPPERS,
      dnsaddr_resolver = options.dnsaddr_resolver or dht._dnsaddr_resolver,
      ignore_resolve_errors = options.ignore_resolve_errors,
      dialable_only = true,
    })
    if not discoverer then return nil, discover_err end
  end
  local peers, peers_err = discoverer:discover({
    dnsaddr_resolver = options.dnsaddr_resolver or dht._dnsaddr_resolver,
    ignore_source_errors = options.ignore_resolve_errors,
    dialable_only = true,
  })
  if not peers then return nil, peers_err end
  local out = {}
  for _, peer in ipairs(peers) do
    if type(peer.addrs) == "table" and type(peer.addrs[1]) == "string" then
      out[#out + 1] = peer.addrs[1]
    end
  end
  return out
end

function M.discover_peers(dht, opts)
  local options = opts or {}
  local discoverer = options.peer_discovery or dht.peer_discovery
  if not discoverer then
    return nil, error_mod.new("state", "no peer discovery configured")
  end
  return discoverer:discover(options)
end

function M.bootstrap(dht, opts)
  local options = opts or {}
  local yield = type(options.yield) == "function" and options.yield or nil
  if not dht.host or type(dht.host.dial) ~= "function" then
    return nil, error_mod.new("state", "bootstrap requires host with dial")
  end
  local candidates, discover_err = dht:discover_peers({
    peer_discovery = options.peer_discovery,
    dnsaddr_resolver = options.dnsaddr_resolver or dht._dnsaddr_resolver,
    ignore_source_errors = options.ignore_discovery_errors,
    dialable_only = true,
  })
  if not candidates then return nil, discover_err end
  log.debug("kad dht bootstrap started", {
    candidates = #candidates,
    require_protocol = options.require_protocol ~= false,
  })

  local result = { attempted = 0, connected = 0, added = 0, skipped = 0, failed = 0, peers = {}, errors = {} }
  local seen = {}
  for _, candidate in ipairs(candidates) do
    local peer_id = candidate.peer_id
    local addrs = dht:_filter_addrs(candidate.addrs or {}, { peer_id = peer_id, purpose = "bootstrap" })
    for _, addr in ipairs(addrs) do
      local dialable, dialable_err = dht:_is_dialable_addr(addr)
      if dialable == nil and dialable_err then
        return nil, dialable_err
      end
      if not dialable then
        result.skipped = result.skipped + 1
        goto continue_addrs
      end
      local key = tostring(peer_id or "") .. "|" .. tostring(addr)
      if seen[key] then goto continue_addrs end
      seen[key] = true
      result.attempted = result.attempted + 1
      log.debug("kad dht bootstrap dial attempt", {
        peer_id = peer_id,
        addr = addr,
      })

      local conn, state, dial_err = dht.host:dial({ peer_id = peer_id, addr = addr }, options.dial_opts)
      if conn then
        result.connected = result.connected + 1
        local discovered_peer = peer_id
        if not discovered_peer and state and state.remote_peer_id then discovered_peer = state.remote_peer_id end
        if not discovered_peer then discovered_peer = extract_peer_id_from_multiaddr(addr) end
        if discovered_peer then
          local require_protocol = options.require_protocol ~= false
          if require_protocol then
            local supported, supported_err = dht:_supports_kad_protocol(discovered_peer or addr, options.protocol_check_opts)
            if not supported then
              log.debug("kad dht bootstrap protocol check failed", {
                peer_id = discovered_peer,
                addr = addr,
                cause = tostring(supported_err),
              })
              result.failed = result.failed + 1
              result.errors[#result.errors + 1] = error_mod.wrap("protocol", "peer does not support kad-dht protocol", supported_err, {
                peer_id = discovered_peer,
                addr = addr,
                protocol_id = dht.protocol_id,
              })
              goto continue_addrs
            end
          end
          if dht.host and dht.host.peerstore then
            dht.host.peerstore:merge(discovered_peer, { addrs = { addr }, protocols = { dht.protocol_id } })
          end
          local added, add_err = dht:add_peer(discovered_peer, { allow_replace = options.allow_replace })
          if added then
            result.added = result.added + 1
            log.debug("kad dht bootstrap peer added", {
              peer_id = discovered_peer,
              addr = addr,
            })
          elseif add_err and error_mod.is_error(add_err) and add_err.kind == "capacity" then
            result.skipped = result.skipped + 1
            log.debug("kad dht bootstrap peer skipped", {
              peer_id = discovered_peer,
              addr = addr,
              reason = "capacity",
            })
          elseif add_err and not options.ignore_add_errors then
            result.failed = result.failed + 1
            result.errors[#result.errors + 1] = add_err
            log.debug("kad dht bootstrap peer add failed", {
              peer_id = discovered_peer,
              addr = addr,
              cause = tostring(add_err),
            })
          end
          result.peers[#result.peers + 1] = discovered_peer
        end
      else
        result.failed = result.failed + 1
        result.errors[#result.errors + 1] = dial_err
        log.debug("kad dht bootstrap dial failed", {
          peer_id = peer_id,
          addr = addr,
          cause = tostring(dial_err),
        })
        if options.fail_fast then return nil, dial_err end
      end

      if type(options.max_success) == "number" and options.max_success > 0 and result.connected >= options.max_success then
        log.debug("kad dht bootstrap completed", result)
        return result
      end
      if yield then
        local yield_ok, yield_err = yield()
        if yield_ok == nil and yield_err then return nil, yield_err end
      end
      ::continue_addrs::
    end
  end
  log.debug("kad dht bootstrap completed", result)
  return result
end

return M
