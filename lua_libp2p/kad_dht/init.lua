local error_mod = require("lua_libp2p.error")
local dnsaddr = require("lua_libp2p.dnsaddr")
local discovery = require("lua_libp2p.discovery")
local discovery_bootstrap = require("lua_libp2p.discovery.bootstrap")
local kbucket = require("lua_libp2p.kbucket")
local multiaddr = require("lua_libp2p.multiaddr")
local peerid = require("lua_libp2p.peerid")
local protocol = require("lua_libp2p.kad_dht.protocol")

local M = {}

M.PROTOCOL_ID = "/ipfs/kad/1.0.0"
M.DEFAULT_K = 20
M.DEFAULT_ALPHA = 10
M.DEFAULT_BOOTSTRAPPERS = {
  "/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
  "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
  "/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
  "/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6L6K6TQK6KiBovQ",
  "/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
  "/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
}

local function copy_list(values)
  local out = {}
  for i, value in ipairs(values or {}) do
    out[i] = value
  end
  return out
end

function M.default_bootstrappers(opts)
  local options = opts or {}
  local list = copy_list(M.DEFAULT_BOOTSTRAPPERS)
  if not options.dialable_only then
    return list
  end

  local out = {}
  for _, addr in ipairs(list) do
    local endpoint = multiaddr.to_tcp_endpoint(addr)
    if endpoint then
      out[#out + 1] = addr
    end
  end
  return out
end

function M.default_peer_discovery(opts)
  local options = opts or {}
  local bootstrap_source, bootstrap_err = discovery_bootstrap.new({
    list = options.bootstrappers or M.DEFAULT_BOOTSTRAPPERS,
    dnsaddr_resolver = options.dnsaddr_resolver,
    dialable_only = options.dialable_only,
    ignore_resolve_errors = options.ignore_resolve_errors,
  })
  if not bootstrap_source then
    return nil, bootstrap_err
  end
  return discovery.new({
    sources = { bootstrap_source },
  })
end

function M.resolve_bootstrap_addrs(addrs, opts)
  local options = opts or {}
  if type(addrs) ~= "table" then
    return nil, error_mod.new("input", "bootstrap addrs must be a list")
  end

  local out = {}
  local bootstrap_source, bootstrap_err = discovery_bootstrap.new({
    list = addrs,
    dnsaddr_resolver = options.dnsaddr_resolver,
    ignore_resolve_errors = options.ignore_resolve_errors,
  })
  if not bootstrap_source then
    return nil, bootstrap_err
  end
  local peers, peers_err = bootstrap_source:discover({
    dialable_only = false,
    ignore_resolve_errors = options.ignore_resolve_errors,
    dnsaddr_resolver = options.dnsaddr_resolver,
  })
  if not peers then
    return nil, peers_err
  end
  for _, peer in ipairs(peers) do
    if type(peer.addrs) == "table" and type(peer.addrs[1]) == "string" then
      out[#out + 1] = peer.addrs[1]
    end
  end

  return out
end

local DHT = {}
DHT.__index = DHT

function DHT:start()
  if self._running then
    return true
  end

  if self.host and type(self.host.handle) == "function" then
    local ok, err = self.host:handle(self.protocol_id, function(stream, ctx)
      return self:_handle_rpc(stream, ctx)
    end)
    if not ok then
      return nil, err
    end
  end

  self._running = true
  return true
end

function DHT:stop()
  if self.host and type(self.host.off) == "function" then
    if self._host_on_connected then
      self.host:off("peer_connected", self._host_on_connected)
    end
    if self._host_on_disconnected then
      self.host:off("peer_disconnected", self._host_on_disconnected)
    end
  end
  self._running = false
  return true
end

function DHT:is_running()
  return self._running
end

function DHT:add_peer(peer_id, opts)
  local added, err = self.routing_table:try_add_peer(peer_id, opts)
  if added and type(peer_id) == "string" and peer_id ~= "" then
    local now = os.time()
    self._peer_health[peer_id] = self._peer_health[peer_id] or {}
    self._peer_health[peer_id].stale = false
    self._peer_health[peer_id].last_connected_at = self._peer_health[peer_id].last_connected_at or now
  end
  return added, err
end

function DHT:remove_peer(peer_id)
  self._peer_health[peer_id] = nil
  return self.routing_table:remove_peer(peer_id)
end

function DHT:find_peer(peer_id)
  return self.routing_table:find_peer(peer_id)
end

function DHT:find_closest_peers(key, count)
  local want = count or self.k
  return self.routing_table:nearest_peers(key, want)
end

local function decode_peer_id_text(id_bytes)
  local parsed = peerid and peerid.parse and peerid.parse(id_bytes)
  if parsed and parsed.id then
    return parsed.id
  end

  local has_non_printable = false
  for i = 1, #id_bytes do
    local b = id_bytes:byte(i)
    if b < 32 or b > 126 then
      has_non_printable = true
      break
    end
  end

  if has_non_printable then
    return peerid.to_base58(id_bytes)
  end

  return id_bytes
end

local function extract_peer_id_from_multiaddr(addr)
  local parsed = multiaddr.parse(addr)
  if not parsed then
    return nil
  end
  for i = #parsed.components, 1, -1 do
    local c = parsed.components[i]
    if c.protocol == "p2p" and c.value then
      return c.value
    end
  end
  return nil
end

function DHT:_closest_peer_records(target_key, count)
  local nearest, nearest_err = self:find_closest_peers(target_key, count)
  if not nearest then
    return nil, nearest_err
  end

  local peers = {}
  for _, entry in ipairs(nearest) do
    local id_bytes, id_err = protocol.peer_bytes(entry.peer_id)
    if not id_bytes then
      return nil, id_err
    end
    peers[#peers + 1] = {
      id = id_bytes,
      addrs = {},
    }
  end
  return peers
end

function DHT:_handle_find_node(req)
  local target_key = req.key
  if type(target_key) ~= "string" or target_key == "" then
    return nil, error_mod.new("input", "FIND_NODE request missing key")
  end

  local peers, peers_err = self:_closest_peer_records(target_key, self.k)
  if not peers then
    return nil, peers_err
  end

  return {
    type = protocol.MESSAGE_TYPE.FIND_NODE,
    key = target_key,
    closer_peers = peers,
  }
end

function DHT:_handle_rpc(stream)
  local req, req_err = protocol.read(stream)
  if not req then
    return nil, req_err
  end

  local response, response_err
  if req.type == protocol.MESSAGE_TYPE.FIND_NODE then
    response, response_err = self:_handle_find_node(req)
  else
    return nil, error_mod.new("unsupported", "kad-dht message type is not supported", { type = req.type })
  end
  if not response then
    return nil, response_err
  end

  local wrote, write_err = protocol.write(stream, response)
  if not wrote then
    return nil, write_err
  end

  if type(stream.close_write) == "function" then
    local ok, close_err = stream:close_write()
    if not ok then
      return nil, close_err
    end
  end

  return true
end

function DHT:find_node(peer_or_addr, target_key, opts)
  if not self.host or type(self.host.new_stream) ~= "function" then
    return nil, error_mod.new("state", "find_node requires host with new_stream")
  end

  local target_bytes, key_err = protocol.peer_bytes(target_key)
  if not target_bytes then
    return nil, key_err
  end

  local stream, _, _, stream_err = self.host:new_stream(peer_or_addr, { self.protocol_id }, opts)
  if not stream then
    return nil, stream_err
  end

  local wrote, write_err = protocol.write(stream, {
    type = protocol.MESSAGE_TYPE.FIND_NODE,
    key = target_bytes,
  })
  if not wrote then
    if type(stream.reset_now) == "function" then
      stream:reset_now()
    elseif type(stream.close) == "function" then
      stream:close()
    end
    return nil, write_err
  end

  if type(stream.close_write) == "function" then
    local ok, close_err = stream:close_write()
    if not ok then
      return nil, close_err
    end
  end

  local response, response_err = protocol.read(stream)
  if not response then
    return nil, response_err
  end

  if response.type ~= protocol.MESSAGE_TYPE.FIND_NODE then
    return nil, error_mod.new("protocol", "unexpected kad-dht response type", {
      expected = protocol.MESSAGE_TYPE.FIND_NODE,
      got = response.type,
    })
  end

  local out = {}
  for _, peer in ipairs(response.closer_peers or {}) do
    out[#out + 1] = {
      peer_id = decode_peer_id_text(peer.id),
      id = peer.id,
      addrs = peer.addrs or {},
    }
  end
  return out
end

function DHT:bootstrap_targets(opts)
  local options = opts or {}
  local discoverer = options.peer_discovery or self.peer_discovery
  if not discoverer then
    local discover_err
    discoverer, discover_err = M.default_peer_discovery({
      bootstrappers = options.bootstrappers or self.bootstrappers or M.DEFAULT_BOOTSTRAPPERS,
      dnsaddr_resolver = options.dnsaddr_resolver or self._dnsaddr_resolver,
      ignore_resolve_errors = options.ignore_resolve_errors,
      dialable_only = true,
    })
    if not discoverer then
      return nil, discover_err
    end
  end

  local peers, peers_err = discoverer:discover({
    dnsaddr_resolver = options.dnsaddr_resolver or self._dnsaddr_resolver,
    ignore_source_errors = options.ignore_resolve_errors,
    dialable_only = true,
  })
  if not peers then
    return nil, peers_err
  end

  local out = {}
  for _, peer in ipairs(peers) do
    if type(peer.addrs) == "table" and type(peer.addrs[1]) == "string" then
      out[#out + 1] = peer.addrs[1]
    end
  end
  return out
end

function DHT:discover_peers(opts)
  local options = opts or {}
  local discoverer = options.peer_discovery or self.peer_discovery
  if not discoverer then
    return nil, error_mod.new("state", "no peer discovery configured")
  end
  return discoverer:discover(options)
end

function DHT:_on_peer_connected(payload)
  local peer_id = payload and payload.peer_id
  if type(peer_id) ~= "string" or peer_id == "" then
    return true
  end

  local now = os.time()
  local health = self._peer_health[peer_id] or {}
  health.last_connected_at = now
  health.stale = false
  self._peer_health[peer_id] = health
  return true
end

function DHT:_on_peer_disconnected(payload)
  local peer_id = payload and payload.peer_id
  if type(peer_id) ~= "string" or peer_id == "" then
    return true
  end

  local health = self._peer_health[peer_id] or {}
  health.last_disconnected_at = os.time()
  health.stale = true
  self._peer_health[peer_id] = health
  return true
end

function DHT:_supports_kad_protocol(peer_or_addr, opts)
  if not self.host or type(self.host.new_stream) ~= "function" then
    return nil, error_mod.new("state", "protocol check requires host with new_stream")
  end

  local stream, _, _, stream_err = self.host:new_stream(peer_or_addr, { self.protocol_id }, opts)
  if not stream then
    return nil, stream_err
  end

  if type(stream.close_write) == "function" then
    pcall(function()
      stream:close_write()
    end)
  end
  if type(stream.close) == "function" then
    pcall(function()
      stream:close()
    end)
  elseif type(stream.reset_now) == "function" then
    pcall(function()
      stream:reset_now()
    end)
  end

  return true
end

function DHT:bootstrap(opts)
  local options = opts or {}
  if not self.host or type(self.host.dial) ~= "function" then
    return nil, error_mod.new("state", "bootstrap requires host with dial")
  end

  local candidates, discover_err = self:discover_peers({
    peer_discovery = options.peer_discovery,
    dnsaddr_resolver = options.dnsaddr_resolver or self._dnsaddr_resolver,
    ignore_source_errors = options.ignore_discovery_errors,
    dialable_only = true,
  })
  if not candidates then
    return nil, discover_err
  end

  local result = {
    attempted = 0,
    connected = 0,
    added = 0,
    failed = 0,
    peers = {},
    errors = {},
  }

  local seen = {}
  for _, candidate in ipairs(candidates) do
    local peer_id = candidate.peer_id
    local addrs = candidate.addrs or {}
    for _, addr in ipairs(addrs) do
      local key = tostring(peer_id or "") .. "|" .. tostring(addr)
      if seen[key] then
        goto continue_addrs
      end
      seen[key] = true

      result.attempted = result.attempted + 1
      local conn, state, dial_err = self.host:dial({
        peer_id = peer_id,
        addr = addr,
      }, options.dial_opts)
      if conn then
        result.connected = result.connected + 1
        local discovered_peer = peer_id
        if not discovered_peer and state and state.remote_peer_id then
          discovered_peer = state.remote_peer_id
        end
        if not discovered_peer then
          discovered_peer = extract_peer_id_from_multiaddr(addr)
        end
        if discovered_peer then
          local require_protocol = options.require_protocol == true
          if require_protocol then
            local check_target = discovered_peer or addr
            local supported, supported_err = self:_supports_kad_protocol(check_target, options.protocol_check_opts)
            if not supported then
              result.failed = result.failed + 1
              result.errors[#result.errors + 1] = error_mod.wrap(
                "protocol",
                "peer does not support kad-dht protocol",
                supported_err,
                {
                  peer_id = discovered_peer,
                  addr = addr,
                  protocol_id = self.protocol_id,
                }
              )
              goto continue_addrs
            end
          end

          local added, add_err = self:add_peer(discovered_peer, {
            allow_replace = options.allow_replace,
          })
          if added then
            result.added = result.added + 1
          elseif add_err and not options.ignore_add_errors then
            result.failed = result.failed + 1
            result.errors[#result.errors + 1] = add_err
          end
          result.peers[#result.peers + 1] = discovered_peer
        end
      else
        result.failed = result.failed + 1
        result.errors[#result.errors + 1] = dial_err
        if options.fail_fast then
          return nil, dial_err
        end
      end

      if type(options.max_success) == "number" and options.max_success > 0 and result.connected >= options.max_success then
        return result
      end

      ::continue_addrs::
    end
  end

  return result
end

function DHT:refresh_once(opts)
  local options = opts or {}
  local now = os.time()
  local min_recheck = options.min_recheck_seconds or 60
  local max_checks = options.max_checks or self.alpha
  if type(max_checks) ~= "number" or max_checks <= 0 then
    return nil, error_mod.new("input", "max_checks must be > 0")
  end

  local peers = self.routing_table:all_peers()
  local report = {
    checked = 0,
    healthy = 0,
    removed = 0,
    skipped = 0,
    errors = {},
  }

  for _, entry in ipairs(peers) do
    if report.checked >= max_checks then
      break
    end

    local peer_id = entry.peer_id
    local health = self._peer_health[peer_id] or {}
    local stale = health.stale == true
    local last_verified = health.last_verified_at or 0
    local recent = (now - last_verified) < min_recheck
    if not stale and recent then
      report.skipped = report.skipped + 1
      goto continue_peers
    end

    report.checked = report.checked + 1
    local ok, check_err = self:_supports_kad_protocol(peer_id, options.protocol_check_opts)
    if ok then
      health.stale = false
      health.last_verified_at = now
      self._peer_health[peer_id] = health
      report.healthy = report.healthy + 1
    else
      report.errors[#report.errors + 1] = check_err
      if stale then
        local removed, remove_err = self:remove_peer(peer_id)
        if removed then
          report.removed = report.removed + 1
        elseif remove_err then
          report.errors[#report.errors + 1] = remove_err
        end
      else
        health.stale = true
        health.last_disconnected_at = health.last_disconnected_at or now
        self._peer_health[peer_id] = health
      end
    end

    ::continue_peers::
  end

  return report
end

function DHT:random_walk(opts)
  local options = opts or {}
  local report = {
    queried = 0,
    responses = 0,
    failed = 0,
    added = 0,
    discovered = 0,
    errors = {},
  }

  local target_key = options.target_key or self.local_peer_id
  local query_limit = options.max_queries or self.alpha
  if type(query_limit) ~= "number" or query_limit <= 0 then
    return nil, error_mod.new("input", "max_queries must be > 0")
  end

  local peers = self.routing_table:all_peers()
  if #peers == 0 and options.bootstrap_if_empty then
    local bootstrap_report, bootstrap_err = self:bootstrap({
      peer_discovery = options.peer_discovery,
      dnsaddr_resolver = options.dnsaddr_resolver,
      ignore_discovery_errors = options.ignore_discovery_errors,
      allow_replace = options.allow_replace,
      fail_fast = options.fail_fast,
      max_success = options.max_success,
    })
    if not bootstrap_report then
      return nil, bootstrap_err
    end
    peers = self.routing_table:all_peers()
  end

  local queried = 0
  for _, entry in ipairs(peers) do
    if queried >= query_limit then
      break
    end

    queried = queried + 1
    report.queried = report.queried + 1

    local closest, closest_err = self:find_node(entry.peer_id, target_key, options.find_node_opts)
    if not closest then
      report.failed = report.failed + 1
      report.errors[#report.errors + 1] = closest_err
      if options.fail_fast then
        return nil, closest_err
      end
      goto continue_queries
    end

    report.responses = report.responses + 1
    report.discovered = report.discovered + #closest

    for _, candidate in ipairs(closest) do
      if candidate.peer_id then
        local added, add_err = self:add_peer(candidate.peer_id, {
          allow_replace = options.allow_replace,
        })
        if added then
          report.added = report.added + 1
        elseif add_err and not options.ignore_add_errors then
          report.errors[#report.errors + 1] = add_err
        end
      end
    end

    ::continue_queries::
  end

  return report
end

function M.new(host, opts)
  local options = opts or {}

  local local_peer_id = options.local_peer_id or options.localPeerId
  if not local_peer_id and host and type(host.peer_id) == "function" then
    local p = host:peer_id()
    local_peer_id = p and p.id
  end
  if type(local_peer_id) ~= "string" or local_peer_id == "" then
    return nil, error_mod.new("input", "kad-dht needs local peer id or host with peer_id()")
  end

  local rt, rt_err
  if options.routing_table then
    rt = options.routing_table
  else
    rt, rt_err = kbucket.new({
      local_peer_id = local_peer_id,
      bucket_size = options.k or options.bucket_size or M.DEFAULT_K,
      hash_function = options.hash_function,
    })
    if not rt then
      return nil, rt_err
    end
  end

  local self_obj = setmetatable({
    host = host,
    local_peer_id = local_peer_id,
    protocol_id = options.protocol_id or M.PROTOCOL_ID,
    k = options.k or M.DEFAULT_K,
    alpha = options.alpha or M.DEFAULT_ALPHA,
    bootstrappers = options.bootstrappers,
    peer_discovery = options.peer_discovery,
    routing_table = rt,
    _peer_health = {},
    _dnsaddr_resolver = options.dnsaddr_resolver,
    _host_on_connected = nil,
    _host_on_disconnected = nil,
    _running = false,
  }, DHT)

  if host and type(host.on) == "function" then
    self_obj._host_on_connected = function(payload)
      return self_obj:_on_peer_connected(payload)
    end
    self_obj._host_on_disconnected = function(payload)
      return self_obj:_on_peer_disconnected(payload)
    end
    local ok, on_err = host:on("peer_connected", self_obj._host_on_connected)
    if not ok then
      return nil, on_err
    end
    ok, on_err = host:on("peer_disconnected", self_obj._host_on_disconnected)
    if not ok then
      if type(host.off) == "function" then
        host:off("peer_connected", self_obj._host_on_connected)
      end
      return nil, on_err
    end
  end

  if not self_obj.peer_discovery and (self_obj.bootstrappers or self_obj._dnsaddr_resolver) then
    local default_discovery, discovery_err = M.default_peer_discovery({
      bootstrappers = self_obj.bootstrappers or M.DEFAULT_BOOTSTRAPPERS,
      dnsaddr_resolver = self_obj._dnsaddr_resolver,
      ignore_resolve_errors = true,
      dialable_only = true,
    })
    if not default_discovery then
      return nil, discovery_err
    end
    self_obj.peer_discovery = default_discovery
  end

  return self_obj
end

M.DHT = DHT
M.dnsaddr = dnsaddr
M.discovery = discovery
M.protocol = protocol

return M
