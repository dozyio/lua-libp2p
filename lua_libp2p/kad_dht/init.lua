--- Kademlia DHT service and client operations.
--
-- The default profile targets the public libp2p DHT: `k = 20`,
-- `alpha = 10`, `disjoint_paths = 10`, and `max_concurrent_queries = 32`.
-- DHT messages use `lua_libp2p.network.MESSAGE_SIZE_MAX` by default, matching
-- the practical 4 MiB KAD RPC cap used by Go and JS implementations.
--
-- Address filtering is separate from transport dialability. The built-in modes
-- are `public`, `private`, and `all`; callers may also pass a function for
-- custom policy. Provider records default to a 48 hour TTL, provider addresses
-- learned from provider records default to 24 hours, and background reprovide
-- runs every 22 hours when enabled.
--
-- The DHT starts in client mode by default. Set `mode = "server"` or enable
-- server-mode behavior explicitly when the node should serve and advertise KAD.
---@class Libp2pKadDhtConfig
---@field local_peer_id? string Override local peer ID; normally derived from host.
---@field protocol_id? string DHT protocol ID override.
---@field mode? 'client'|'server'|'auto' DHT mode. Default: `client`.
---@field k? integer Bucket size / result count. Default: module default.
---@field bucket_size? integer Alias for `k` when creating routing table.
---@field alpha? integer Query parallelism. Default: module default.
---@field disjoint_paths? integer Number of disjoint query paths.
---@field max_concurrent_queries? integer
---@field max_message_size? integer
---@field bootstrappers? string[] Bootstrap peer multiaddrs.
---@field peer_discovery? table Peer discovery source.
---@field datastore? table Shared datastore for provider/record stores.
---@field provider_store? table Prebuilt provider store.
---@field provider_datastore? table Provider datastore override.
---@field provider_ttl_seconds? number Provider record TTL.
---@field provider_addr_ttl_seconds? number Provider address TTL.
---@field record_store? table Prebuilt record store.
---@field record_datastore? table Record datastore override.
---@field record_ttl_seconds? number Value record TTL.
---@field record_validator? function
---@field record_selector? function
---@field record_validators? table
---@field record_selectors? table
---@field routing_table? table Prebuilt routing table.
---@field hash_function? function Routing table hash function.
---@field address_filter? string|function|table Address filter or filter mode.
---@field address_filter_mode? string Alias for `address_filter`.
---@field query_filter? function
---@field routing_table_filter? function
---@field peer_diversity_filter? function
---@field peer_diversity_max_peers_per_ip_group? integer|false
---@field peer_diversity_max_peers_per_ip_group_per_bucket? integer|false
---@field populate_from_peerstore_on_start? boolean Default: true.
---@field populate_from_peerstore_limit? integer Default: 1000.
---@field populate_from_peerstore_protocol_check_timeout? number
---@field dnsaddr_resolver? function
---@field auto_server_mode? boolean Enable automatic server-mode advertisement.
---@field maintenance_enabled? boolean Default: true.
---@field maintenance_interval_seconds? number
---@field maintenance_startup_retry_seconds? number
---@field maintenance_startup_retry_max_seconds? number
---@field maintenance_min_recheck_seconds? number
---@field maintenance_max_checks? integer
---@field maintenance_walk_every? integer
---@field maintenance_walk_timeout? number
---@field maintenance_protocol_check_timeout? number
---@field max_failed_checks_before_evict? integer
---@field reprovider_enabled? boolean
---@field reprovider_interval_seconds? number
---@field reprovider_initial_delay_seconds? number
---@field reprovider_jitter_seconds? number
---@field reprovider_random? function
---@field reprovider_batch_size? integer
---@field reprovider_max_parallel? integer
---@field reprovider_timeout? number
---@field peer_id_bytes_cache_size? integer
---@field filtered_addr_cache_size? integer
---@field filtered_addr_cache_ttl_seconds? number
---@field now? fun(): number

local error_mod = require("lua_libp2p.error")
local bootstrap = require("lua_libp2p.bootstrap")
local bootstrap_dht = require("lua_libp2p.kad_dht.bootstrap")
local dnsaddr = require("lua_libp2p.dnsaddr")
local discovery = require("lua_libp2p.discovery")
local kbucket = require("lua_libp2p.kad_dht.kbucket")
local maintenance = require("lua_libp2p.kad_dht.maintenance")
local multiaddr = require("lua_libp2p.multiformats.multiaddr")
local operation = require("lua_libp2p.operation")
local peerid = require("lua_libp2p.peerid")
local providers = require("lua_libp2p.kad_dht.providers")
local provider_routing = require("lua_libp2p.kad_dht.provider_routing")
local query = require("lua_libp2p.kad_dht.query")
local random_walk = require("lua_libp2p.kad_dht.random_walk")
local records = require("lua_libp2p.kad_dht.records")
local record_validators = require("lua_libp2p.kad_dht.record_validators")
local reprovider = require("lua_libp2p.kad_dht.reprovider")
local values = require("lua_libp2p.kad_dht.values")
local protocol = require("lua_libp2p.kad_dht.protocol")
local log = require("lua_libp2p.log").subsystem("kad_dht")
local table_utils = require("lua_libp2p.util.tables")

local copy_list = table_utils.copy_list

local M = {}
M.provides = { "peer_routing", "content_routing", "value_routing", "kad_dht" }
M.requires = { "identify", "ping" }

M.PROTOCOL_ID = "/ipfs/kad/1.0.0"
M.DEFAULT_K = 20
M.DEFAULT_ALPHA = 10
M.DEFAULT_DISJOINT_PATHS = 10
M.DEFAULT_MAX_CONCURRENT_QUERIES = 32
M.DEFAULT_ADDRESS_FILTER = "public"
M.DEFAULT_MAINTENANCE_ENABLED = maintenance.DEFAULT_ENABLED
M.DEFAULT_MAINTENANCE_INTERVAL_SECONDS = maintenance.DEFAULT_INTERVAL_SECONDS
M.DEFAULT_MAINTENANCE_STARTUP_RETRY_SECONDS = maintenance.DEFAULT_STARTUP_RETRY_SECONDS
M.DEFAULT_MAINTENANCE_STARTUP_RETRY_MAX_SECONDS = maintenance.DEFAULT_STARTUP_RETRY_MAX_SECONDS
M.DEFAULT_MAINTENANCE_MIN_RECHECK_SECONDS = maintenance.DEFAULT_MIN_RECHECK_SECONDS
M.DEFAULT_MAINTENANCE_MAX_CHECKS = maintenance.DEFAULT_MAX_CHECKS
M.DEFAULT_MAINTENANCE_WALK_EVERY = maintenance.DEFAULT_WALK_EVERY
M.DEFAULT_MAX_FAILED_CHECKS_BEFORE_EVICT = maintenance.DEFAULT_MAX_FAILED_CHECKS_BEFORE_EVICT
M.DEFAULT_PROVIDER_TTL_SECONDS = providers.DEFAULT_TTL_SECONDS
M.DEFAULT_PROVIDER_ADDR_TTL_SECONDS = 24 * 60 * 60
M.DEFAULT_RECORD_TTL_SECONDS = records.DEFAULT_TTL_SECONDS
M.DEFAULT_REPROVIDER_ENABLED = reprovider.DEFAULT_ENABLED
M.DEFAULT_REPROVIDER_INTERVAL_SECONDS = reprovider.DEFAULT_INTERVAL_SECONDS
M.DEFAULT_REPROVIDER_INITIAL_DELAY_SECONDS = reprovider.DEFAULT_INITIAL_DELAY_SECONDS
M.DEFAULT_REPROVIDER_JITTER_SECONDS = reprovider.DEFAULT_JITTER_SECONDS
M.DEFAULT_REPROVIDER_BATCH_SIZE = reprovider.DEFAULT_BATCH_SIZE
M.DEFAULT_REPROVIDER_MAX_PARALLEL = reprovider.DEFAULT_MAX_PARALLEL
M.DEFAULT_REPROVIDER_TIMEOUT = reprovider.DEFAULT_TIMEOUT
M.DEFAULT_PEER_ID_BYTES_CACHE_SIZE = 4096
M.DEFAULT_FILTERED_ADDR_CACHE_SIZE = 4096
M.DEFAULT_FILTERED_ADDR_CACHE_TTL_SECONDS = 2
M.DEFAULT_POPULATE_FROM_PEERSTORE_ON_START = true
M.DEFAULT_POPULATE_FROM_PEERSTORE_LIMIT = 1000
M.DEFAULT_POPULATE_FROM_PEERSTORE_PROTOCOL_CHECK_TIMEOUT = 10
M.KAD_PEER_TAG_NAME = "kad-peer"
M.KAD_PEER_TAG_VALUE = 1
M.MODE_CLIENT = "client"
M.MODE_SERVER = "server"
M.MODE_AUTO = "auto"

local function now_seconds()
  local ok_socket, socket = pcall(require, "socket")
  if ok_socket and type(socket.gettime) == "function" then
    return socket.gettime()
  end
  return os.time()
end

local function debug_perf(dht)
  return dht and dht.host and rawget(dht.host, "_debug_perf") or nil
end

local function debug_perf_add(dht, key, elapsed_seconds)
  local perf = debug_perf(dht)
  if type(perf) ~= "table" then
    return
  end
  perf[key .. "_calls"] = (perf[key .. "_calls"] or 0) + 1
  perf[key .. "_ms"] = (perf[key .. "_ms"] or 0) + (elapsed_seconds * 1000)
end

local function debug_perf_add_group(dht, group_key, item_key, elapsed_seconds)
  local perf = debug_perf(dht)
  if type(perf) ~= "table" then
    return
  end
  local group = perf[group_key]
  if type(group) ~= "table" then
    group = {}
    perf[group_key] = group
  end
  local key = tostring(item_key or "unknown")
  local item = group[key]
  if type(item) ~= "table" then
    item = { calls = 0, ms = 0 }
    group[key] = item
  end
  item.calls = (item.calls or 0) + 1
  item.ms = (item.ms or 0) + (elapsed_seconds * 1000)
end

--- Create default bootstrap peer discovery source.
-- `opts.bootstrappers` (`table<string>`) overrides bootstrap list.
-- `opts.dnsaddr_resolver` (`function`) resolves dnsaddr entries.
-- `opts.dialable_only` (`boolean`) filters to dialable addresses.
-- `opts.ignore_resolve_errors` (`boolean`) tolerates resolver failures.
function M.default_peer_discovery(opts)
  return bootstrap_dht.default_peer_discovery(opts)
end

--- Resolve bootstrap addresses (including dnsaddr where configured).
-- `opts.dnsaddr_resolver` (`function`) resolves dnsaddr entries.
-- `opts.ignore_resolve_errors` (`boolean`) tolerates resolver failures.
function M.resolve_bootstrap_addrs(addrs, opts)
  return bootstrap_dht.resolve_bootstrap_addrs(addrs, opts)
end

local function has_public_direct_addr(addrs)
  for _, addr in ipairs(addrs or {}) do
    if
      type(addr) == "string"
      and addr ~= ""
      and not multiaddr.is_private_addr(addr)
      and not multiaddr.is_relay_addr(addr)
    then
      return true
    end
  end
  return false
end

local function list_contains(list, value)
  for _, item in ipairs(list or {}) do
    if item == value then
      return true
    end
  end
  return false
end

local DHT = {}
DHT.__index = DHT

--- Start KAD service hooks and optional maintenance.
--- true|nil ok
--- table|nil err
function DHT:start()
  if self._running then
    return true
  end

  if self.mode == "server" then
    local ok, err = self:_register_handler()
    if not ok then
      return nil, err
    end
  end

  self._running = true

  local peerstore_ok, peerstore_err = self:_start_peerstore_population()
  if not peerstore_ok then
    return nil, peerstore_err
  end

  local maintenance_ok, maintenance_err = maintenance.start(self)
  if not maintenance_ok then
    return nil, maintenance_err
  end

  local reprovider_ok, reprovider_err = reprovider.start(self)
  if not reprovider_ok then
    return nil, reprovider_err
  end

  return true
end

function DHT:_register_handler()
  if self._handler_registered then
    return true
  end
  if not (self.host and type(self.host.handle) == "function") then
    return true
  end
  local ok, err = self.host:handle(self.protocol_id, function(stream, ctx)
    return self:_handle_rpc(stream, ctx)
  end)
  if not ok then
    return nil, err
  end
  self._handler_registered = true
  return true
end

function DHT:_unregister_handler()
  if not self._handler_registered then
    return true
  end
  if self.host and type(self.host.unhandle) == "function" then
    local removed, err = self.host:unhandle(self.protocol_id)
    if removed == nil then
      return nil, err
    end
  end
  self._handler_registered = false
  return true
end

function DHT:set_mode(mode, opts)
  if mode ~= "client" and mode ~= "server" then
    return nil, error_mod.new("input", "kad dht mode must be client or server")
  end
  if self.mode == mode then
    return true
  end
  local options = opts or {}
  local old_mode = self.mode
  self.mode = mode
  if self._running then
    local ok, err
    if mode == "server" then
      ok, err = self:_register_handler()
    else
      ok, err = self:_unregister_handler()
    end
    if not ok then
      self.mode = old_mode
      return nil, err
    end
  end
  if self.host and type(self.host.emit) == "function" then
    local emit_ok, emit_err = self.host:emit("kad_dht:mode_changed", {
      old_mode = old_mode,
      mode = mode,
      reason = options.reason or "set_mode",
      auto = options.auto == true,
    })
    if not emit_ok then
      return nil, emit_err
    end
  end
  return true
end

function DHT:get_mode()
  return self.mode
end

function DHT:_on_self_peer_update(payload)
  if not self._auto_server_mode then
    return true
  end
  if has_public_direct_addr(payload and payload.addrs) then
    return self:set_mode("server", { reason = "public_self_address", auto = true })
  end
  return self:set_mode("client", { reason = "no_public_self_address", auto = true })
end

--- Stop KAD service activity.
--- true
function DHT:stop()
  if self.host and type(self.host.off) == "function" then
    if self._host_on_protocols_updated then
      self.host:off("peer_protocols_updated", self._host_on_protocols_updated)
    end
    if self._host_on_self_peer_update then
      self.host:off("self_peer_update", self._host_on_self_peer_update)
    end
  end
  self:_unregister_handler()
  if self._peerstore_populate_task and self.host and type(self.host.cancel_task) == "function" then
    self.host:cancel_task(self._peerstore_populate_task.id)
  end
  self._peerstore_populate_task = nil
  maintenance.stop(self)
  reprovider.stop(self)
  self._running = false
  return true
end

function DHT:is_running()
  return self._running
end

local function addr_ip_group(addr)
  local parsed = multiaddr.parse(addr)
  if not parsed then
    return nil
  end
  local legacy_class_a = {
    [12] = true,
    [17] = true,
    [19] = true,
    [38] = true,
    [48] = true,
    [53] = true,
    [56] = true,
    [73] = true,
  }
  for _, component in ipairs(parsed.components or {}) do
    if component.protocol == "ip4" then
      local a, b = tostring(component.value or ""):match("^(%d+)%.(%d+)%.(%d+)%.(%d+)$")
      if a and b then
        local first = tonumber(a)
        if legacy_class_a[first] then
          return "ip4:" .. tostring(first) .. ".0.0.0/8"
        end
        return "ip4:" .. tostring(first) .. "." .. tostring(tonumber(b)) .. ".0.0/16"
      end
    elseif component.protocol == "ip6" then
      local groups = {}
      local value = tostring(component.value or ""):lower():gsub("%%.+$", "")
      if value:find("::", 1, true) then
        local left, right = value:match("^(.-)::(.-)$")
        local left_groups = {}
        local right_groups = {}
        for group in tostring(left or ""):gmatch("[^:]+") do
          left_groups[#left_groups + 1] = group
        end
        for group in tostring(right or ""):gmatch("[^:]+") do
          right_groups[#right_groups + 1] = group
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
        for group in value:gmatch("[^:]+") do
          groups[#groups + 1] = group
        end
      end
      if groups[1] and groups[2] then
        return "ip6:" .. string.format("%x:%x::/32", tonumber(groups[1], 16) or 0, tonumber(groups[2], 16) or 0)
      end
    end
  end
  return nil
end

local function peer_ip_groups(addrs)
  local seen = {}
  local out = {}
  for _, addr in ipairs(addrs or {}) do
    local group = addr_ip_group(addr)
    if group and not seen[group] then
      seen[group] = true
      out[#out + 1] = group
    end
  end
  return out
end

function DHT:_peer_addrs(peer_id, opts)
  local options = opts or {}
  if type(options.addrs) == "table" then
    return options.addrs
  end
  if self.host and self.host.peerstore and type(self.host.peerstore.get_addrs) == "function" then
    return self.host.peerstore:get_addrs(peer_id) or {}
  end
  return {}
end

function DHT:_check_ip_group_diversity(peer_id, opts)
  local max_global = self.peer_diversity_max_peers_per_ip_group
  local max_per_bucket = self.peer_diversity_max_peers_per_ip_group_per_bucket
  if (not max_global or max_global <= 0) and (not max_per_bucket or max_per_bucket <= 0) then
    return true
  end
  local groups = peer_ip_groups(self:_peer_addrs(peer_id, opts))
  if #groups == 0 then
    return true
  end
  local wanted = {}
  for _, group in ipairs(groups) do
    wanted[group] = true
  end
  local counts = {}
  local bucket_counts = {}
  local candidate_bucket, candidate_bucket_err = self.routing_table:bucket_for_peer(peer_id)
  if not candidate_bucket and candidate_bucket_err then
    return nil, candidate_bucket_err
  end
  for _, peer in ipairs(self.routing_table:all_peers()) do
    if peer.peer_id ~= peer_id then
      for _, group in ipairs(peer_ip_groups(self:_peer_addrs(peer.peer_id))) do
        if wanted[group] then
          counts[group] = (counts[group] or 0) + 1
          if candidate_bucket and peer.bucket == candidate_bucket then
            bucket_counts[group] = (bucket_counts[group] or 0) + 1
          end
        end
      end
    end
  end
  for group in pairs(wanted) do
    if max_global and max_global > 0 and (counts[group] or 0) >= max_global then
      return nil,
        error_mod.new("filtered", "peer rejected by ip group diversity limit", {
          peer_id = peer_id,
          ip_group = group,
          max_peers_per_ip_group = max_global,
        })
    end
    if max_per_bucket and max_per_bucket > 0 and (bucket_counts[group] or 0) >= max_per_bucket then
      return nil,
        error_mod.new("filtered", "peer rejected by bucket ip group diversity limit", {
          peer_id = peer_id,
          ip_group = group,
          bucket = candidate_bucket,
          max_peers_per_ip_group_per_bucket = max_per_bucket,
        })
    end
  end
  return true
end

--- Add a peer to the routing table.
-- `opts` is forwarded to kbucket insertion policy.
--- peer_id string
--- opts? table
--- true|nil added
--- table|nil err
function DHT:add_peer(peer_id, opts)
  local options = opts or {}
  if
    options.skip_kad_protocol_filter ~= true
    and self.host
    and self.host.peerstore
    and type(self.host.peerstore.get_protocols) == "function"
  then
    local protocols = self.host.peerstore:get_protocols(peer_id)
    if type(protocols) == "table" and #protocols > 0 and not self:_peerstore_supports_kad(peer_id) then
      return nil,
        error_mod.new("filtered", "peer rejected because it does not support kad-dht", {
          peer_id = peer_id,
          protocol_id = self.protocol_id,
        })
    end
  end
  local diversity_ok, diversity_err = self:_check_ip_group_diversity(peer_id, options)
  if not diversity_ok then
    return nil, diversity_err
  end
  local routing_filter = options.routing_table_filter or self.routing_table_filter
  if routing_filter ~= nil then
    if type(routing_filter) ~= "function" then
      return nil, error_mod.new("input", "kad-dht routing_table_filter must be a function")
    end
    local ok, allowed_or_err, filter_err = pcall(routing_filter, {
      peer_id = peer_id,
    }, {
      dht = self,
      opts = options,
    })
    if not ok then
      return nil, error_mod.new("protocol", "kad-dht routing_table_filter failed", { cause = allowed_or_err })
    end
    if allowed_or_err == nil and filter_err ~= nil then
      return nil, filter_err
    end
    if allowed_or_err == false then
      return nil, error_mod.new("filtered", "peer rejected by routing table filter", { peer_id = peer_id })
    end
  end
  local diversity_filter = options.peer_diversity_filter or self.peer_diversity_filter
  if diversity_filter ~= nil then
    if type(diversity_filter) ~= "function" then
      return nil, error_mod.new("input", "kad-dht peer_diversity_filter must be a function")
    end
    local ok, allowed_or_err, filter_err = pcall(diversity_filter, {
      peer_id = peer_id,
    }, {
      dht = self,
      opts = options,
      peers = self.routing_table:all_peers(),
      k = self.k,
    })
    if not ok then
      return nil, error_mod.new("protocol", "kad-dht peer_diversity_filter failed", { cause = allowed_or_err })
    end
    if allowed_or_err == nil and filter_err ~= nil then
      return nil, filter_err
    end
    if allowed_or_err == false then
      return nil, error_mod.new("filtered", "peer rejected by diversity filter", { peer_id = peer_id })
    end
  end
  local had_routing_peers = #(self.routing_table:all_peers() or {}) > 0
  local added, err = self.routing_table:try_add_peer(peer_id, options)
  if added and type(peer_id) == "string" and peer_id ~= "" then
    local now = os.time()
    self._peer_health[peer_id] = self._peer_health[peer_id] or {}
    self._peer_health[peer_id].stale = false
    self._peer_health[peer_id].failed_checks = 0
    self._peer_health[peer_id].last_connected_at = self._peer_health[peer_id].last_connected_at or now
    log.debug("kad dht peer added", {
      peer_id = peer_id,
    })
    self:_tag_kad_peer(peer_id)
    if not had_routing_peers then
      maintenance.trigger(self)
    end
  elseif err then
    log.debug("kad dht peer rejected", {
      peer_id = tostring(peer_id),
      cause = tostring(err),
    })
  end
  return added, err
end

function DHT:_peerstore_supports_kad(peer_id)
  return self.host
    and self.host.peerstore
    and type(self.host.peerstore.supports_protocol) == "function"
    and self.host.peerstore:supports_protocol(peer_id, self.protocol_id) == true
end

function DHT:_tag_kad_peer(peer_id)
  if
    type(peer_id) == "string"
    and peer_id ~= ""
    and self.host
    and self.host.peerstore
    and type(self.host.peerstore.tag) == "function"
  then
    self.host.peerstore:tag(peer_id, M.KAD_PEER_TAG_NAME, {
      value = M.KAD_PEER_TAG_VALUE,
      ttl = math.huge,
    })
  end
end

function DHT:_untag_kad_peer(peer_id)
  if
    type(peer_id) == "string"
    and peer_id ~= ""
    and self.host
    and self.host.peerstore
    and type(self.host.peerstore.untag) == "function"
  then
    self.host.peerstore:untag(peer_id, M.KAD_PEER_TAG_NAME)
  end
end

function DHT:_record_kad_peer(peer_id, addrs)
  if type(peer_id) ~= "string" or peer_id == "" then
    return nil
  end
  if self.host and self.host.peerstore then
    self.host.peerstore:merge(peer_id, {
      addrs = addrs or {},
      protocols = { self.protocol_id },
    })
  end
  local added, add_err = self:add_peer(peer_id)
  if added then
    self:_tag_kad_peer(peer_id)
  end
  return added, add_err
end

function DHT:_populate_routing_table_from_peerstore(opts)
  local options = opts or {}
  local ps = self.host and self.host.peerstore
  if not (ps and type(ps.all) == "function") then
    return { scanned = 0, candidates = 0, added = 0, skipped = 0, failed = 0, errors = {} }
  end

  local peers, peers_err = ps:all()
  if not peers then
    return nil, peers_err
  end

  local limit = options.limit or self._populate_from_peerstore_limit or M.DEFAULT_POPULATE_FROM_PEERSTORE_LIMIT
  local report = { scanned = 0, candidates = 0, added = 0, skipped = 0, failed = 0, errors = {} }
  local local_peer_id = self.local_peer_id
  for _, peer in ipairs(peers) do
    if report.candidates >= limit then
      break
    end
    report.scanned = report.scanned + 1

    local peer_id = peer and peer.peer_id
    if type(peer_id) ~= "string" or peer_id == "" or peer_id == local_peer_id then
      report.skipped = report.skipped + 1
      goto continue_peer
    end

    local tags = peer.tags or {}
    if tags[M.KAD_PEER_TAG_NAME] == nil then
      report.skipped = report.skipped + 1
      goto continue_peer
    end

    if not list_contains(peer.protocols or {}, self.protocol_id) then
      report.skipped = report.skipped + 1
      goto continue_peer
    end

    local addrs = peer.addrs
    if (type(addrs) ~= "table" or #addrs == 0) and type(ps.get_addrs) == "function" then
      addrs = ps:get_addrs(peer_id)
    end
    addrs = self:_filter_addrs(addrs or {}, { peer_id = peer_id, purpose = "peerstore_warm_start" })
    local dialable_addrs, dialable_err = self:_dialable_tcp_addrs(addrs)
    if not dialable_addrs then
      report.failed = report.failed + 1
      report.errors[#report.errors + 1] = dialable_err
      goto continue_peer
    end
    if #dialable_addrs == 0 then
      report.skipped = report.skipped + 1
      goto continue_peer
    end

    report.candidates = report.candidates + 1
    local supported, support_err = self:_supports_kad_protocol(peer_id, {
      ctx = options.ctx,
      timeout = options.protocol_check_timeout
        or self._populate_from_peerstore_protocol_check_timeout
        or M.DEFAULT_POPULATE_FROM_PEERSTORE_PROTOCOL_CHECK_TIMEOUT,
    })
    if not supported then
      self:_untag_kad_peer(peer_id)
      report.failed = report.failed + 1
      report.errors[#report.errors + 1] = support_err
      goto continue_peer
    end

    local added, add_err = self:add_peer(peer_id, {
      addrs = dialable_addrs,
      allow_replace = true,
    })
    if added then
      report.added = report.added + 1
    elseif add_err then
      self:_untag_kad_peer(peer_id)
      report.failed = report.failed + 1
      report.errors[#report.errors + 1] = add_err
    else
      report.skipped = report.skipped + 1
    end

    ::continue_peer::
  end

  return report
end

function DHT:_start_peerstore_population()
  if not self._populate_from_peerstore_on_start then
    return true
  end
  if not (self.host and type(self.host.spawn_task) == "function") then
    return true
  end
  if not (self.host.peerstore and type(self.host.peerstore.all) == "function") then
    return true
  end
  if self._peerstore_populate_task then
    return true
  end

  local task, task_err = self.host:spawn_task("kad.peerstore_populate", function(ctx)
    local report, report_err = self:_populate_routing_table_from_peerstore({
      ctx = ctx,
      limit = self._populate_from_peerstore_limit,
      protocol_check_timeout = self._populate_from_peerstore_protocol_check_timeout,
    })
    if not report then
      log.debug("kad dht peerstore warm start failed", { cause = tostring(report_err) })
      return true
    end
    log.debug("kad dht peerstore warm start completed", report)
    if self.host and type(self.host.emit) == "function" then
      self.host:emit("kad_dht:peerstore_populate", report)
    end
    return true
  end, { service = "kad_dht" })
  if not task then
    return nil, task_err
  end
  self._peerstore_populate_task = task
  return true
end

local function rpc_target_peer_id(peer_or_addr)
  if type(peer_or_addr) == "table" then
    return peer_or_addr.peer_id
  end
  if type(peer_or_addr) == "string" and peer_or_addr:sub(1, 1) ~= "/" then
    return peer_or_addr
  end
  return nil
end

local function is_orderly_no_response_close(err)
  if not err then
    return false
  end
  local kind = type(err) == "table" and err.kind or nil
  local message = type(err) == "table" and err.message or tostring(err)
  return kind == "closed" and message:find("closed during read", 1, true) ~= nil
end

--- Remove a peer from routing table and health tracking.
--- peer_id string
--- boolean removed
function DHT:remove_peer(peer_id)
  self._peer_health[peer_id] = nil
  local removed = self.routing_table:remove_peer(peer_id)
  if removed then
    self:_untag_kad_peer(peer_id)
  end
  return removed
end

--- Lookup a peer in local routing table.
--- peer_id string
--- table|nil peer
function DHT:get_local_peer(peer_id)
  return self.routing_table:find_peer(peer_id)
end

--- Network peer-routing lookup with optional local routing-table cache.
-- Returns a report containing `peer` when found, plus closest peers/lookup data.
-- `opts.use_cache=false` forces a network lookup even when the peer is local.
-- `opts.use_network=false` checks only the local cache.
--- peer_id string Peer id to find.
--- opts? table Lookup controls.
--- table|nil report
--- table|nil err
function DHT:_find_peer_network(peer_id, opts)
  local options = opts or {}
  if type(peer_id) ~= "string" or peer_id == "" then
    return nil, error_mod.new("input", "peer_id must be non-empty")
  end
  if options.use_cache ~= false then
    local cached = self:get_local_peer(peer_id)
    if cached then
      local addrs = cached.addrs
      if addrs == nil and self.host and self.host.peerstore then
        addrs = self.host.peerstore:get_addrs(peer_id)
      end
      return {
        peer = {
          peer_id = peer_id,
          addrs = addrs or {},
        },
        closest_peers = {},
        lookup = { target = peer_id, queried = {}, closest_peers = {}, errors = {}, termination = "local_peer" },
      }
    end
  end
  if options.use_network == false then
    return {
      peer = nil,
      closest_peers = {},
      lookup = { target = peer_id, queried = {}, closest_peers = {}, errors = {}, termination = "network_disabled" },
    }
  end

  local peers, lookup = self:_get_closest_peers(peer_id, options)
  if not peers then
    return nil, lookup
  end
  for _, peer in ipairs(peers) do
    if peer.peer_id == peer_id then
      return {
        peer = peer,
        closest_peers = peers,
        lookup = lookup,
      }
    end
  end
  return {
    peer = nil,
    closest_peers = peers,
    lookup = lookup,
  }
end

--- Find a peer through peer routing, using local cache by default.
-- Pass `use_cache=false` to force a network lookup. For local-only routing
-- table lookup use @{get_local_peer}. Pass `use_network=false` to avoid
-- network lookup after a cache miss.
--- table|nil op
--- table|nil err
function DHT:find_peer(peer_id, opts)
  return self:find_peer_network(peer_id, opts)
end

--- Find closest peers from local routing table.
--- key string Target key or peer id bytes/text.
--- count? number Max peers.
--- table peers
function DHT:find_closest_peers(key, count)
  local want = count or self.k
  return self.routing_table:nearest_peers(key, want)
end

--- Store a provider record locally.
--- key string Content key/CID multihash bytes.
--- peer_info table Provider peer info (`peer_id`/`id`, `addrs`).
--- opts? table `ttl` or `ttl_seconds`.
--- true|nil ok
--- table|nil err
function DHT:add_provider(key, peer_info, opts)
  local valid_key, key_err = provider_routing.validate_provider_key(key, "provider")
  if not valid_key then
    return nil, key_err
  end
  return self.provider_store:add(key, peer_info, opts)
end

--- Return locally stored, non-expired providers for a key.
--- key string Content key/CID multihash bytes.
--- opts? table `limit`.
--- table|nil providers
--- table|nil err
function DHT:get_local_providers(key, opts)
  local valid_key, key_err = provider_routing.validate_provider_key(key, "provider")
  if not valid_key then
    return nil, key_err
  end
  return self.provider_store:get(key, opts)
end

--- Return provider keys where this host is the local provider.
--- opts? table `limit`.
--- table|nil keys
--- table|nil err
function DHT:list_local_provider_keys(opts)
  local options = opts or {}
  local list_opts = {}
  for k, v in pairs(options) do
    list_opts[k] = v
  end
  list_opts.peer_id = self.local_peer_id
  return self.provider_store:list_keys(list_opts)
end

--- Register or clear a value record validator for a key namespace.
-- Namespace is the first path segment, e.g. `/ipns/<name>` -> `ipns`.
function DHT:set_record_validator(namespace, fn)
  return values.set_record_policy(self.record_validators, namespace, fn, "record validator")
end

--- Register or clear a value record selector for a key namespace.
-- Selectors receive `(key, existing, incoming, dht)` and return `"incoming"`,
-- `"existing"`, `true` (incoming), or `false` (existing).
function DHT:set_record_selector(namespace, fn)
  return values.set_record_policy(self.record_selectors, namespace, fn, "record selector")
end

--- Store a value record locally.
--- key string Record key bytes.
--- record table KAD record (`key`, `value`, optional `time_received`).
--- opts? table `ttl` or `ttl_seconds`.
--- true|nil ok
--- table|nil err
function DHT:put_local_record(key, record, opts)
  local validated, validate_err = self:_validate_record(key, record)
  if not validated then
    return nil, validate_err
  end
  local selected, select_err = self:_select_record(key, record)
  if selected == nil then
    return nil, select_err
  end
  if selected == false then
    return true
  end
  return self.record_store:put(key, record, opts)
end

--- Return a locally stored, non-expired value record.
--- key string Record key bytes.
--- opts? table Reserved for future selectors.
--- table|nil record
--- table|nil err
function DHT:get_local_record(key, opts)
  local _ = opts
  return self.record_store:get(key)
end

function DHT:_synthesize_pk_record(key)
  return values.synthesize_pk_record(self, key)
end

function DHT:_merge_pk_record(key, record)
  return values.merge_pk_record(self, key, record)
end

function DHT:_local_value_record(key)
  return values.local_value_record(self, key)
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

local function decode_kad_multiaddrs(addrs)
  local out = {}
  for _, addr in ipairs(addrs or {}) do
    if type(addr) == "string" and addr:sub(1, 1) == "/" then
      out[#out + 1] = addr
    elseif type(addr) == "string" and addr ~= "" then
      local parsed = multiaddr.from_bytes(addr)
      if parsed and parsed.text then
        out[#out + 1] = parsed.text
      end
    end
  end
  return out
end

local function is_capacity_error(err)
  return error_mod.is_error(err) and err.kind == "capacity"
end

local function normalize_address_filter(filter)
  if filter == nil then
    return M.DEFAULT_ADDRESS_FILTER
  end
  if type(filter) == "function" then
    return filter
  end
  if filter == "public" or filter == "private" or filter == "all" then
    return filter
  end
  return nil,
    error_mod.new("input", "unsupported kad-dht address filter", {
      filter = filter,
      supported = { "public", "private", "all" },
    })
end

function DHT:_addr_allowed(addr, ctx)
  local filter = self.address_filter
  if type(filter) == "function" then
    local ok, allowed = pcall(filter, addr, ctx or {})
    if not ok then
      return false
    end
    return allowed ~= false
  end
  if filter == "all" then
    return true
  end
  if filter == "private" then
    return multiaddr.is_private_addr(addr)
  end
  return multiaddr.is_public_addr(addr)
end

function DHT:_filter_addrs(addrs, ctx)
  local out = {}
  for _, addr in ipairs(addrs or {}) do
    if self:_addr_allowed(addr, ctx) then
      out[#out + 1] = addr
    end
  end
  return out
end

function DHT:_dialable_tcp_addrs(addrs)
  if self.host and type(self.host.filter_dialable_addrs) == "function" then
    return self.host:filter_dialable_addrs(addrs)
  end
  return nil, error_mod.new("state", "kad dht host must provide filter_dialable_addrs")
end

function DHT:_is_dialable_addr(addr)
  if self.host and type(self.host.is_dialable_addr) == "function" then
    return self.host:is_dialable_addr(addr)
  end
  return nil, error_mod.new("state", "kad dht host must provide is_dialable_addr")
end

local function find_node_target_peer_id(target_key)
  local parsed = peerid.from_bytes(target_key)
  if parsed and parsed.id then
    return parsed.id
  end
  parsed = peerid.parse(target_key)
  if parsed and parsed.id then
    return parsed.id
  end
  return nil
end

function DHT:_peer_record(peer_id, purpose)
  if type(peer_id) ~= "string" or peer_id == "" then
    return nil
  end
  local phase_started = debug_perf(self) and now_seconds() or nil
  local id_bytes = self._peer_id_bytes_cache and self._peer_id_bytes_cache[peer_id] or nil
  local id_err = nil
  if not id_bytes then
    id_bytes, id_err = protocol.peer_bytes(peer_id)
    if id_bytes and self._peer_id_bytes_cache then
      self._peer_id_bytes_cache[peer_id] = id_bytes
      self._peer_id_bytes_cache_count = (self._peer_id_bytes_cache_count or 0) + 1
      if self._peer_id_bytes_cache_count > (self._peer_id_bytes_cache_size or M.DEFAULT_PEER_ID_BYTES_CACHE_SIZE) then
        self._peer_id_bytes_cache = { [peer_id] = id_bytes }
        self._peer_id_bytes_cache_count = 1
      end
    end
  end
  if phase_started then
    debug_perf_add(self, "kad_peer_record_peer_bytes", now_seconds() - phase_started)
    phase_started = now_seconds()
  end
  if not id_bytes then
    return nil, id_err
  end
  local addrs = {}
  if self.host and self.host.peerstore and type(self.host.peerstore.get_addrs) == "function" then
    local cache_key = tostring(purpose or "response") .. "\0" .. peer_id
    local cache = self._filtered_addr_cache
    local cached = cache and cache[cache_key] or nil
    local now = now_seconds()
    if cached and cached.expires_at and cached.expires_at > now then
      addrs = copy_list(cached.addrs)
      if phase_started then
        debug_perf_add(self, "kad_peer_record_addr_cache_hit", now_seconds() - phase_started)
      end
    else
      local raw_addrs = self.host.peerstore:get_addrs(peer_id)
      if phase_started then
        debug_perf_add(self, "kad_peer_record_get_addrs", now_seconds() - phase_started)
        phase_started = now_seconds()
      end
      addrs = self:_filter_addrs(raw_addrs, {
        peer_id = peer_id,
        purpose = purpose or "response",
      })
      if phase_started then
        debug_perf_add(self, "kad_peer_record_filter_addrs", now_seconds() - phase_started)
      end
      if cache and self._filtered_addr_cache_ttl_seconds and self._filtered_addr_cache_ttl_seconds > 0 then
        cache[cache_key] = {
          addrs = copy_list(addrs),
          expires_at = now + self._filtered_addr_cache_ttl_seconds,
        }
        self._filtered_addr_cache_count = (self._filtered_addr_cache_count or 0) + 1
        if self._filtered_addr_cache_count > (self._filtered_addr_cache_size or M.DEFAULT_FILTERED_ADDR_CACHE_SIZE) then
          self._filtered_addr_cache = {
            [cache_key] = {
              addrs = copy_list(addrs),
              expires_at = now + self._filtered_addr_cache_ttl_seconds,
            },
          }
          self._filtered_addr_cache_count = 1
        end
      end
    end
  end
  return {
    id = id_bytes,
    addrs = addrs,
  }
end

function DHT:_closest_peer_records(target_key, count, opts)
  local options = opts or {}
  local exclude = options.exclude or {}
  local want = tonumber(count) or self.k
  local phase_started = debug_perf(self) and now_seconds() or nil
  local nearest, nearest_err = self:find_closest_peers(target_key, want + 2)
  if phase_started then
    debug_perf_add(self, "kad_closest_lookup", now_seconds() - phase_started)
    phase_started = now_seconds()
  end
  if not nearest then
    return nil, nearest_err
  end

  local peers = {}
  local seen = {}
  local function add_peer_record(peer_id, purpose, allow_empty_addrs)
    if exclude[peer_id] or seen[peer_id] then
      return true
    end
    local record_started = debug_perf(self) and now_seconds() or nil
    local record, record_err = self:_peer_record(peer_id, purpose)
    if record_started then
      debug_perf_add(self, "kad_closest_peer_record", now_seconds() - record_started)
    end
    if not record then
      return nil, record_err
    end
    if allow_empty_addrs or #record.addrs > 0 then
      peers[#peers + 1] = record
      seen[peer_id] = true
    end
    return true
  end

  local target_peer_id = find_node_target_peer_id(target_key)
  if phase_started then
    debug_perf_add(self, "kad_closest_parse_target", now_seconds() - phase_started)
    phase_started = now_seconds()
  end
  if target_peer_id then
    local ok, err = add_peer_record(target_peer_id, "find_node_target", true)
    if not ok then
      return nil, err
    end
  end

  for _, entry in ipairs(nearest) do
    if #peers >= want then
      break
    end
    local ok, err = add_peer_record(entry.peer_id, "response", false)
    if not ok then
      return nil, err
    end
  end
  if phase_started then
    debug_perf_add(self, "kad_closest_build", now_seconds() - phase_started)
  end
  return peers
end

function DHT:_handle_find_node(req, ctx)
  local target_key = req.key
  if type(target_key) ~= "string" or target_key == "" then
    return nil, error_mod.new("input", "FIND_NODE request missing key")
  end

  local exclude = {
    [self.local_peer_id] = true,
  }
  local requester = ctx and (ctx.peer_id or (ctx.state and ctx.state.remote_peer_id))
  if requester then
    exclude[requester] = true
  end
  local peers, peers_err = self:_closest_peer_records(target_key, self.k, { exclude = exclude })
  if not peers then
    return nil, peers_err
  end

  return {
    type = protocol.MESSAGE_TYPE.FIND_NODE,
    key = target_key,
    closer_peers = peers,
  }
end

function DHT:_local_provider_info(opts)
  return provider_routing.local_provider_info(self, opts)
end

function DHT:_handle_add_provider(req)
  return provider_routing.handle_add_provider(self, req)
end

function DHT:_handle_get_providers(req)
  return provider_routing.handle_get_providers(self, req)
end

function DHT:_validate_record(key, record)
  return values.validate_record(self, key, record)
end

function DHT:_select_record(key, incoming)
  return values.select_record(self, key, incoming)
end

function DHT:_handle_put_value(req)
  return values.handle_put_value(self, req)
end

function DHT:_handle_get_value(req)
  return values.handle_get_value(self, req)
end

--- Internal ADD_PROVIDER RPC wrapper.
-- `opts` uses same transport fields as @{_rpc}.
function DHT:_add_provider(peer_or_addr, key, provider_info, opts)
  return provider_routing.add_provider(self, peer_or_addr, key, provider_info, opts)
end

local function reset_stream(stream)
  if stream and type(stream.reset) == "function" then
    return stream:reset()
  end
  return true
end

local function message_type_name(message_type)
  if message_type == protocol.MESSAGE_TYPE.FIND_NODE then
    return "FIND_NODE"
  elseif message_type == protocol.MESSAGE_TYPE.GET_VALUE then
    return "GET_VALUE"
  elseif message_type == protocol.MESSAGE_TYPE.PUT_VALUE then
    return "PUT_VALUE"
  elseif message_type == protocol.MESSAGE_TYPE.ADD_PROVIDER then
    return "ADD_PROVIDER"
  elseif message_type == protocol.MESSAGE_TYPE.GET_PROVIDERS then
    return "GET_PROVIDERS"
  elseif message_type == protocol.MESSAGE_TYPE.PING then
    return "PING"
  end
  return tostring(message_type)
end

function DHT:_handle_rpc(stream, ctx)
  local phase_started = debug_perf(self) and now_seconds() or nil
  local req, req_err = protocol.read(stream, { max_message_size = self.max_message_size })
  if phase_started then
    debug_perf_add(self, "kad_rpc_read", now_seconds() - phase_started)
    phase_started = now_seconds()
  end
  if not req then
    reset_stream(stream)
    return nil, req_err
  end

  log.debug("kad dht inbound rpc received", {
    peer_id = ctx and (ctx.peer_id or (ctx.state and ctx.state.remote_peer_id)) or nil,
    message_type = message_type_name(req.type),
    key_size = type(req.key) == "string" and #req.key or 0,
  })

  local response, response_err
  if req.type == protocol.MESSAGE_TYPE.FIND_NODE then
    response, response_err = self:_handle_find_node(req, ctx)
  elseif req.type == protocol.MESSAGE_TYPE.GET_VALUE then
    response, response_err = self:_handle_get_value(req)
  elseif req.type == protocol.MESSAGE_TYPE.PUT_VALUE then
    response, response_err = self:_handle_put_value(req)
  elseif req.type == protocol.MESSAGE_TYPE.ADD_PROVIDER then
    response, response_err = self:_handle_add_provider(req)
  elseif req.type == protocol.MESSAGE_TYPE.GET_PROVIDERS then
    response, response_err = self:_handle_get_providers(req)
  else
    reset_stream(stream)
    log.debug("kad dht inbound rpc unsupported", {
      peer_id = ctx and (ctx.peer_id or (ctx.state and ctx.state.remote_peer_id)) or nil,
      message_type = message_type_name(req.type),
    })
    return nil, error_mod.new("unsupported", "kad-dht message type is not supported", { type = req.type })
  end
  if phase_started then
    local dispatch_elapsed = now_seconds() - phase_started
    debug_perf_add(self, "kad_rpc_dispatch", dispatch_elapsed)
    debug_perf_add_group(self, "kad_rpc_dispatch_by_type", message_type_name(req.type), dispatch_elapsed)
    phase_started = now_seconds()
  end
  if not response then
    if response_err then
      reset_stream(stream)
      log.debug("kad dht inbound rpc failed", {
        peer_id = ctx and (ctx.peer_id or (ctx.state and ctx.state.remote_peer_id)) or nil,
        message_type = message_type_name(req.type),
        cause = tostring(response_err),
      })
      return nil, response_err
    end
    if type(stream.close_write) == "function" then
      local ok, close_err = stream:close_write()
      if not ok then
        return nil, close_err
      end
    end
    if phase_started then
      debug_perf_add(self, "kad_rpc_close_write", now_seconds() - phase_started)
    end
    log.debug("kad dht inbound rpc completed", {
      peer_id = ctx and (ctx.peer_id or (ctx.state and ctx.state.remote_peer_id)) or nil,
      message_type = message_type_name(req.type),
      response = "none",
    })
    return true
  end

  local wrote, write_err = protocol.write(stream, response, { max_message_size = self.max_message_size })
  if phase_started then
    debug_perf_add(self, "kad_rpc_write", now_seconds() - phase_started)
    phase_started = now_seconds()
  end
  if not wrote then
    log.debug("kad dht inbound rpc write failed", {
      peer_id = ctx and (ctx.peer_id or (ctx.state and ctx.state.remote_peer_id)) or nil,
      message_type = message_type_name(req.type),
      cause = tostring(write_err),
    })
    return nil, write_err
  end

  if type(stream.close_write) == "function" then
    local ok, close_err = stream:close_write()
    if not ok then
      return nil, close_err
    end
  end
  if phase_started then
    debug_perf_add(self, "kad_rpc_close_write", now_seconds() - phase_started)
  end

  log.debug("kad dht inbound rpc completed", {
    peer_id = ctx and (ctx.peer_id or (ctx.state and ctx.state.remote_peer_id)) or nil,
    message_type = message_type_name(req.type),
    response_type = message_type_name(response.type),
    closer_peers = type(response.closer_peers) == "table" and #response.closer_peers or 0,
    provider_peers = type(response.provider_peers) == "table" and #response.provider_peers or 0,
    has_record = response.record ~= nil,
  })

  return true
end

--- Internal FIND_NODE RPC wrapper.
-- `opts.timeout`, `opts.ctx`, `opts.stream_opts`, `opts.max_message_size`.
function DHT:_find_node(peer_or_addr, target_key, opts)
  opts = opts or {}
  if
    not opts.ctx
    and not opts._internal_task_ctx
    and self.host
    and type(self.host.spawn_task) == "function"
    and type(self.host.run_until_task) == "function"
  then
    local task, task_err = self.host:spawn_task("kad.find_node.inline", function(ctx)
      local task_opts = {}
      for k, v in pairs(opts) do
        task_opts[k] = v
      end
      task_opts.ctx = ctx
      task_opts._internal_task_ctx = true
      return self:_find_node(peer_or_addr, target_key, task_opts)
    end, { service = "kad_dht" })
    if not task then
      return nil, task_err
    end
    return self.host:run_until_task(task, {
      timeout = opts.timeout,
      poll_interval = opts.poll_interval,
    })
  end

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
  }, { max_message_size = self.max_message_size })
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

  local response, response_err = protocol.read(stream, { max_message_size = self.max_message_size })
  if not response then
    return nil, response_err
  end

  if response.type ~= protocol.MESSAGE_TYPE.FIND_NODE then
    return nil,
      error_mod.new("protocol", "unexpected kad-dht response type", {
        expected = protocol.MESSAGE_TYPE.FIND_NODE,
        got = response.type,
      })
  end

  local out = {}
  for _, peer in ipairs(response.closer_peers or {}) do
    local peer_id_text = decode_peer_id_text(peer.id)
    local addrs = self:_filter_addrs(decode_kad_multiaddrs(peer.addrs), {
      peer_id = peer_id_text,
      purpose = "query_result",
    })
    if self.host and self.host.peerstore and peer_id_text then
      self.host.peerstore:merge(peer_id_text, {
        addrs = addrs,
      })
    end
    out[#out + 1] = {
      peer_id = peer_id_text,
      id = peer.id,
      addrs = addrs,
    }
  end
  return out
end

function DHT:_decode_response_peers(response, purpose)
  local function decode_list(list)
    local out = {}
    for _, peer in ipairs(list or {}) do
      local peer_id_text = decode_peer_id_text(peer.id)
      local addrs = self:_filter_addrs(decode_kad_multiaddrs(peer.addrs), {
        peer_id = peer_id_text,
        purpose = purpose,
      })
      if self.host and self.host.peerstore and peer_id_text then
        self.host.peerstore:merge(peer_id_text, {
          addrs = addrs,
        })
      end
      out[#out + 1] = {
        peer_id = peer_id_text,
        id = peer.id,
        addrs = addrs,
      }
    end
    return out
  end
  return decode_list(response.closer_peers), decode_list(response.provider_peers)
end

--- Internal generic KAD RPC helper.
-- `opts.timeout`, `opts.ctx`, `opts.stream_opts`, `opts.max_message_size`.
function DHT:_rpc(peer_or_addr, request, expected_type, opts)
  opts = opts or {}
  if
    not opts.ctx
    and not opts._internal_task_ctx
    and self.host
    and type(self.host.spawn_task) == "function"
    and type(self.host.run_until_task) == "function"
  then
    local task, task_err = self.host:spawn_task("kad.rpc.inline", function(ctx)
      local task_opts = {}
      for k, v in pairs(opts) do
        task_opts[k] = v
      end
      task_opts.ctx = ctx
      task_opts._internal_task_ctx = true
      return self:_rpc(peer_or_addr, request, expected_type, task_opts)
    end, { service = "kad_dht" })
    if not task then
      return nil, task_err
    end
    return self.host:run_until_task(task, {
      timeout = opts.timeout,
      poll_interval = opts.poll_interval,
    })
  end

  if not self.host or type(self.host.new_stream) ~= "function" then
    return nil, error_mod.new("state", "kad rpc requires host with new_stream")
  end
  local stream, _, _, stream_err = self.host:new_stream(peer_or_addr, { self.protocol_id }, opts)
  if not stream then
    return nil, stream_err
  end
  local wrote, write_err = protocol.write(stream, request, { max_message_size = self.max_message_size })
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
  local response, response_err = protocol.read(stream, { max_message_size = self.max_message_size })
  if not response then
    if opts.add_provider and is_orderly_no_response_close(response_err) then
      local target_peer_id = rpc_target_peer_id(peer_or_addr)
      if target_peer_id then
        self:_record_kad_peer(target_peer_id, type(peer_or_addr) == "table" and peer_or_addr.addrs or nil)
      end
      return { type = expected_type, key = request.key }
    end
    return nil, response_err
  end
  if response.type ~= expected_type then
    return nil,
      error_mod.new("protocol", "unexpected kad-dht response type", {
        expected = expected_type,
        got = response.type,
      })
  end
  local target_peer_id = rpc_target_peer_id(peer_or_addr)
  if target_peer_id then
    self:_record_kad_peer(target_peer_id, type(peer_or_addr) == "table" and peer_or_addr.addrs or nil)
  end
  return response
end

--- Internal GET_VALUE RPC wrapper.
-- `opts` uses same transport fields as @{_rpc}.
-- `opts.timeout`, `opts.ctx`, `opts.stream_opts`, and `opts.max_message_size` are honored.
function DHT:_get_value(peer_or_addr, key, opts)
  return values.get_value(self, peer_or_addr, key, opts)
end

--- Internal PUT_VALUE RPC wrapper.
-- `opts` uses same transport fields as @{_rpc}.
-- `opts.timeout`, `opts.ctx`, `opts.stream_opts`, and `opts.max_message_size` are honored.
function DHT:_put_value(peer_or_addr, key, record, opts)
  return values.put_value(self, peer_or_addr, key, record, opts)
end

--- Internal GET_PROVIDERS RPC wrapper.
-- `opts` uses same transport fields as @{_rpc}.
-- `opts.timeout`, `opts.ctx`, `opts.stream_opts`, and `opts.max_message_size` are honored.
function DHT:_get_providers(peer_or_addr, key, opts)
  return provider_routing.get_providers(self, peer_or_addr, key, opts)
end

local function seed_candidates_from_routing_table(self, key, count)
  local nearest = self:find_closest_peers(key, count or self.k) or {}
  local out = {}
  for _, entry in ipairs(nearest) do
    local addrs = {}
    if self.host and self.host.peerstore then
      local filtered, filtered_err =
        self:_dialable_tcp_addrs(self:_filter_addrs(self.host.peerstore:get_addrs(entry.peer_id), {
          peer_id = entry.peer_id,
          purpose = "client_query_seed",
        }))
      if not filtered then
        return nil, filtered_err
      end
      addrs = filtered
    end
    if #addrs > 0 then
      out[#out + 1] = { peer_id = entry.peer_id, addrs = addrs }
    end
  end
  return out
end

function DHT:_sort_query_candidates(target_hash, candidates)
  return query.sort_candidates(self, target_hash, candidates)
end

function DHT:_strict_lookup_complete(target_hash, states, k)
  return query.strict_complete(self, target_hash, states, k)
end

--- Internal iterative lookup engine.
-- `opts.alpha`, `opts.k`, `opts.disjoint_paths`, `opts.query_timeout_seconds`,
-- `opts.max_query_rounds`, `opts.ctx`, and `opts.yield` tune execution.
function DHT:_run_client_lookup(key, seed_peers, query_func, opts)
  return query.run_client_lookup(self, key, seed_peers, query_func, opts)
end

--- Internal iterative FIND_VALUE.
-- `opts` forwarded to @{_run_client_lookup} and query functions.
function DHT:_find_value(key, opts)
  return values.find_value(self, key, opts)
end

--- Internal iterative GET_CLOSEST_PEERS.
-- `opts` forwarded to @{_run_client_lookup} and query functions.
function DHT:_get_closest_peers(key, opts)
  local options = opts or {}
  local seed_peers = options.peers
  if not seed_peers then
    local lookup_err
    seed_peers, lookup_err = seed_candidates_from_routing_table(self, key, self.k)
    if not seed_peers then
      return nil, lookup_err
    end
  end
  local lookup, lookup_err = self:_run_client_lookup(key, seed_peers, function(peer, ctx)
    local query_options = options
    if ctx then
      query_options = {}
      for k, v in pairs(options) do
        query_options[k] = v
      end
      query_options.ctx = ctx
    end
    local target = peer.peer_id and { peer_id = peer.peer_id, addrs = peer.addrs }
      or (peer.addr or (peer.addrs and peer.addrs[1]))
    local target_bytes, key_err = protocol.peer_bytes(key)
    if not target_bytes then
      return nil, key_err
    end
    local response, err = self:_rpc(target or { peer_id = peer.peer_id, addrs = peer.addrs }, {
      type = protocol.MESSAGE_TYPE.FIND_NODE,
      key = target_bytes,
    }, protocol.MESSAGE_TYPE.FIND_NODE, query_options)
    if not response then
      return nil, err
    end
    local closest = self:_decode_response_peers(response, "get_closest_peers_result")
    return { closer_peers = closest }
  end, options)
  if not lookup then
    return nil, lookup_err
  end

  local seen = {}
  local out = {}
  for _, peer in ipairs(lookup.closest_peers or {}) do
    if peer.peer_id and not seen[peer.peer_id] then
      seen[peer.peer_id] = true
      out[#out + 1] = peer
    end
  end
  local target_bytes = protocol.peer_bytes(key) or key
  local target_hash = self.routing_table:_hash(target_bytes)
  if target_hash then
    query.sort_candidates(self, target_hash, out)
  end
  while #out > (options.count or self.k) do
    out[#out] = nil
  end
  lookup.peers = out
  return out, lookup
end

--- Internal iterative FIND_PROVIDERS.
-- `opts` forwarded to @{_run_client_lookup} and query functions.
function DHT:_find_providers(key, opts)
  return provider_routing.find_providers(self, key, opts)
end

--- Internal value publication workflow.
-- Stores the local record and announces it to closest known peers.
function DHT:_put_value_workflow(key, value_or_record, opts)
  return values.put_value_workflow(self, key, value_or_record, opts)
end

--- Internal provider announcement workflow.
-- Stores the local provider record and announces it to closest known peers.
function DHT:_provide(key, opts)
  return provider_routing.provide(self, key, opts)
end

function DHT:_reprovide(opts)
  return reprovider.reprovide(self, opts)
end

--- Spawn a scheduler-backed query task wrapper.
-- `opts.result_count` controls operation nil/error arity.
function DHT:_spawn_query_task(task_name, fn, opts)
  if not (self.host and type(self.host.spawn_task) == "function") then
    return nil, error_mod.new("state", "kad_dht requires host task scheduler")
  end
  local task, task_err = self.host:spawn_task(task_name, fn, {
    service = "kad_dht",
  })
  if not task then
    return nil, task_err
  end
  return operation.new(self.host, task, opts)
end

--- Find a peer by id via lookup.
-- `opts` supports `timeout`, `ctx`, and `stream_opts`.
--- peer_or_addr string|table Query target.
--- target_key string Peer id to find.
--- opts? table
--- table|nil op Operation.
--- table|nil err
function DHT:find_node(peer_or_addr, target_key, opts)
  local options = opts or {}
  return self:_spawn_query_task("kad.find_node", function(ctx)
    local task_opts = {}
    for k, v in pairs(options) do
      task_opts[k] = v
    end
    task_opts.ctx = task_opts.ctx or ctx
    return self:_find_node(peer_or_addr, target_key, task_opts)
  end)
end

--- Query a peer for value records.
-- `opts` supports `timeout`, `ctx`, and `stream_opts`.
--- table|nil op
--- table|nil err
function DHT:get_value(peer_or_addr, key, opts)
  local options = opts or {}
  return self:_spawn_query_task("kad.get_value", function(ctx)
    local task_opts = {}
    for k, v in pairs(options) do
      task_opts[k] = v
    end
    task_opts.ctx = task_opts.ctx or ctx
    return self:_get_value(peer_or_addr, key, task_opts)
  end)
end

--- Query a peer to store a value record.
-- `opts` supports `timeout`, `ctx`, and `stream_opts`.
--- table|nil op
--- table|nil err
function DHT:put_value_to_peer(peer_or_addr, key, record, opts)
  local options = opts or {}
  return self:_spawn_query_task("kad.put_value_to_peer", function(ctx)
    local task_opts = {}
    for k, v in pairs(options) do
      task_opts[k] = v
    end
    task_opts.ctx = task_opts.ctx or ctx
    return self:_put_value(peer_or_addr, key, record, task_opts)
  end)
end

--- Query a peer for provider records.
-- `opts` supports `timeout`, `ctx`, and `stream_opts`.
--- table|nil op
--- table|nil err
function DHT:get_providers(peer_or_addr, key, opts)
  local options = opts or {}
  return self:_spawn_query_task("kad.get_providers", function(ctx)
    local task_opts = {}
    for k, v in pairs(options) do
      task_opts[k] = v
    end
    task_opts.ctx = task_opts.ctx or ctx
    return self:_get_providers(peer_or_addr, key, task_opts)
  end)
end

--- Store and publish a value record to closest peers.
-- `value_or_record` may be raw value bytes or a KAD record table.
--- table|nil op
--- table|nil err
function DHT:put_value(key, value_or_record, opts)
  local options = opts or {}
  return self:_spawn_query_task("kad.put_value", function(ctx)
    local task_opts = {}
    for k, v in pairs(options) do
      task_opts[k] = v
    end
    task_opts.scheduler_task = true
    task_opts.ctx = task_opts.ctx or ctx
    task_opts.yield = task_opts.yield or function()
      return ctx:checkpoint()
    end
    return self:_put_value_workflow(key, value_or_record, task_opts)
  end)
end

--- JS/libp2p-style value routing alias for @{put_value}.
--- table|nil op
--- table|nil err
function DHT:put(key, value_or_record, opts)
  return self:put_value(key, value_or_record, opts)
end

--- Announce this host as a provider for a content key.
-- `opts` supports lookup controls, `addrs`, provider TTL controls, and `fail_fast`.
--- table|nil op
--- table|nil err
function DHT:provide(key, opts)
  local options = opts or {}
  return self:_spawn_query_task("kad.provide", function(ctx)
    local task_opts = {}
    for k, v in pairs(options) do
      task_opts[k] = v
    end
    task_opts.scheduler_task = true
    task_opts.ctx = task_opts.ctx or ctx
    task_opts.yield = task_opts.yield or function()
      return ctx:checkpoint()
    end
    return self:_provide(key, task_opts)
  end)
end

--- Re-announce locally stored provider records.
-- `opts.keys` can limit to explicit provider keys; otherwise local provider keys
-- are read from the provider store. `opts.batch_size` bounds listed keys.
--- table|nil op
--- table|nil err
function DHT:reprovide(opts)
  local options = opts or {}
  return self:_spawn_query_task("kad.reprovide", function(ctx)
    local task_opts = {}
    for k, v in pairs(options) do
      task_opts[k] = v
    end
    task_opts.scheduler_task = true
    task_opts.ctx = task_opts.ctx or ctx
    task_opts.yield = task_opts.yield or function()
      return ctx:checkpoint()
    end
    return self:_reprovide(task_opts)
  end)
end

--- Lookup value through iterative query.
-- `opts` supports lookup controls like `alpha`, `k`, `disjoint_paths`,
-- `seed_peers`, `find_node_opts`, `query_timeout_seconds`, `max_query_rounds`,
-- and scheduler `ctx`/`yield`.
--- table|nil op
--- table|nil err
function DHT:find_value(key, opts)
  local options = opts or {}
  return self:_spawn_query_task("kad.find_value", function(ctx)
    local task_opts = {}
    for k, v in pairs(options) do
      task_opts[k] = v
    end
    task_opts.scheduler_task = true
    task_opts.ctx = task_opts.ctx or ctx
    task_opts.yield = task_opts.yield or function()
      return ctx:checkpoint()
    end
    return self:_find_value(key, task_opts)
  end)
end

--- JS/libp2p-style value routing alias for @{find_value}.
--- table|nil op
--- table|nil err
function DHT:get(key, opts)
  return self:find_value(key, opts)
end

--- Lookup closest peers through iterative query.
-- Uses same options as @{find_value}.
-- `opts.result_count` is fixed internally to include report and peers.
--- table|nil op
--- table|nil err
function DHT:get_closest_peers(key, opts)
  local options = opts or {}
  return self:_spawn_query_task("kad.get_closest_peers", function(ctx)
    local task_opts = {}
    for k, v in pairs(options) do
      task_opts[k] = v
    end
    task_opts.scheduler_task = true
    task_opts.ctx = task_opts.ctx or ctx
    task_opts.yield = task_opts.yield or function()
      return ctx:checkpoint()
    end
    return self:_get_closest_peers(key, task_opts)
  end, { result_count = 2 })
end

--- Find a peer through peer routing, optionally using local cache first.
-- Unlike @{find_peer}, this is a network-capable operation. Pass
-- `use_cache=false` to bypass the local routing table.
--- table|nil op
--- table|nil err
function DHT:find_peer_network(peer_id, opts)
  local options = opts or {}
  return self:_spawn_query_task("kad.find_peer", function(ctx)
    local task_opts = {}
    for k, v in pairs(options) do
      task_opts[k] = v
    end
    task_opts.scheduler_task = true
    task_opts.ctx = task_opts.ctx or ctx
    task_opts.yield = task_opts.yield or function()
      return ctx:checkpoint()
    end
    return self:_find_peer_network(peer_id, task_opts)
  end)
end

--- Lookup providers through iterative query.
-- Uses same options as @{find_value}.
-- `opts` may include `seed_peers`, `alpha`, `k`, `query_timeout_seconds`, and `ctx`.
--- table|nil op
--- table|nil err
function DHT:find_providers(key, opts)
  local options = opts or {}
  return self:_spawn_query_task("kad.find_providers", function(ctx)
    local task_opts = {}
    for k, v in pairs(options) do
      task_opts[k] = v
    end
    task_opts.scheduler_task = true
    task_opts.ctx = task_opts.ctx or ctx
    task_opts.yield = task_opts.yield or function()
      return ctx:checkpoint()
    end
    return self:_find_providers(key, task_opts)
  end)
end

--- Resolve bootstrap targets into dialable addresses.
-- `opts` supports `peer_discovery`, `bootstrappers`, `dnsaddr_resolver`,
-- and `ignore_resolve_errors`.
--- opts? table
--- table|nil addrs
--- table|nil err
function DHT:bootstrap_targets(opts)
  return bootstrap_dht.bootstrap_targets(self, opts)
end

--- Discover peers from configured discovery source.
-- `opts.ignore_source_errors` (`boolean`) suppresses source failures.
--- opts? table
--- table|nil peers
--- table|nil err
function DHT:discover_peers(opts)
  return bootstrap_dht.discover_peers(self, opts)
end

function DHT:_on_peer_protocols_updated(payload)
  local peer_id = payload and payload.peer_id
  if type(peer_id) ~= "string" or peer_id == "" then
    return true
  end
  local protocols = payload.protocols or {}
  local supports_kad = false
  for _, protocol_id in ipairs(protocols) do
    if protocol_id == self.protocol_id then
      supports_kad = true
      break
    end
  end
  if not supports_kad then
    return true
  end

  local addrs = nil
  if self.host and self.host.peerstore then
    addrs = self:_filter_addrs(self.host.peerstore:get_addrs(peer_id), {
      peer_id = peer_id,
      purpose = "protocol_update",
    })
  end
  local added, add_err = self:add_peer(peer_id, {
    addrs = addrs,
    allow_replace = true,
  })
  local health = self._peer_health[peer_id] or {}
  health.last_verified_at = os.time()
  health.failed_checks = 0
  health.stale = false
  self._peer_health[peer_id] = health
  if not added and add_err and not is_capacity_error(add_err) then
    return nil, add_err
  end
  return true
end

--- Internal peer protocol support probe.
-- `opts.timeout`, `opts.ctx`, and `opts.stream_opts` tune identify query.
function DHT:_supports_kad_protocol(peer_or_addr, opts)
  opts = opts or {}
  if
    not opts.ctx
    and not opts._internal_task_ctx
    and self.host
    and type(self.host.spawn_task) == "function"
    and type(self.host.run_until_task) == "function"
  then
    local task, task_err = self.host:spawn_task("kad.supports_protocol.inline", function(ctx)
      local task_opts = {}
      for k, v in pairs(opts) do
        task_opts[k] = v
      end
      task_opts.ctx = ctx
      task_opts._internal_task_ctx = true
      return self:_supports_kad_protocol(peer_or_addr, task_opts)
    end, { service = "kad_dht" })
    if not task then
      return nil, task_err
    end
    return self.host:run_until_task(task, {
      timeout = opts.timeout,
      poll_interval = opts.poll_interval,
    })
  end

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

--- Internal bootstrap dial workflow.
-- `opts.bootstrap_target_count`, `opts.dial_timeout`, `opts.ignore_dial_errors`,
-- `opts.ctx`, and `opts.yield` control sequencing.
function DHT:_bootstrap(opts)
  return bootstrap_dht.bootstrap(self, opts)
end

--- Refresh peer liveness/protocol support and evict stale peers.
-- `opts` supports `min_recheck_seconds`, `max_checks`,
-- `max_failed_checks_before_evict`, and `protocol_check_opts`.
--- opts? table
--- table|nil report
--- table|nil err
function DHT:refresh_once(opts)
  return maintenance.refresh_once(self, opts)
end

local function xor_distance(left, right)
  local out = {}
  for i = 1, #left do
    out[i] = string.char(left:byte(i) ~ right:byte(i))
  end
  return table.concat(out)
end

function DHT:_distance_to_target(peer_id, target_hash)
  local bytes, bytes_err = protocol.peer_bytes(peer_id)
  if not bytes then
    return nil, bytes_err
  end
  local hashed, hash_err = self.routing_table:_hash(bytes)
  if not hashed then
    return nil, hash_err
  end
  return xor_distance(target_hash, hashed)
end

--- Internal random-walk execution.
-- `opts.count`, `opts.alpha`, `opts.disjoint_paths`, `opts.find_node_opts`,
-- `opts.ctx`, and `opts.yield` tune walk behavior.
function DHT:_random_walk(opts)
  return random_walk.run(self, opts)
end

--- Execute a scheduler-backed random walk query.
-- `opts` supports walk/query controls like `count`, `alpha`, `disjoint_paths`,
-- `find_node_opts`, and scheduler `ctx`/`yield`.
--- opts? table
--- table|nil op
--- table|nil err
function DHT:random_walk(opts)
  return random_walk.spawn(self, opts)
end

--- Build a KAD service instance.
-- Common `opts`: `mode`, `auto_server_mode`, `k`, `alpha`, `disjoint_paths`, `protocol_id`,
-- `address_filter`, `peer_discovery`, `bootstrappers`, `routing_table`,
-- and maintenance controls (`maintenance_enabled`, `maintenance_interval_seconds`, etc.).
-- Additional options: `local_peer_id`, `dnsaddr_resolver`,
-- `max_message_size`, and protocol-check tuning values.
-- `mode="auto"` starts in client mode and switches to server mode when the
-- host advertises a public direct address, matching js-libp2p DHT behavior.
-- `opts.maintenance_enabled` defaults to `true`; interval/check knobs use
-- `maintenance_*` names from module defaults.
-- Maintenance eviction semantics follow go-libp2p-kad-dht: after the
-- recheck grace window, one failed liveness/protocol maintenance probe evicts
-- the peer.
--- host table Host instance.
--- opts? table
--- table|nil dht
--- table|nil err
function M.new(host, opts)
  local options = opts or {}

  local address_filter, address_filter_err =
    normalize_address_filter(options.address_filter or options.address_filter_mode)
  if not address_filter then
    return nil, address_filter_err
  end

  local max_peers_per_ip_group = options.peer_diversity_max_peers_per_ip_group
  if max_peers_per_ip_group == false then
    max_peers_per_ip_group = nil
  elseif max_peers_per_ip_group ~= nil then
    max_peers_per_ip_group = tonumber(max_peers_per_ip_group)
    if not max_peers_per_ip_group or max_peers_per_ip_group < 0 then
      return nil, error_mod.new("input", "peer_diversity_max_peers_per_ip_group must be a non-negative number")
    end
  end

  local max_peers_per_ip_group_per_bucket = options.peer_diversity_max_peers_per_ip_group_per_bucket
  if max_peers_per_ip_group_per_bucket == false then
    max_peers_per_ip_group_per_bucket = nil
  elseif max_peers_per_ip_group_per_bucket ~= nil then
    max_peers_per_ip_group_per_bucket = tonumber(max_peers_per_ip_group_per_bucket)
    if not max_peers_per_ip_group_per_bucket or max_peers_per_ip_group_per_bucket < 0 then
      return nil,
        error_mod.new("input", "peer_diversity_max_peers_per_ip_group_per_bucket must be a non-negative number")
    end
  end

  local populate_from_peerstore_limit = options.populate_from_peerstore_limit or M.DEFAULT_POPULATE_FROM_PEERSTORE_LIMIT
  populate_from_peerstore_limit = tonumber(populate_from_peerstore_limit)
  if not populate_from_peerstore_limit or populate_from_peerstore_limit < 0 then
    return nil, error_mod.new("input", "populate_from_peerstore_limit must be a non-negative number")
  end

  local populate_from_peerstore_protocol_check_timeout = options.populate_from_peerstore_protocol_check_timeout
    or M.DEFAULT_POPULATE_FROM_PEERSTORE_PROTOCOL_CHECK_TIMEOUT
  populate_from_peerstore_protocol_check_timeout = tonumber(populate_from_peerstore_protocol_check_timeout)
  if not populate_from_peerstore_protocol_check_timeout or populate_from_peerstore_protocol_check_timeout <= 0 then
    return nil,
      error_mod.new("input", "populate_from_peerstore_protocol_check_timeout must be a positive number")
  end

  local local_peer_id = options.local_peer_id
  if not local_peer_id and host and type(host.peer_id) == "function" then
    local p = host:peer_id()
    local_peer_id = p and p.id
  end
  if type(local_peer_id) ~= "string" or local_peer_id == "" then
    return nil, error_mod.new("input", "kad-dht needs local peer id or host with peer_id()")
  end

  local provider_store, provider_store_err
  if options.provider_store then
    provider_store = options.provider_store
  else
    local provider_ttl_seconds = options.provider_ttl_seconds
    if provider_ttl_seconds == nil then
      provider_ttl_seconds = M.DEFAULT_PROVIDER_TTL_SECONDS
    end
    provider_store, provider_store_err = providers.new({
      datastore = options.provider_datastore or options.datastore,
      default_ttl_seconds = provider_ttl_seconds,
      now = options.now,
    })
    if not provider_store then
      return nil, provider_store_err
    end
  end

  local record_store, record_store_err
  if options.record_store then
    record_store = options.record_store
  else
    local record_ttl_seconds = options.record_ttl_seconds
    if record_ttl_seconds == nil then
      record_ttl_seconds = M.DEFAULT_RECORD_TTL_SECONDS
    end
    record_store, record_store_err = records.new({
      datastore = options.record_datastore or options.datastore,
      default_ttl_seconds = record_ttl_seconds,
      now = options.now,
    })
    if not record_store then
      return nil, record_store_err
    end
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

  local mode = options.mode == "auto" and "client" or (options.mode or "client")
  local maintenance_enabled
  if options.maintenance_enabled == nil then
    maintenance_enabled = M.DEFAULT_MAINTENANCE_ENABLED
  else
    maintenance_enabled = options.maintenance_enabled == true
  end

  local self_obj = setmetatable({
    host = host,
    local_peer_id = local_peer_id,
    protocol_id = options.protocol_id or M.PROTOCOL_ID,
    k = options.k or M.DEFAULT_K,
    alpha = options.alpha or M.DEFAULT_ALPHA,
    disjoint_paths = options.disjoint_paths or M.DEFAULT_DISJOINT_PATHS,
    max_concurrent_queries = options.max_concurrent_queries or M.DEFAULT_MAX_CONCURRENT_QUERIES,
    max_message_size = options.max_message_size or protocol.MAX_MESSAGE_SIZE,
    mode = mode,
    address_filter = address_filter,
    bootstrappers = options.bootstrappers,
    peer_discovery = options.peer_discovery,
    query_filter = options.query_filter,
    routing_table_filter = options.routing_table_filter,
    peer_diversity_filter = options.peer_diversity_filter,
    peer_diversity_max_peers_per_ip_group = max_peers_per_ip_group,
    peer_diversity_max_peers_per_ip_group_per_bucket = max_peers_per_ip_group_per_bucket,
    provider_addr_ttl_seconds = options.provider_addr_ttl_seconds == nil and M.DEFAULT_PROVIDER_ADDR_TTL_SECONDS
      or options.provider_addr_ttl_seconds,
    provider_store = provider_store,
    record_store = record_store,
    record_validator = options.record_validator,
    record_selector = options.record_selector,
    record_validators = values.merge_policy_maps(record_validators.default_validators(), options.record_validators),
    record_selectors = values.merge_policy_maps(record_validators.default_selectors(), options.record_selectors),
    routing_table = rt,
    _peer_health = {},
    _dnsaddr_resolver = options.dnsaddr_resolver,
    _host_on_protocols_updated = nil,
    _host_on_self_peer_update = nil,
    _auto_server_mode = options.auto_server_mode == true or options.mode == "auto",
    _maintenance_enabled = maintenance_enabled,
    _maintenance_interval_seconds = options.maintenance_interval_seconds or M.DEFAULT_MAINTENANCE_INTERVAL_SECONDS,
    _maintenance_startup_retry_seconds = options.maintenance_startup_retry_seconds
      or M.DEFAULT_MAINTENANCE_STARTUP_RETRY_SECONDS,
    _maintenance_startup_retry_max_seconds = options.maintenance_startup_retry_max_seconds
      or M.DEFAULT_MAINTENANCE_STARTUP_RETRY_MAX_SECONDS,
    _maintenance_startup_retry_current_seconds = options.maintenance_startup_retry_seconds
      or M.DEFAULT_MAINTENANCE_STARTUP_RETRY_SECONDS,
    _maintenance_established = false,
    _maintenance_running = false,
    _maintenance_min_recheck_seconds = options.maintenance_min_recheck_seconds
      or M.DEFAULT_MAINTENANCE_MIN_RECHECK_SECONDS,
    _maintenance_max_checks = options.maintenance_max_checks or M.DEFAULT_MAINTENANCE_MAX_CHECKS,
    _maintenance_walk_every = options.maintenance_walk_every or M.DEFAULT_MAINTENANCE_WALK_EVERY,
    _maintenance_walk_timeout = options.maintenance_walk_timeout or maintenance.DEFAULT_WALK_TIMEOUT,
    _maintenance_protocol_check_timeout = options.maintenance_protocol_check_timeout
      or maintenance.DEFAULT_PROTOCOL_CHECK_TIMEOUT,
    _max_failed_checks_before_evict = options.max_failed_checks_before_evict
      or M.DEFAULT_MAX_FAILED_CHECKS_BEFORE_EVICT,
    _maintenance_task = nil,
    _maintenance_trigger_task = nil,
    _reprovider_enabled = options.reprovider_enabled == true or M.DEFAULT_REPROVIDER_ENABLED,
    _reprovider_interval_seconds = options.reprovider_interval_seconds or M.DEFAULT_REPROVIDER_INTERVAL_SECONDS,
    _reprovider_initial_delay_seconds = options.reprovider_initial_delay_seconds
      or M.DEFAULT_REPROVIDER_INITIAL_DELAY_SECONDS,
    _reprovider_jitter_seconds = options.reprovider_jitter_seconds or M.DEFAULT_REPROVIDER_JITTER_SECONDS,
    _reprovider_random = options.reprovider_random,
    _reprovider_batch_size = options.reprovider_batch_size or M.DEFAULT_REPROVIDER_BATCH_SIZE,
    _reprovider_max_parallel = options.reprovider_max_parallel or reprovider.DEFAULT_MAX_PARALLEL,
    _reprovider_timeout = options.reprovider_timeout or M.DEFAULT_REPROVIDER_TIMEOUT,
    _reprovider_task = nil,
    _peer_id_bytes_cache = {},
    _peer_id_bytes_cache_count = 0,
    _peer_id_bytes_cache_size = options.peer_id_bytes_cache_size or M.DEFAULT_PEER_ID_BYTES_CACHE_SIZE,
    _filtered_addr_cache = {},
    _filtered_addr_cache_count = 0,
    _filtered_addr_cache_size = options.filtered_addr_cache_size or M.DEFAULT_FILTERED_ADDR_CACHE_SIZE,
    _filtered_addr_cache_ttl_seconds = options.filtered_addr_cache_ttl_seconds == nil
        and M.DEFAULT_FILTERED_ADDR_CACHE_TTL_SECONDS
      or options.filtered_addr_cache_ttl_seconds,
    _populate_from_peerstore_on_start = options.populate_from_peerstore_on_start ~= false,
    _populate_from_peerstore_limit = populate_from_peerstore_limit,
    _populate_from_peerstore_protocol_check_timeout = populate_from_peerstore_protocol_check_timeout,
    _peerstore_populate_task = nil,
    _running = false,
  }, DHT)

  if host and type(host.on) == "function" then
    self_obj._host_on_protocols_updated = function(payload)
      return self_obj:_on_peer_protocols_updated(payload)
    end
    local ok, on_err = host:on("peer_protocols_updated", self_obj._host_on_protocols_updated)
    if not ok then
      return nil, on_err
    end
    if self_obj._auto_server_mode then
      self_obj._host_on_self_peer_update = function(payload)
        return self_obj:_on_self_peer_update(payload)
      end
      ok, on_err = host:on("self_peer_update", self_obj._host_on_self_peer_update)
      if not ok then
        return nil, on_err
      end
    end
  end

  if not self_obj.peer_discovery and host and host.peer_discovery then
    self_obj.peer_discovery = host.peer_discovery
  end

  if not self_obj.peer_discovery and self_obj.bootstrappers then
    local default_discovery, discovery_err = M.default_peer_discovery({
      bootstrappers = self_obj.bootstrappers or bootstrap.DEFAULT_BOOTSTRAPPERS,
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
M.bootstrap_dht = bootstrap_dht
M.dnsaddr = dnsaddr
M.discovery = discovery
M.maintenance = maintenance
M.providers = providers
M.provider_routing = provider_routing
M.query = query
M.random_walk = random_walk
M.records = records
M.record_validators = record_validators
M.reprovider = reprovider
M.values = values
M.protocol = protocol

return M
