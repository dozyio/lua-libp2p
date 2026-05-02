local error_mod = require("lua_libp2p.error")
local bootstrap = require("lua_libp2p.bootstrap")
local dnsaddr = require("lua_libp2p.dnsaddr")
local discovery = require("lua_libp2p.discovery")
local discovery_bootstrap = require("lua_libp2p.peer_discovery_bootstrap")
local kbucket = require("lua_libp2p.kbucket")
local multiaddr = require("lua_libp2p.multiaddr")
local operation = require("lua_libp2p.operation")
local peerid = require("lua_libp2p.peerid")
local protocol = require("lua_libp2p.kad_dht.protocol")
local log = require("lua_libp2p.log")

local M = {}
local compare_distance
M.provides = { "peer_routing", "kad_dht" }
M.requires = { "identify", "ping" }

M.PROTOCOL_ID = "/ipfs/kad/1.0.0"
M.DEFAULT_K = 20
M.DEFAULT_ALPHA = 10
M.DEFAULT_DISJOINT_PATHS = 10
M.DEFAULT_ADDRESS_FILTER = "public"
M.DEFAULT_MAINTENANCE_ENABLED = true
M.DEFAULT_MAINTENANCE_INTERVAL_SECONDS = 30
M.DEFAULT_MAINTENANCE_MIN_RECHECK_SECONDS = 60
M.DEFAULT_MAINTENANCE_MAX_CHECKS = 10
M.DEFAULT_MAINTENANCE_WALK_EVERY = 4
M.DEFAULT_MAX_FAILED_CHECKS_BEFORE_EVICT = 2

function M.default_peer_discovery(opts)
  local options = opts or {}
  local yield = type(options.yield) == "function" and options.yield or nil
  local bootstrap_source, bootstrap_err = discovery_bootstrap.new({
    list = options.bootstrappers or bootstrap.DEFAULT_BOOTSTRAPPERS,
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

  if self.mode == "server" and self.host and type(self.host.handle) == "function" then
    local ok, err = self.host:handle(self.protocol_id, function(stream, ctx)
      return self:_handle_rpc(stream, ctx)
    end)
    if not ok then
      return nil, err
    end
  end

  self._running = true

  if self._maintenance_enabled and self.host and type(self.host.spawn_task) == "function" then
    local task, task_err = self.host:spawn_task("kad.maintenance", function(ctx)
      local tick = 0
      while self._running do
        tick = tick + 1
        local refresh_report, refresh_err = self:refresh_once({
          min_recheck_seconds = self._maintenance_min_recheck_seconds,
          max_checks = self._maintenance_max_checks,
          protocol_check_opts = {
            timeout = self._maintenance_protocol_check_timeout,
            stream_opts = { ctx = ctx },
          },
        })
        if not refresh_report and refresh_err then
          log.debug("kad dht maintenance refresh failed", {
            cause = tostring(refresh_err),
            subsystem = "kad_dht",
          })
        else
          local emit_event = self.host and self.host.emit
          if type(emit_event) == "function" then
            emit_event(self.host, "kad_dht:maintenance:refresh", refresh_report)
          end
        end

        if self._maintenance_walk_every > 0 and (tick % self._maintenance_walk_every) == 0 then
          local walk_op = self:random_walk({
            alpha = self.alpha,
            disjoint_paths = self.disjoint_paths,
            find_node_opts = { ctx = ctx },
          })
          if walk_op and type(walk_op.result) == "function" then
            local walk_report = walk_op:result({ ctx = ctx, timeout = self._maintenance_walk_timeout })
            if self.host and type(self.host.emit) == "function" then
              self.host:emit("kad_dht:maintenance:walk", walk_report)
            end
          end
        end

        local sleep_ok, sleep_err = ctx:sleep(self._maintenance_interval_seconds)
        if sleep_ok == nil and sleep_err then
          return nil, sleep_err
        end
      end
      return true
    end, { service = "kad_dht" })
    if not task then
      return nil, task_err
    end
    self._maintenance_task = task
  end

  return true
end

function DHT:stop()
  if self.host and type(self.host.off) == "function" then
    if self._host_on_protocols_updated then
      self.host:off("peer_protocols_updated", self._host_on_protocols_updated)
    end
  end
  if self._maintenance_task and self.host and type(self.host.cancel_task) == "function" then
    self.host:cancel_task(self._maintenance_task.id)
  end
  self._maintenance_task = nil
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
    self._peer_health[peer_id].failed_checks = 0
    self._peer_health[peer_id].last_connected_at = self._peer_health[peer_id].last_connected_at or now
    log.debug("kad dht peer added", {
      peer_id = peer_id,
      subsystem = "kad_dht",
    })
  elseif err then
    log.debug("kad dht peer rejected", {
      peer_id = tostring(peer_id),
      cause = tostring(err),
      subsystem = "kad_dht",
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
  return self:add_peer(peer_id)
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
    local protocol = parsed.components[i].protocol
    if protocol ~= "p2p" then
      return false
    end
  end
  return true
end

local function dialable_tcp_addrs(addrs)
  local out = {}
  for _, addr in ipairs(addrs or {}) do
    if is_dialable_tcp_addr(addr) then
      out[#out + 1] = addr
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
  return nil, error_mod.new("input", "unsupported kad-dht address filter", {
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
    local addrs = {}
    if self.host and self.host.peerstore then
      addrs = self:_filter_addrs(self.host.peerstore:get_addrs(entry.peer_id), {
        peer_id = entry.peer_id,
        purpose = "response",
      })
    end
    peers[#peers + 1] = {
      id = id_bytes,
      addrs = addrs,
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
  local req, req_err = protocol.read(stream, { max_message_size = self.max_message_size })
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

  local wrote, write_err = protocol.write(stream, response, { max_message_size = self.max_message_size })
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

function DHT:_find_node(peer_or_addr, target_key, opts)
  opts = opts or {}
  if not opts.ctx
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
    return nil, error_mod.new("protocol", "unexpected kad-dht response type", {
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

function DHT:_rpc(peer_or_addr, request, expected_type, opts)
  opts = opts or {}
  if not opts.ctx
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
    return nil, response_err
  end
  if response.type ~= expected_type then
    return nil, error_mod.new("protocol", "unexpected kad-dht response type", {
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

function DHT:_get_value(peer_or_addr, key, opts)
  if type(key) ~= "string" or key == "" then
    return nil, error_mod.new("input", "GET_VALUE key must be non-empty bytes")
  end
  local response, err = self:_rpc(peer_or_addr, {
    type = protocol.MESSAGE_TYPE.GET_VALUE,
    key = key,
  }, protocol.MESSAGE_TYPE.GET_VALUE, opts)
  if not response then
    return nil, err
  end
  local closer = self:_decode_response_peers(response, "get_value_result")
  return {
    record = response.record,
    closer_peers = closer,
    key = response.key,
  }
end

function DHT:_get_providers(peer_or_addr, key, opts)
  if type(key) ~= "string" or key == "" then
    return nil, error_mod.new("input", "GET_PROVIDERS key must be non-empty bytes")
  end
  local response, err = self:_rpc(peer_or_addr, {
    type = protocol.MESSAGE_TYPE.GET_PROVIDERS,
    key = key,
  }, protocol.MESSAGE_TYPE.GET_PROVIDERS, opts)
  if not response then
    return nil, err
  end
  local closer, providers = self:_decode_response_peers(response, "get_providers_result")
  return {
    providers = providers,
    provider_peers = providers,
    closer_peers = closer,
    key = response.key,
  }
end

local function seed_candidates_from_routing_table(self, key, count)
  local nearest = self:find_closest_peers(key, count or self.k) or {}
  local out = {}
  for _, entry in ipairs(nearest) do
    local addrs = {}
    if self.host and self.host.peerstore then
      addrs = dialable_tcp_addrs(self:_filter_addrs(self.host.peerstore:get_addrs(entry.peer_id), {
        peer_id = entry.peer_id,
        purpose = "client_query_seed",
      }))
    end
    if #addrs > 0 then
      out[#out + 1] = { peer_id = entry.peer_id, addrs = addrs }
    end
  end
  return out
end

function DHT:_sort_query_candidates(target_hash, candidates)
  table.sort(candidates, function(a, b)
    local da = self:_distance_to_target(a.peer_id or "", target_hash) or string.rep("\255", 32)
    local db = self:_distance_to_target(b.peer_id or "", target_hash) or string.rep("\255", 32)
    return compare_distance(da, db) < 0
  end)
end

function DHT:_strict_lookup_complete(target_hash, states, k)
  local peers = {}
  local heard_or_waiting = 0
  for peer_id, state in pairs(states) do
    if state.state ~= "unreachable" then
      peers[#peers + 1] = state
      if state.state == "heard" or state.state == "waiting" then
        heard_or_waiting = heard_or_waiting + 1
      end
    end
  end
  self:_sort_query_candidates(target_hash, peers)
  if #peers == 0 then
    return heard_or_waiting == 0, "starvation"
  end
  local limit = math.min(k or self.k, #peers)
  for i = 1, limit do
    if peers[i].state ~= "queried" then
      return false
    end
  end
  if #peers < (k or self.k) then
    return heard_or_waiting == 0, "closest_available_queried"
  end
  return true, "closest_queried"
end

function DHT:_run_client_lookup(key, seed_peers, query_func, opts)
  local options = opts or {}
  local target_bytes, target_err = protocol.peer_bytes(key)
  if not target_bytes then
    target_bytes = key
  end
  local target_hash, hash_err = self.routing_table:_hash(target_bytes)
  if not target_hash then
    return nil, hash_err or target_err
  end

  local states = {}
  local queue = {}
  local function enqueue(peer)
    if type(peer) ~= "table" or type(peer.peer_id) ~= "string" or peer.peer_id == self.local_peer_id then
      return
    end
    if states[peer.peer_id] then
      return
    end
    local addrs = dialable_tcp_addrs(peer.addrs or (peer.addr and { peer.addr }) or {})
    if #addrs == 0 and self.host and self.host.peerstore then
      addrs = dialable_tcp_addrs(self:_filter_addrs(self.host.peerstore:get_addrs(peer.peer_id), {
        peer_id = peer.peer_id,
        purpose = "client_query_enqueue",
      }))
    end
    local has_connection = self.host and type(self.host._find_connection) == "function" and self.host:_find_connection(peer.peer_id) ~= nil
    if #addrs == 0 and self.host and self.host.peerstore and not has_connection then
      return
    end
    local entry = { peer_id = peer.peer_id, addrs = addrs, addr = peer.addr or addrs[1], state = "heard" }
    states[peer.peer_id] = entry
    queue[#queue + 1] = entry
  end
  for _, peer in ipairs(seed_peers or {}) do
    enqueue(peer)
  end

  local result = {
    queried = 0,
    responses = 0,
    failed = 0,
    cancelled = 0,
    errors = {},
    closest_peers = {},
    queried_peers = {},
    termination = nil,
    active_peak = 0,
  }
  local alpha = options.alpha or self.alpha
  local max_inflight = alpha * (options.disjoint_paths or self.disjoint_paths or 1)
  local strict_k = options.lookup_k or self.k
  local stop = false
  local active = 0
  local yield = type(options.yield) == "function" and options.yield or nil

  local function complete(peer, ok, response_or_err)
    active = active - 1
    if ok then
      peer.state = "queried"
      result.responses = result.responses + 1
      result.queried_peers[#result.queried_peers + 1] = peer
      self:_record_kad_peer(peer.peer_id, peer.addrs)
      local response = response_or_err or {}
      if response.stop then
        stop = true
      end
      for _, closer in ipairs(response.closer_peers or {}) do
        result.closest_peers[#result.closest_peers + 1] = closer
        enqueue(closer)
      end
    else
      peer.state = "unreachable"
      result.failed = result.failed + 1
      if response_or_err then
        result.errors[#result.errors + 1] = response_or_err
      end
    end
  end

  local function spawn(peer)
    peer.state = "waiting"
    active = active + 1
    if active > result.active_peak then
      result.active_peak = active
    end
    result.queried = result.queried + 1
    local co = coroutine.create(function()
      local response, err = query_func(peer)
      if not response then
        return false, err
      end
      return true, response
    end)
    return { co = co, peer = peer }
  end

  if options.scheduler_task == true and options.ctx and self.host and type(self.host.spawn_task) == "function" then
    local tasks = {}

    local function spawn_task(peer)
      peer.state = "waiting"
      active = active + 1
      if active > result.active_peak then
        result.active_peak = active
      end
      result.queried = result.queried + 1
      local task, task_err = self.host:spawn_task("kad.client_query", function(task_ctx)
        local response, err = query_func(peer, task_ctx)
        if not response then
          return nil, err
        end
        return response
      end, { service = "kad_dht" })
      if not task then
        complete(peer, false, task_err)
        return
      end
      tasks[#tasks + 1] = { task = task, peer = peer }
    end

    local function fill_tasks()
      self:_sort_query_candidates(target_hash, queue)
      while #queue > 0 and active < max_inflight and not stop do
        local peer = table.remove(queue, 1)
        if peer.state == "heard" then
          spawn_task(peer)
        end
      end
    end

    local function reap_tasks()
      for i = #tasks, 1, -1 do
        local item = tasks[i]
        local task = item.task
        if task.status == "completed" then
          complete(item.peer, true, task.result)
          table.remove(tasks, i)
        elseif task.status == "failed" then
          complete(item.peer, false, task.error or task.status)
          table.remove(tasks, i)
        elseif task.status == "cancelled" then
          active = active - 1
          item.peer.state = "unreachable"
          result.cancelled = result.cancelled + 1
          table.remove(tasks, i)
        end
      end
    end

    local function cancel_active_tasks()
      if not (self.host and type(self.host.cancel_task) == "function") then
        return
      end
      for _, item in ipairs(tasks) do
        local task = item.task
        if task and task.status ~= "completed" and task.status ~= "failed" and task.status ~= "cancelled" then
          self.host:cancel_task(task.id)
          result.cancelled = result.cancelled + 1
        end
      end
      tasks = {}
      active = 0
    end

    local function running_tasks()
      local out = {}
      for _, item in ipairs(tasks) do
        local task = item.task
        if task and task.status ~= "completed" and task.status ~= "failed" and task.status ~= "cancelled" then
          out[#out + 1] = task
        end
      end
      return out
    end

    while true do
      reap_tasks()
      fill_tasks()
      reap_tasks()

      local done, reason = self:_strict_lookup_complete(target_hash, states, strict_k)
      if stop then
        cancel_active_tasks()
        result.termination = result.termination or "application"
        break
      end
      if done then
        cancel_active_tasks()
        result.termination = reason
        break
      end
      if active == 0 and #queue == 0 then
        result.termination = "starvation"
        break
      end

      local waiting = running_tasks()
      local checkpoint_ok, checkpoint_err
      if #waiting > 0 and type(options.ctx.await_any_task) == "function" then
        checkpoint_ok, checkpoint_err = options.ctx:await_any_task(waiting)
      else
        checkpoint_ok, checkpoint_err = options.ctx:checkpoint()
      end
      if checkpoint_ok == nil and checkpoint_err then
        result.termination = "yield_error"
        result.errors[#result.errors + 1] = checkpoint_err
        break
      end
    end

    self:_sort_query_candidates(target_hash, result.closest_peers)
    return result
  elseif options.scheduler_task == true then
    while not stop do
      self:_sort_query_candidates(target_hash, queue)
      local peer
      while #queue > 0 and not peer do
        local candidate = table.remove(queue, 1)
        if candidate.state == "heard" then
          peer = candidate
        end
      end

      if not peer then
        local done, reason = self:_strict_lookup_complete(target_hash, states, strict_k)
        result.termination = done and reason or "starvation"
        break
      end

      peer.state = "waiting"
      active = active + 1
      if active > result.active_peak then
        result.active_peak = active
      end
      result.queried = result.queried + 1

      local response, response_err = query_func(peer)
      complete(peer, response ~= nil, response or response_err)

      if yield then
        local yield_ok, yield_err = yield()
        if yield_ok == nil and yield_err then
          result.termination = "yield_error"
          result.errors[#result.errors + 1] = yield_err
          stop = true
        end
      end

      local done, reason = self:_strict_lookup_complete(target_hash, states, strict_k)
      if stop then
        if not result.termination then
          result.termination = "application"
        end
        break
      end
      if done then
        result.termination = reason
        break
      end
    end

    self:_sort_query_candidates(target_hash, result.closest_peers)
    return result
  end

  local workers = {}
  local function step_lookup()
    self:_sort_query_candidates(target_hash, queue)
    while #queue > 0 and active < max_inflight and not stop do
      local peer = table.remove(queue, 1)
      if peer.state == "heard" then
        workers[#workers + 1] = spawn(peer)
      end
    end

    for i = #workers, 1, -1 do
      local worker = workers[i]
      local ok, success, response_or_err = coroutine.resume(worker.co)
      if not ok then
        success = false
        response_or_err = error_mod.new("protocol", "kad client query coroutine failed", { cause = success })
      end
      if coroutine.status(worker.co) == "dead" then
        complete(worker.peer, success == true, response_or_err)
        table.remove(workers, i)
      end
    end

    if yield then
      local yield_ok, yield_err = yield()
      if yield_ok == nil and yield_err then
        result.termination = "yield_error"
        result.errors[#result.errors + 1] = yield_err
        stop = true
        return true
      end
    end

    local done, reason = self:_strict_lookup_complete(target_hash, states, strict_k)
    if stop then
      result.termination = "application"
      return true
    end
    if done then
      result.termination = reason
      return true
    end
    if active == 0 and #queue == 0 then
      result.termination = "starvation"
      return true
    end

    return false
  end

  while not step_lookup() do
    if #workers == 0 and #queue == 0 then
      break
    end
  end

  return result
end

function DHT:_find_value(key, opts)
  local options = opts or {}
  local found
  local lookup, lookup_err = self:_run_client_lookup(key, options.peers or seed_candidates_from_routing_table(self, key, self.k), function(peer, ctx)
    local query_options = options
    if ctx then
      query_options = {}
      for k, v in pairs(options) do
        query_options[k] = v
      end
      query_options.ctx = ctx
    end
    local result, err = self:_get_value(peer.addr or (peer.addrs and peer.addrs[1]) or { peer_id = peer.peer_id, addrs = peer.addrs }, key, query_options)
    if not result then
      return nil, err
    end
    if result.record and result.record.value ~= nil then
      found = result
      return { closer_peers = result.closer_peers, stop = true }
    end
    return { closer_peers = result.closer_peers }
  end, options)
  if not lookup then
    return nil, lookup_err
  end
  if found then
    found.lookup = lookup
    return found
  end
  return { record = nil, closer_peers = lookup.closest_peers, lookup = lookup, errors = lookup.errors }
end

function DHT:_get_closest_peers(key, opts)
  local options = opts or {}
  local lookup, lookup_err = self:_run_client_lookup(key, options.peers or seed_candidates_from_routing_table(self, key, self.k), function(peer, ctx)
    local query_options = options
    if ctx then
      query_options = {}
      for k, v in pairs(options) do
        query_options[k] = v
      end
      query_options.ctx = ctx
    end
    local target = peer.peer_id and { peer_id = peer.peer_id, addrs = peer.addrs } or (peer.addr or (peer.addrs and peer.addrs[1]))
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
      if #out >= (options.count or self.k) then
        break
      end
    end
  end
  lookup.peers = out
  return out, lookup
end

function DHT:_find_providers(key, opts)
  local options = opts or {}
  local providers = {}
  local lookup, lookup_err = self:_run_client_lookup(key, options.peers or seed_candidates_from_routing_table(self, key, self.k), function(peer, ctx)
    local query_options = options
    if ctx then
      query_options = {}
      for k, v in pairs(options) do
        query_options[k] = v
      end
      query_options.ctx = ctx
    end
    local result, err = self:_get_providers(peer.addr or (peer.addrs and peer.addrs[1]) or { peer_id = peer.peer_id, addrs = peer.addrs }, key, query_options)
    if not result then
      return nil, err
    end
    for _, provider in ipairs(result.providers or {}) do
      providers[#providers + 1] = provider
      if #providers >= (options.limit or 1) then
        return { closer_peers = result.closer_peers, stop = true }
      end
    end
    return { closer_peers = result.closer_peers }
  end, options)
  if not lookup then
    return nil, lookup_err
  end
  return { providers = providers, provider_peers = providers, closer_peers = lookup.closest_peers, lookup = lookup, errors = lookup.errors }
end

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

function DHT:bootstrap_targets(opts)
  local options = opts or {}
  local discoverer = options.peer_discovery or self.peer_discovery
  if not discoverer then
    local discover_err
    discoverer, discover_err = M.default_peer_discovery({
      bootstrappers = options.bootstrappers or self.bootstrappers or bootstrap.DEFAULT_BOOTSTRAPPERS,
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

function DHT:_supports_kad_protocol(peer_or_addr, opts)
  opts = opts or {}
  if not opts.ctx
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

function DHT:_bootstrap(opts)
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
    skipped = 0,
    failed = 0,
    peers = {},
    errors = {},
  }

  local seen = {}
  for _, candidate in ipairs(candidates) do
    local peer_id = candidate.peer_id
    local addrs = self:_filter_addrs(candidate.addrs or {}, {
      peer_id = peer_id,
      purpose = "bootstrap",
    })
    for _, addr in ipairs(addrs) do
      if not is_dialable_tcp_addr(addr) then
        result.skipped = result.skipped + 1
        goto continue_addrs
      end

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
          local require_protocol = options.require_protocol ~= false
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

          if self.host and self.host.peerstore then
            self.host.peerstore:merge(discovered_peer, {
              addrs = { addr },
              protocols = { self.protocol_id },
            })
          end

          local added, add_err = self:add_peer(discovered_peer, {
            allow_replace = options.allow_replace,
          })
          if added then
            result.added = result.added + 1
          elseif add_err and is_capacity_error(add_err) then
            result.skipped = result.skipped + 1
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
      if yield then
        local yield_ok, yield_err = yield()
        if yield_ok == nil and yield_err then
          return nil, yield_err
        end
      end

      ::continue_addrs::
    end
  end

  return result
end

function DHT:refresh_once(opts)
  local options = opts or {}
  local yield = type(options.yield) == "function" and options.yield or nil
  local now = os.time()
  local min_recheck = options.min_recheck_seconds or 60
  local max_checks = options.max_checks or self.alpha
  local max_failed_checks_before_evict = options.max_failed_checks_before_evict or self._max_failed_checks_before_evict
  if type(max_checks) ~= "number" or max_checks <= 0 then
    return nil, error_mod.new("input", "max_checks must be > 0")
  end
  if type(max_failed_checks_before_evict) ~= "number" or max_failed_checks_before_evict <= 0 then
    return nil, error_mod.new("input", "max_failed_checks_before_evict must be > 0")
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
    local failed_checks = tonumber(health.failed_checks) or (health.stale == true and 1 or 0)
    local stale = failed_checks > 0
    local last_verified = health.last_verified_at or 0
    local recent = (now - last_verified) < min_recheck
    if not stale and recent then
      report.skipped = report.skipped + 1
      goto continue_peers
    end

    report.checked = report.checked + 1
    local ok, check_err = self:_supports_kad_protocol(peer_id, options.protocol_check_opts)
    if ok then
      health.failed_checks = 0
      health.stale = false
      health.last_verified_at = now
      self._peer_health[peer_id] = health
      report.healthy = report.healthy + 1
    else
      report.errors[#report.errors + 1] = check_err
      failed_checks = failed_checks + 1
      if failed_checks >= max_failed_checks_before_evict then
        local removed, remove_err = self:remove_peer(peer_id)
        if removed then
          report.removed = report.removed + 1
        elseif remove_err then
          report.errors[#report.errors + 1] = remove_err
        end
      else
        health.failed_checks = failed_checks
        health.stale = true
        health.last_disconnected_at = health.last_disconnected_at or now
        self._peer_health[peer_id] = health
      end
    end

    ::continue_peers::
  end

  return report
end

function compare_distance(left, right)
  local size = #left
  if #right < size then
    size = #right
  end
  for i = 1, size do
    local a = left:byte(i)
    local b = right:byte(i)
    if a < b then
      return -1
    end
    if a > b then
      return 1
    end
  end
  if #left < #right then
    return -1
  end
  if #left > #right then
    return 1
  end
  return 0
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

function DHT:_random_walk(opts)
  local options = opts or {}
  local report = {
    queried = 0,
    responses = 0,
    failed = 0,
    added = 0,
    skipped = 0,
    discovered = 0,
    errors = {},
  }

  local target_key = options.target_key or self.local_peer_id
  local target_bytes, target_err = protocol.peer_bytes(target_key)
  if not target_bytes then
    return nil, target_err
  end
  local target_hash, hash_err = self.routing_table:_hash(target_bytes)
  if not target_hash then
    return nil, hash_err
  end

  local alpha = options.alpha or self.alpha
  if type(alpha) ~= "number" or alpha <= 0 then
    return nil, error_mod.new("input", "alpha must be > 0")
  end
  local disjoint_paths = options.disjoint_paths or self.disjoint_paths
  if type(disjoint_paths) ~= "number" or disjoint_paths <= 0 then
    return nil, error_mod.new("input", "disjoint_paths must be > 0")
  end

  local initial_peers = self.routing_table:all_peers()
  if options.legacy_walker ~= true then
    local seeds = {}
    local initial_peer_ids = {}
    for _, entry in ipairs(initial_peers) do
      initial_peer_ids[entry.peer_id] = true
      local addrs = {}
      if self.host and self.host.peerstore then
        addrs = dialable_tcp_addrs(self:_filter_addrs(self.host.peerstore:get_addrs(entry.peer_id), {
          peer_id = entry.peer_id,
          purpose = "random_walk_seed",
        }))
      end
      if #addrs > 0 then
        seeds[#seeds + 1] = { peer_id = entry.peer_id, addrs = addrs }
      elseif not (self.host and self.host.peerstore) then
        seeds[#seeds + 1] = { peer_id = entry.peer_id }
      end
    end
    local closest, lookup = self:_get_closest_peers(target_key, {
      peers = seeds,
      alpha = alpha,
      disjoint_paths = disjoint_paths,
      count = self.k,
      scheduler_task = options.scheduler_task,
      ctx = options.ctx,
    })
    if not closest then
      return nil, lookup
    end
    report.queried = lookup.queried or 0
    report.responses = lookup.responses or 0
    report.failed = lookup.failed or 0
    report.errors = lookup.errors or {}
    report.discovered = #(lookup.closest_peers or {})
    report.termination = lookup.termination
    report.active_peak = lookup.active_peak
    for _, peer in ipairs(lookup.queried_peers or {}) do
      if initial_peer_ids[peer.peer_id] and self:find_peer(peer.peer_id) then
        goto continue
      end
      initial_peer_ids[peer.peer_id] = true
      if self:find_peer(peer.peer_id) then
        report.added = report.added + 1
        goto continue
      end
      if not self:_peerstore_supports_kad(peer.peer_id) then
        report.skipped = report.skipped + 1
        goto continue
      end
      local added, add_err = self:add_peer(peer.peer_id, { allow_replace = options.allow_replace })
      if added then
        report.added = report.added + 1
      elseif add_err and is_capacity_error(add_err) then
        report.skipped = report.skipped + 1
      elseif add_err and not options.ignore_add_errors then
        report.errors[#report.errors + 1] = add_err
      end
      ::continue::
    end
    return report
  end

  local paths = {}
  for i = 1, disjoint_paths do
    paths[i] = {}
  end
  local queued = {}
  local queried_peers = {}
  local next_seed_path = 1
  local function enqueue(path_index, peer, referrer_distance)
    if not peer or not peer.peer_id or peer.peer_id == self.local_peer_id then
      return
    end
    if queued[peer.peer_id] or queried_peers[peer.peer_id] then
      return
    end
    if peer.addrs ~= nil and type(peer.addrs) == "table" and #dialable_tcp_addrs(self:_filter_addrs(peer.addrs, {
      peer_id = peer.peer_id,
      purpose = "random_walk_enqueue",
    })) == 0 then
      return
    end

    local distance, distance_err = self:_distance_to_target(peer.peer_id, target_hash)
    if not distance then
      if not options.ignore_add_errors then
        report.errors[#report.errors + 1] = distance_err
      end
      return
    end
    if referrer_distance and compare_distance(distance, referrer_distance) >= 0 then
      return
    end

    peer._distance = distance
    queued[peer.peer_id] = true
    paths[path_index][#paths[path_index] + 1] = peer
  end

  for _, entry in ipairs(initial_peers) do
    enqueue(next_seed_path, entry)
    next_seed_path = (next_seed_path % disjoint_paths) + 1
  end

  local function queue_has_entries()
    for _, queue in ipairs(paths) do
      if #queue > 0 then
        return true
      end
    end
    return false
  end

  local function next_from_path(path_index)
    local queue = paths[path_index]
    if #queue == 0 then
      return nil
    end
    table.sort(queue, function(a, b)
      local cmp = compare_distance(a._distance, b._distance)
      if cmp == 0 then
        return a.peer_id < b.peer_id
      end
      return cmp < 0
    end)
    return table.remove(queue, 1)
  end

  local function query_target_for(entry)
    if entry.addr and not is_dialable_tcp_addr(entry.addr) then
      return entry.peer_id
    end
    if entry.addrs then
      local addrs = dialable_tcp_addrs(self:_filter_addrs(entry.addrs, {
        peer_id = entry.peer_id,
        purpose = "random_walk_query",
      }))
      if #addrs > 0 then
        return {
          peer_id = entry.peer_id,
          addrs = addrs,
        }
      end
    end
    if not entry.addr and not entry.addrs then
      return entry.peer_id
    end
    return entry
  end

  local function apply_query_result(path_index, entry, closest)
    report.responses = report.responses + 1
    report.discovered = report.discovered + #closest

    for _, candidate in ipairs(closest) do
      if candidate.peer_id then
        local added, add_err = self:add_peer(candidate.peer_id, {
          allow_replace = options.allow_replace,
        })
        if added then
          report.added = report.added + 1
        elseif add_err and is_capacity_error(add_err) then
          report.skipped = report.skipped + 1
        elseif add_err and not options.ignore_add_errors then
          report.errors[#report.errors + 1] = add_err
        end
        enqueue(path_index, candidate, entry._distance)
      end
    end
  end

  local yield = type(options.yield) == "function" and options.yield or nil

  local active = true
  while active do
    active = false
    for path_index, queue in ipairs(paths) do
      if #queue > 0 then
        active = true
        table.sort(queue, function(a, b)
          local cmp = compare_distance(a._distance, b._distance)
          if cmp == 0 then
            return a.peer_id < b.peer_id
          end
          return cmp < 0
        end)

        local batch = {}
        for _ = 1, math.min(alpha, #queue) do
          batch[#batch + 1] = table.remove(queue, 1)
        end

        for _, entry in ipairs(batch) do
          queued[entry.peer_id] = nil
          if queried_peers[entry.peer_id] then
            goto continue_queries
          end
          queried_peers[entry.peer_id] = true

          report.queried = report.queried + 1

          local query_target = entry
          if not entry.addr and not entry.addrs then
            query_target = entry.peer_id
          end

          local closest, closest_err = self:_find_node(query_target, target_key, options.find_node_opts)
          if yield then
            local yield_ok, yield_err = yield()
            if yield_ok == nil and yield_err then
              return nil, yield_err
            end
          end
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
              elseif add_err and is_capacity_error(add_err) then
                report.skipped = report.skipped + 1
              elseif add_err and not options.ignore_add_errors then
                report.errors[#report.errors + 1] = add_err
              end
              enqueue(path_index, candidate, entry._distance)
            end
          end

          ::continue_queries::
        end
      end
    end
  end

  return report
end

function DHT:random_walk(opts)
  if not (self.host and type(self.host.spawn_task) == "function") then
    return nil, error_mod.new("state", "kad_dht random_walk requires host task scheduler")
  end
  local options = opts or {}
  local task, task_err = self.host:spawn_task("kad.random_walk", function(ctx)
    local task_opts = {}
    for k, v in pairs(options) do
      task_opts[k] = v
    end
    task_opts.scheduler_task = true
    task_opts.ctx = task_opts.ctx or ctx
    task_opts.yield = task_opts.yield or function()
      return ctx:checkpoint()
    end
    task_opts.find_node_opts = task_opts.find_node_opts or {}
    task_opts.find_node_opts.ctx = task_opts.find_node_opts.ctx or ctx
    return self:_random_walk(task_opts)
  end, {
    service = "kad_dht",
  })
  if not task then
    return nil, task_err
  end
  return operation.new(self.host, task)
end

function M.new(host, opts)
  local options = opts or {}

  local address_filter, address_filter_err = normalize_address_filter(options.address_filter or options.address_filter_mode)
  if not address_filter then
    return nil, address_filter_err
  end

  local local_peer_id = options.local_peer_id
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
    disjoint_paths = options.disjoint_paths or M.DEFAULT_DISJOINT_PATHS,
    max_message_size = options.max_message_size or protocol.MAX_MESSAGE_SIZE,
    mode = options.mode or "client",
    address_filter = address_filter,
    bootstrappers = options.bootstrappers,
    peer_discovery = options.peer_discovery,
    routing_table = rt,
    _peer_health = {},
    _dnsaddr_resolver = options.dnsaddr_resolver,
    _host_on_protocols_updated = nil,
    _maintenance_enabled = options.maintenance_enabled ~= false and M.DEFAULT_MAINTENANCE_ENABLED,
    _maintenance_interval_seconds = options.maintenance_interval_seconds or M.DEFAULT_MAINTENANCE_INTERVAL_SECONDS,
    _maintenance_min_recheck_seconds = options.maintenance_min_recheck_seconds or M.DEFAULT_MAINTENANCE_MIN_RECHECK_SECONDS,
    _maintenance_max_checks = options.maintenance_max_checks or M.DEFAULT_MAINTENANCE_MAX_CHECKS,
    _maintenance_walk_every = options.maintenance_walk_every or M.DEFAULT_MAINTENANCE_WALK_EVERY,
    _maintenance_walk_timeout = options.maintenance_walk_timeout or 20,
    _maintenance_protocol_check_timeout = options.maintenance_protocol_check_timeout or 4,
    _max_failed_checks_before_evict = options.max_failed_checks_before_evict or M.DEFAULT_MAX_FAILED_CHECKS_BEFORE_EVICT,
    _maintenance_task = nil,
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
M.dnsaddr = dnsaddr
M.discovery = discovery
M.protocol = protocol

return M
