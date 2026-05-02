local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log")
local multiaddr = require("lua_libp2p.multiaddr")
local autonat_proto = require("lua_libp2p.protocol.autonat_v2")

local M = {}
M.provides = { "autonat" }
M.requires = { "identify", "ping" }

local DEFAULT_DISCOVERY_MAX_SERVERS = 12
local DEFAULT_DISCOVERY_TARGET_RESPONSES = 2
local DEFAULT_MONITOR_RETRY_INTERVAL = 5
local DEFAULT_MONITOR_HEALTHY_INTERVAL = 30
local DEFAULT_MONITOR_MIN_SUCCESS_ROUNDS = 2
local DEFAULT_CHECKED_PEER_COOLDOWN = 30

local Client = {}
Client.__index = Client

local function copy_list(values)
  local out = {}
  for i, value in ipairs(values or {}) do
    out[i] = value
  end
  return out
end

local function nonce()
  local hi = math.random(0, 0x1fffff)
  local lo = math.random(0, 0x7fffffff)
  return hi * 0x80000000 + lo
end

local function emit_event(host, name, payload)
  if not (host and type(host.emit) == "function") then
    return true
  end
  local ok, err = host:emit(name, payload)
  if not ok then
    log.warn("autonat event handler failed", {
      event = name,
      cause = tostring(err),
    })
  end
  return true
end

local function peer_id_from_target(target)
  if type(target) == "string" then
    if target:sub(1, 1) ~= "/" then
      return target
    end
    local parsed = multiaddr.parse(target)
    if parsed then
      for i = #parsed.components, 1, -1 do
        if parsed.components[i].protocol == "p2p" then
          return parsed.components[i].value
        end
      end
    end
  elseif type(target) == "table" then
    return target.peer_id or target.peerId
  end
  return nil
end

local function addrs_for_proto(addrs, required_proto)
  if required_proto == nil then
    return copy_list(addrs or {})
  end
  local out = {}
  for _, addr in ipairs(addrs or {}) do
    if type(addr) == "string" then
      local parsed = multiaddr.parse(addr)
      if parsed and type(parsed.components) == "table" then
        for _, component in ipairs(parsed.components) do
          if component.protocol == required_proto then
            out[#out + 1] = addr
            break
          end
        end
      end
    end
  end
  return out
end

local function candidate_addrs(host, opts)
  local options = opts or {}
  local source = options.addrs
  if source == nil and host and host.address_manager and type(host.address_manager.get_observed_addrs) == "function" then
    source = host.address_manager:get_observed_addrs()
  end
  if source == nil and host and type(host.get_multiaddrs_raw) == "function" then
    source = host:get_multiaddrs_raw()
  end

  local out = {}
  for _, addr in ipairs(source or {}) do
    if type(addr) == "string" and addr ~= "" then
      if multiaddr.is_relay_addr(addr) and not options.allow_relay_addrs then
        goto continue_addr
      end
      if options.allow_private_addrs or not multiaddr.is_private_addr(addr) then
        out[#out + 1] = addr
      end
    end
    ::continue_addr::
  end
  return out
end

local function encode_addrs(addrs)
  local out = {}
  for _, addr in ipairs(addrs or {}) do
    local bytes, err = multiaddr.to_bytes(addr)
    if not bytes then
      return nil, err
    end
    out[#out + 1] = bytes
  end
  return out
end

local function response_name(map, code)
  for name, value in pairs(map) do
    if value == code then
      return name
    end
  end
  return tostring(code)
end

function Client:_record_result(result)
  local addr = result.addr
  if addr and self.host and self.host.address_manager and type(self.host.address_manager.set_reachability) == "function" then
    self.host.address_manager:set_reachability(addr, {
      status = result.reachable and "public" or "private",
      source = "autonat_v2",
      type = result.type or "observed",
      verified = result.reachable == true,
      checked_at = result.checked_at,
      server_peer_id = result.server_peer_id,
      response_status = result.response_status,
      dial_status = result.dial_status,
    })
  end
  self.results[addr or tostring(result.nonce)] = result
end

function Client:_handle_dial_data_request(stream, request)
  if self.allow_dial_data == false then
    return nil, error_mod.new("permission", "autonat dial data rejected")
  end
  local total = tonumber(request.numBytes) or 0
  if total > self.max_dial_data_bytes then
    return nil, error_mod.new("permission", "autonat dial data request too large", {
      requested = total,
      max = self.max_dial_data_bytes,
    })
  end
  local chunk_size = math.min(self.dial_data_chunk_size, autonat_proto.DIAL_DATA_CHUNK_SIZE)
  local chunk = string.rep("\0", chunk_size)
  local sent = 0
  while sent < total do
    local n = math.min(chunk_size, total - sent)
    local ok, err = autonat_proto.write_message(stream, {
      dialDataResponse = { data = chunk:sub(1, n) },
    })
    if not ok then
      return nil, err
    end
    sent = sent + n
  end
  return true
end

function Client:check(server, opts)
  local options = opts or {}
  if (not options.stream_opts or options.stream_opts.ctx == nil)
    and self.host
    and type(self.host.spawn_task) == "function"
    and type(self.host.run_until_task) == "function"
  then
    local task, task_err = self.host:spawn_task("autonat.check.inline", function(ctx)
      local task_opts = {}
      for k, v in pairs(options) do
        task_opts[k] = v
      end
      task_opts.stream_opts = task_opts.stream_opts or {}
      task_opts.stream_opts.ctx = task_opts.stream_opts.ctx or ctx
      return self:check(server, task_opts)
    end, { service = "autonat" })
    if not task then
      return nil, task_err
    end
    return self.host:run_until_task(task, {
      timeout = options.timeout,
      poll_interval = options.poll_interval,
    })
  end

  local addrs = candidate_addrs(self.host, options)
  if #addrs == 0 then
    return nil, error_mod.new("state", "autonat check requires at least one public candidate address")
  end
  local encoded_addrs, addrs_err = encode_addrs(addrs)
  if not encoded_addrs then
    return nil, addrs_err
  end
  if not (self.host and type(self.host.new_stream) == "function") then
    return nil, error_mod.new("state", "autonat client requires host:new_stream")
  end

  local request_nonce = options.nonce or nonce()
  self._pending[request_nonce] = {
    server_peer_id = peer_id_from_target(server),
    addrs = copy_list(addrs),
    started_at = os.time(),
  }

  local stream, selected, _, stream_err = self.host:new_stream(server, { autonat_proto.DIAL_REQUEST_ID }, options.stream_opts)
  if not stream then
    self._pending[request_nonce] = nil
    return nil, stream_err
  end

  local wrote, write_err = autonat_proto.write_message(stream, {
    dialRequest = {
      addrs = encoded_addrs,
      nonce = request_nonce,
    },
  })
  if not wrote then
    self._pending[request_nonce] = nil
    pcall(function() stream:close() end)
    return nil, write_err
  end

  local response
  while true do
    local message, read_err = autonat_proto.read_message(stream, {
      max_message_size = self.max_message_size,
    })
    if not message then
      self._pending[request_nonce] = nil
      pcall(function() stream:close() end)
      return nil, read_err
    end
    if message.dialDataRequest then
      local ok, err = self:_handle_dial_data_request(stream, message.dialDataRequest)
      if not ok then
        self._pending[request_nonce] = nil
        pcall(function() stream:close() end)
        return nil, err
      end
    elseif message.dialResponse then
      response = message.dialResponse
      break
    else
      self._pending[request_nonce] = nil
      pcall(function() stream:close() end)
      return nil, error_mod.new("protocol", "unexpected autonat response message")
    end
  end

  pcall(function() stream:close() end)
  self._pending[request_nonce] = nil

  if response.status ~= autonat_proto.RESPONSE_STATUS.OK then
    local failed = {
      server_peer_id = peer_id_from_target(server),
      nonce = request_nonce,
      selected_protocol = selected,
      response_status = response_name(autonat_proto.RESPONSE_STATUS, response.status),
      dial_status = response_name(autonat_proto.DIAL_STATUS, response.dialStatus),
      checked_at = os.time(),
      reachable = false,
    }
    emit_event(self.host, "autonat:request:failed", failed)
    return failed
  end

  local addr_index = (response.addrIdx or 0) + 1
  local result = {
    server_peer_id = peer_id_from_target(server),
    nonce = request_nonce,
    selected_protocol = selected,
    addr = addrs[addr_index],
    addr_index = response.addrIdx,
    response_status = response_name(autonat_proto.RESPONSE_STATUS, response.status),
    dial_status = response_name(autonat_proto.DIAL_STATUS, response.dialStatus),
    checked_at = os.time(),
    reachable = response.dialStatus == autonat_proto.DIAL_STATUS.OK,
    dial_back_verified = self._verified[request_nonce] == true,
    type = options.type or "observed",
  }
  self:_record_result(result)
  emit_event(self.host, "autonat:address:checked", result)
  if result.reachable then
    emit_event(self.host, "autonat:address:reachable", result)
  else
    emit_event(self.host, "autonat:address:unreachable", result)
  end
  self._verified[request_nonce] = nil
  return result
end

function Client:start_check(server, opts)
  if not (self.host and type(self.host.spawn_task) == "function") then
    return nil, error_mod.new("state", "autonat start_check requires host task scheduler")
  end
  local options = opts or {}
  return self.host:spawn_task("autonat.check", function(ctx)
    local check_opts = {}
    for k, v in pairs(options) do
      check_opts[k] = v
    end
    check_opts.stream_opts = check_opts.stream_opts or {}
    check_opts.stream_opts.ctx = check_opts.stream_opts.ctx or ctx
    return self:check(server, check_opts)
  end, {
    service = "autonat",
  })
end

function Client:_discover_servers(limit, opts)
  local options = opts or {}
  local host = self.host
  local checked_peers = options.checked_peers or {}
  local checked_peer_cooldown = options.checked_peer_cooldown_seconds or 0
  local now = os.time()
  local max_candidates = limit or 1
  local required_addr_proto = options.required_addr_proto
  local walk_triggered = false

  local function collect_candidates()
    local candidates = {}
    local proto_candidates = 0
    local all_peers = (host and host.peerstore and host.peerstore:all()) or {}
    for _, peer in ipairs(all_peers) do
      local checked_at = checked_peers[peer.peer_id]
      local recently_checked = checked_at == true
        or (type(checked_at) == "number" and checked_peer_cooldown > 0 and (now - checked_at) < checked_peer_cooldown)
      local peer_addrs = addrs_for_proto(host.peerstore:get_addrs(peer.peer_id), required_addr_proto)
      if not recently_checked
        and host.peerstore:supports_protocol(peer.peer_id, autonat_proto.DIAL_REQUEST_ID)
        and #peer_addrs > 0
      then
        proto_candidates = proto_candidates + 1
        candidates[#candidates + 1] = {
          peer_id = peer.peer_id,
          addrs = peer_addrs,
        }
        if #candidates >= max_candidates then
          break
        end
      end
    end

    if #candidates == 0 and options.allow_fallback ~= false then
      for _, peer in ipairs(all_peers) do
        local checked_at = checked_peers[peer.peer_id]
        local recently_checked = checked_at == true
          or (type(checked_at) == "number" and checked_peer_cooldown > 0 and (now - checked_at) < checked_peer_cooldown)
        local peer_addrs = addrs_for_proto(host.peerstore:get_addrs(peer.peer_id), required_addr_proto)
        if not recently_checked and #peer_addrs > 0 then
          candidates[#candidates + 1] = {
            peer_id = peer.peer_id,
            addrs = peer_addrs,
            fallback = true,
          }
          if #candidates >= max_candidates then
            break
          end
        end
      end
    end

    return candidates, proto_candidates, #all_peers
  end

  local out, proto_candidates, peerstore_peers = collect_candidates()

  if #out == 0 and host and host.kad_dht and options.enable_walk ~= false and type(host.kad_dht.random_walk) == "function" then
    walk_triggered = true
    local walk_op = host.kad_dht:random_walk({
      count = options.dht_count or 20,
      alpha = options.dht_alpha or 3,
      disjoint_paths = options.dht_paths or 2,
    })
    if walk_op then
      local walk_report = walk_op:result({
        ctx = options.ctx,
        timeout = options.walk_timeout or 12,
      })
      emit_event(host, "autonat:discovery:walk", {
        report = walk_report,
        routing_table_peers = #(host.kad_dht.routing_table:all_peers() or {}),
      })
    end
    out, proto_candidates, peerstore_peers = collect_candidates()
  end

  emit_event(host, "autonat:discovery:candidates", {
    peerstore_peers = peerstore_peers,
    protocol_candidates = proto_candidates,
    candidates = out,
    required_addr_proto = required_addr_proto,
    walk_triggered = walk_triggered,
  })

  return out
end

function Client:_discover(opts)
  local options = opts or {}
  local host = self.host
  local stats = {
    checked = 0,
    responses = 0,
    refused = 0,
    reachable = 0,
  }
  local checked_peers = options.checked_peers or {}
  local persist_checked_peers = options.preserve_checked_peers == true
  local deadline = os.time() + (options.discovery_timeout or 90)
  emit_event(host, "autonat:discovery:started", { deadline = deadline })

  local max_servers = options.max_autonat_servers or DEFAULT_DISCOVERY_MAX_SERVERS
  local target_responses = options.target_autonat_responses or DEFAULT_DISCOVERY_TARGET_RESPONSES

  while stats.checked < max_servers
    and (stats.responses < target_responses or stats.reachable == 0)
  do
    if options.stop_on_first_reachable and stats.reachable > 0 then
      break
    end
    local candidates = self:_discover_servers(max_servers, {
      checked_peers = checked_peers,
      checked_peer_cooldown_seconds = options.checked_peer_cooldown_seconds,
      required_addr_proto = options.required_addr_proto,
      seed_wait_seconds = options.seed_wait_seconds,
      dht_count = options.dht_count,
      dht_alpha = options.dht_alpha,
      dht_paths = options.dht_paths,
      walk_timeout = options.walk_timeout,
      poll_interval = options.poll_interval,
      allow_fallback = options.allow_fallback,
      ctx = options.ctx,
    })

    if #candidates == 0 then
      if os.time() >= deadline then
        break
      end
      if type(host.sleep) == "function" then
        host:sleep(options.retry_delay_seconds or 2, {
          ctx = options.ctx,
          poll_interval = options.poll_interval,
        })
      end
    else
      for _, server in ipairs(candidates) do
        if stats.checked >= max_servers then
          break
        end
        local peer_id = server.peer_id
        if checked_peers[peer_id] then
          goto continue_server
        end
        checked_peers[peer_id] = os.time()
        stats.checked = stats.checked + 1

        local check_opts = {
          timeout = options.check_timeout or 8,
          type = options.check_type or "observed",
          stream_opts = {
            ctx = options.ctx,
          },
        }
        if type(options.check_opts_builder) == "function" then
          local built = options.check_opts_builder(server, stats, options.ctx)
          if type(built) == "table" then
            check_opts = built
            check_opts.stream_opts = check_opts.stream_opts or {}
            check_opts.stream_opts.ctx = check_opts.stream_opts.ctx or options.ctx
          end
        end

        local result, check_err = self:check(server, check_opts)
        if result then
          if result.response_status == "E_DIAL_REFUSED" then
            stats.refused = stats.refused + 1
          else
            stats.responses = stats.responses + 1
          end
          if result.reachable == true then
            stats.reachable = stats.reachable + 1
          end
        else
          emit_event(host, "autonat:discovery:check_failed", {
            peer_id = peer_id,
            error = check_err,
          })
        end
        emit_event(host, "autonat:discovery:progress", stats)
        ::continue_server::
      end
    end

    if os.time() >= deadline then
      break
    end
  end

  if options.drain_seconds and options.drain_seconds > 0 and type(host.sleep) == "function" then
    host:sleep(options.drain_seconds, {
      ctx = options.ctx,
      poll_interval = options.poll_interval,
    })
  end
  emit_event(host, "autonat:discovery:completed", stats)
  if persist_checked_peers then
    stats.checked_peers = checked_peers
  end
  return stats
end

function Client:start_discovery(opts)
  if not (self.host and type(self.host.spawn_task) == "function") then
    return nil, error_mod.new("state", "autonat start_discovery requires host task scheduler")
  end
  local options = opts or {}
  return self.host:spawn_task("autonat.discovery", function(ctx)
    local discover_opts = {}
    for k, v in pairs(options) do
      discover_opts[k] = v
    end
    discover_opts.ctx = discover_opts.ctx or ctx
    return self:_discover(discover_opts)
  end, {
    service = "autonat",
  })
end

function Client:start_monitor(opts)
  if not (self.host and type(self.host.spawn_task) == "function") then
    return nil, error_mod.new("state", "autonat start_monitor requires host task scheduler")
  end
  if self._monitor_task and self._monitor_task.status ~= "completed" and self._monitor_task.status ~= "failed" and self._monitor_task.status ~= "cancelled" then
    return self._monitor_task
  end
  local options = opts or {}
  local task, task_err = self.host:spawn_task("autonat.monitor", function(ctx)
    local checked_peers = {}
    local min_success_rounds = options.min_success_rounds or DEFAULT_MONITOR_MIN_SUCCESS_ROUNDS
    local retry_interval = options.retry_interval_seconds or DEFAULT_MONITOR_RETRY_INTERVAL
    local healthy_interval = options.healthy_interval_seconds or DEFAULT_MONITOR_HEALTHY_INTERVAL
    local round_timeout = options.round_timeout_seconds or 30
    local success_rounds = 0
    local rounds = 0
    emit_event(self.host, "autonat:monitor:started", {
      min_success_rounds = min_success_rounds,
      retry_interval_seconds = retry_interval,
      healthy_interval_seconds = healthy_interval,
    })
    while true do
      rounds = rounds + 1
      emit_event(self.host, "autonat:monitor:round_started", { round = rounds })
      local stats = self:_discover({
        ctx = ctx,
        discovery_timeout = round_timeout,
        seed_wait_seconds = options.seed_wait_seconds,
        walk_timeout = options.walk_timeout,
        dht_alpha = options.dht_alpha,
        dht_paths = options.dht_paths,
        dht_count = options.dht_count,
        max_autonat_servers = options.max_autonat_servers,
        target_autonat_responses = options.target_autonat_responses,
        stop_on_first_reachable = options.stop_on_first_reachable,
        drain_seconds = options.drain_seconds,
        check_type = options.check_type,
        check_timeout = options.check_timeout,
        poll_interval = options.poll_interval,
        check_opts_builder = options.check_opts_builder,
        checked_peers = checked_peers,
        preserve_checked_peers = true,
        checked_peer_cooldown_seconds = options.checked_peer_cooldown_seconds or DEFAULT_CHECKED_PEER_COOLDOWN,
        required_addr_proto = options.required_addr_proto,
      })
      checked_peers = stats.checked_peers or checked_peers
      if stats.reachable and stats.reachable > 0 then
        success_rounds = success_rounds + 1
      else
        success_rounds = 0
      end
      local verified = success_rounds >= min_success_rounds
      emit_event(self.host, "autonat:monitor:round_completed", {
        round = rounds,
        success_rounds = success_rounds,
        verified = verified,
        stats = stats,
      })
      if verified then
        emit_event(self.host, "autonat:monitor:verified", {
          round = rounds,
          success_rounds = success_rounds,
          stats = stats,
        })
      end
      local next_sleep = verified and healthy_interval or retry_interval
      local ok, sleep_err = ctx:sleep(next_sleep)
      if ok == nil and sleep_err then
        emit_event(self.host, "autonat:monitor:stopped", {
          round = rounds,
          cause = tostring(sleep_err),
        })
        return nil, sleep_err
      end
    end
  end, {
    service = "autonat",
  })
  if not task then
    return nil, task_err
  end
  self._monitor_task = task
  return task
end

function Client:_handle_dial_back(stream)
  local message, read_err = autonat_proto.read_dial_back(stream, {
    max_message_size = self.max_message_size,
  })
  if not message then
    return nil, read_err
  end
  if not self._pending[message.nonce] then
    return nil, error_mod.new("protocol", "autonat dial-back nonce not pending")
  end
  self._verified[message.nonce] = true
  local ok, write_err = autonat_proto.write_dial_back_response(stream, {
    status = autonat_proto.DIAL_BACK_STATUS.OK,
  })
  if not ok then
    return nil, write_err
  end
  if type(stream.close) == "function" then
    pcall(function() stream:close() end)
  end
  return true
end

function Client:start()
  if self.started then
    return true
  end
  if not (self.host and type(self.host.handle) == "function") then
    return nil, error_mod.new("state", "autonat service requires host:handle")
  end
  local ok, err = self.host:handle(autonat_proto.DIAL_BACK_ID, function(stream)
    return self:_handle_dial_back(stream)
  end)
  if not ok then
    return nil, err
  end
  self.started = true
  return true
end

function Client:stop()
  if self._monitor_task and self.host and type(self.host.cancel_task) == "function" then
    self.host:cancel_task(self._monitor_task.id)
  end
  self._monitor_task = nil
  self.started = false
  return true
end

function Client:status()
  local pending = 0
  for _ in pairs(self._pending) do
    pending = pending + 1
  end
  return {
    started = self.started,
    pending = pending,
    results = self.results,
  }
end

function M.new(host, opts)
  if type(host) ~= "table" then
    return nil, error_mod.new("input", "autonat client requires host")
  end
  local options = opts or {}
  return setmetatable({
    host = host,
    allow_dial_data = options.allow_dial_data ~= false,
    max_dial_data_bytes = options.max_dial_data_bytes or (100 * 1024),
    dial_data_chunk_size = options.dial_data_chunk_size or autonat_proto.DIAL_DATA_CHUNK_SIZE,
    max_message_size = options.max_message_size or autonat_proto.MAX_MESSAGE_SIZE,
    _pending = {},
    _verified = {},
    _monitor_task = nil,
    results = {},
    started = false,
  }, Client)
end

M.Client = Client
M.DIAL_REQUEST_ID = autonat_proto.DIAL_REQUEST_ID
M.DIAL_BACK_ID = autonat_proto.DIAL_BACK_ID

return M
