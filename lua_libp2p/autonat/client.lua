--- AutoNAT v2 client/service.
---@class Libp2pAutoNatConfig
---@field allow_dial_data? boolean Allow AutoNAT v2 dial-data requests. Default: true.
---@field max_dial_data_bytes? integer Maximum dial-data bytes accepted/sent.
---@field dial_data_chunk_size? integer Dial-data chunk size.
---@field max_message_size? integer Maximum AutoNAT message size.
---@field monitor_on_start? boolean Start monitor when host starts. Default: false.
---@field monitor_start_opts? table Monitor options used when `monitor_on_start=true`.
---@field monitor_opts? table Alias for `monitor_start_opts`.

local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log").subsystem("autonat")
local multiaddr = require("lua_libp2p.multiformats.multiaddr")
local autonat_proto = require("lua_libp2p.protocol.autonat_v2")
local table_utils = require("lua_libp2p.util.tables")

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

local copy_list = table_utils.copy_list

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

local function target_label(target)
  if type(target) == "table" then
    return target.peer_id or target.peerId or target.addr or target.multiaddr
  end
  return target
end

local function should_backoff_peer(err)
  if error_mod.is_error(err) then
    if err.kind == "unsupported" or err.kind == "timeout" or err.kind == "closed" or err.kind == "protocol" then
      return true
    end
  end
  local text = tostring(err or ""):lower()
  return string.find(text, "no common protocol", 1, true) ~= nil
    or string.find(text, "unsupported", 1, true) ~= nil
    or string.find(text, "stream reset", 1, true) ~= nil
    or string.find(text, "timed out", 1, true) ~= nil
    or string.find(text, "timeout", 1, true) ~= nil
    or string.find(text, "closed", 1, true) ~= nil
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
  if
    source == nil
    and host
    and host.address_manager
    and type(host.address_manager.get_self_observed_addrs) == "function"
  then
    source = host.address_manager:get_self_observed_addrs()
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
  if addr and self.host and self.host.address_manager then
    local metadata = {
      status = result.reachable and "public" or "private",
      source = "autonat_v2",
      type = result.type or "observed",
      verified = result.reachable == true and result.dial_back_verified == true,
      checked_at = result.checked_at,
      server_peer_id = result.server_peer_id,
      response_status = result.response_status,
      dial_status = result.dial_status,
    }
    if
      result.type == "ip-mapping"
      and metadata.verified
      and type(self.host.address_manager.verify_public_address_mapping) == "function"
    then
      self.host.address_manager:verify_public_address_mapping(addr, metadata)
    elseif type(self.host.address_manager.set_reachability) == "function" then
      self.host.address_manager:set_reachability(addr, metadata)
    end
    if type(self.host._emit_self_peer_update_if_changed) == "function" then
      self.host:_emit_self_peer_update_if_changed()
    end
  end
  self.results[addr or tostring(result.nonce)] = result
  log.debug("autonat result recorded", {
    server_peer_id = result.server_peer_id,
    addr = result.addr,
    reachable = result.reachable == true,
    dial_back_verified = result.dial_back_verified == true,
    response_status = result.response_status,
    dial_status = result.dial_status,
    type = result.type,
  })
end

function Client:_handle_dial_data_request(stream, request)
  if self.allow_dial_data == false then
    log.debug("autonat dial data rejected", {
      reason = "disabled",
      requested_bytes = tonumber(request.numBytes) or 0,
    })
    return nil, error_mod.new("permission", "autonat dial data rejected")
  end
  local total = tonumber(request.numBytes) or 0
  if total > self.max_dial_data_bytes then
    log.debug("autonat dial data rejected", {
      reason = "too_large",
      requested_bytes = total,
      max_bytes = self.max_dial_data_bytes,
    })
    return nil,
      error_mod.new("permission", "autonat dial data request too large", {
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
  log.debug("autonat dial data sent", {
    bytes = sent,
  })
  return true
end

--- Run a synchronous AutoNAT check against one server.
-- `opts` may include `stream_opts`, `timeout`, `poll_interval`, `nonce`,
-- `addrs`, `allow_private_addrs`, and `allow_relay_addrs`.
--- server string|table Server peer target.
--- opts? table Check options.
--- table|nil result
--- table|nil err
function Client:check(server, opts)
  local options = opts or {}
  if
    (not options.stream_opts or options.stream_opts.ctx == nil)
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
    log.debug("autonat check skipped", {
      server_peer_id = peer_id_from_target(server),
      target = target_label(server),
      reason = "no_candidate_addrs",
    })
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
  log.debug("autonat check started", {
    server_peer_id = peer_id_from_target(server),
    target = target_label(server),
    nonce = request_nonce,
    addrs = #addrs,
  })
  self._pending[request_nonce] = {
    server_peer_id = peer_id_from_target(server),
    addrs = copy_list(addrs),
    started_at = os.time(),
  }

  local stream, selected, _, stream_err =
    self.host:new_stream(server, { autonat_proto.DIAL_REQUEST_ID }, options.stream_opts)
  if not stream then
    self._pending[request_nonce] = nil
    log.debug("autonat check stream failed", {
      server_peer_id = peer_id_from_target(server),
      nonce = request_nonce,
      cause = tostring(stream_err),
    })
    return nil, stream_err
  end
  log.debug("autonat check stream opened", {
    server_peer_id = peer_id_from_target(server),
    nonce = request_nonce,
    protocol = selected,
  })

  local wrote, write_err = autonat_proto.write_message(stream, {
    dialRequest = {
      addrs = encoded_addrs,
      nonce = request_nonce,
    },
  })
  if not wrote then
    self._pending[request_nonce] = nil
    pcall(function()
      stream:close()
    end)
    log.debug("autonat dial request write failed", {
      server_peer_id = peer_id_from_target(server),
      nonce = request_nonce,
      cause = tostring(write_err),
    })
    return nil, write_err
  end
  log.debug("autonat dial request sent", {
    server_peer_id = peer_id_from_target(server),
    nonce = request_nonce,
    addrs = #encoded_addrs,
  })

  local response
  while true do
    local message, read_err = autonat_proto.read_message(stream, {
      max_message_size = self.max_message_size,
    })
    if not message then
      self._pending[request_nonce] = nil
      pcall(function()
        stream:close()
      end)
      log.debug("autonat response read failed", {
        server_peer_id = peer_id_from_target(server),
        nonce = request_nonce,
        cause = tostring(read_err),
      })
      return nil, read_err
    end
    if message.dialDataRequest then
      log.debug("autonat dial data requested", {
        server_peer_id = peer_id_from_target(server),
        nonce = request_nonce,
        requested_bytes = tonumber(message.dialDataRequest.numBytes) or 0,
      })
      local ok, err = self:_handle_dial_data_request(stream, message.dialDataRequest)
      if not ok then
        self._pending[request_nonce] = nil
        pcall(function()
          stream:close()
        end)
        log.debug("autonat dial data failed", {
          server_peer_id = peer_id_from_target(server),
          nonce = request_nonce,
          cause = tostring(err),
        })
        return nil, err
      end
    elseif message.dialResponse then
      response = message.dialResponse
      log.debug("autonat dial response received", {
        server_peer_id = peer_id_from_target(server),
        nonce = request_nonce,
        response_status = response_name(autonat_proto.RESPONSE_STATUS, response.status),
        dial_status = response_name(autonat_proto.DIAL_STATUS, response.dialStatus),
        addr_index = response.addrIdx,
      })
      break
    else
      self._pending[request_nonce] = nil
      pcall(function()
        stream:close()
      end)
      log.debug("autonat unexpected response", {
        server_peer_id = peer_id_from_target(server),
        nonce = request_nonce,
      })
      return nil, error_mod.new("protocol", "unexpected autonat response message")
    end
  end

  pcall(function()
    stream:close()
  end)
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
    log.debug("autonat check completed", {
      server_peer_id = failed.server_peer_id,
      nonce = request_nonce,
      reachable = false,
      response_status = failed.response_status,
      dial_status = failed.dial_status,
    })
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
  emit_event(self.host, "autonat:reachability:checked", result)
  if result.reachable then
    emit_event(self.host, "autonat:address:reachable", result)
  else
    emit_event(self.host, "autonat:address:unreachable", result)
  end
  log.debug("autonat check completed", {
    server_peer_id = result.server_peer_id,
    nonce = request_nonce,
    addr = result.addr,
    reachable = result.reachable == true,
    dial_back_verified = result.dial_back_verified == true,
    response_status = result.response_status,
    dial_status = result.dial_status,
  })
  self._verified[request_nonce] = nil
  return result
end

--- Spawn an asynchronous AutoNAT check task.
--- server string|table Server peer target.
--- opts? table Same options as @{check}.
-- `opts.stream_opts.ctx` is injected automatically when omitted.
--- table|nil task
--- table|nil err
function Client:start_check(server, opts)
  if not (self.host and type(self.host.spawn_task) == "function") then
    return nil, error_mod.new("state", "autonat start_check requires host task scheduler")
  end
  local options = opts or {}
  log.debug("autonat check task scheduled", {
    server_peer_id = peer_id_from_target(server),
    target = target_label(server),
  })
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

--- Select candidate AutoNAT servers from peerstore/discovery.
-- `opts.checked_peers` tracks cooldown state by peer id.
-- `opts.checked_peer_cooldown_seconds` suppresses recently checked peers.
-- `opts.required_addr_proto` filters by `ip4`/`ip6` preference.
function Client:_discover_servers(limit, opts)
  local options = opts or {}
  local host = self.host
  local checked_peers = options.checked_peers or {}
  local backoff_peers = options.backoff_peers or {}
  local backoff_seconds = options.peer_backoff_seconds or DEFAULT_CHECKED_PEER_COOLDOWN
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
      local backed_off_at = backoff_peers[peer.peer_id]
      local backed_off = backed_off_at == true
        or (type(backed_off_at) == "number" and backoff_seconds > 0 and (now - backed_off_at) < backoff_seconds)
      local peer_addrs = addrs_for_proto(host.peerstore:get_addrs(peer.peer_id), required_addr_proto)
      if
        not recently_checked
        and not backed_off
        and host.peerstore:supports_protocol(peer.peer_id, autonat_proto.DIAL_REQUEST_ID)
        and #peer_addrs > 0
      then
        proto_candidates = proto_candidates + 1
        if #candidates < max_candidates then
          candidates[#candidates + 1] = {
            peer_id = peer.peer_id,
            addrs = peer_addrs,
          }
        end
      end
    end

    if #candidates == 0 and options.allow_fallback ~= false then
      for _, peer in ipairs(all_peers) do
        local checked_at = checked_peers[peer.peer_id]
        local recently_checked = checked_at == true
          or (type(checked_at) == "number" and checked_peer_cooldown > 0 and (now - checked_at) < checked_peer_cooldown)
        local backed_off_at = backoff_peers[peer.peer_id]
        local backed_off = backed_off_at == true
          or (type(backed_off_at) == "number" and backoff_seconds > 0 and (now - backed_off_at) < backoff_seconds)
        local peer_addrs = addrs_for_proto(host.peerstore:get_addrs(peer.peer_id), required_addr_proto)
        if not recently_checked and not backed_off and #peer_addrs > 0 then
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

  if
    #out == 0
    and host
    and host.kad_dht
    and options.enable_walk ~= false
    and type(host.kad_dht.random_walk) == "function"
  then
    walk_triggered = true
    log.debug("autonat discovery random walk started", {
      required_addr_proto = required_addr_proto,
    })
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
      log.debug("autonat discovery random walk completed", {
        routing_table_peers = #(host.kad_dht.routing_table:all_peers() or {}),
        report = type(walk_report) == "table" and walk_report.termination or nil,
      })
    end
    out, proto_candidates, peerstore_peers = collect_candidates()
  end

  local snapshot = {
    peerstore_peers = peerstore_peers,
    protocol_candidates = proto_candidates,
    candidates = #out,
    required_addr_proto = required_addr_proto,
  }
  local previous = self._last_discovery_candidate_snapshot
  local changed = not previous
    or previous.peerstore_peers ~= snapshot.peerstore_peers
    or previous.protocol_candidates ~= snapshot.protocol_candidates
    or previous.candidates ~= snapshot.candidates
    or previous.required_addr_proto ~= snapshot.required_addr_proto
  if changed or walk_triggered then
    self._last_discovery_candidate_snapshot = snapshot
    emit_event(host, "autonat:discovery:candidates", {
      peerstore_peers = peerstore_peers,
      protocol_candidates = proto_candidates,
      candidates = out,
      required_addr_proto = required_addr_proto,
      walk_triggered = walk_triggered,
    })
  end
  if (#out > 0 and changed) or walk_triggered then
    log.debug("autonat discovery candidates ready", {
      peerstore_peers = peerstore_peers,
      protocol_candidates = proto_candidates,
      candidates = #out,
      required_addr_proto = required_addr_proto,
      walk_triggered = walk_triggered,
    })
  end

  return out
end

--- Internal AutoNAT discovery/check execution.
-- `opts.max_autonat_servers`, `opts.target_autonat_responses`, and
-- `opts.stop_on_first_reachable` shape check strategy.
-- `opts.discovery_timeout_seconds`, `opts.check_timeout`, `opts.drain_seconds`
-- control timing.
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
  log.debug("autonat discovery started", {
    deadline = deadline,
    max_autonat_servers = options.max_autonat_servers or DEFAULT_DISCOVERY_MAX_SERVERS,
    target_autonat_responses = options.target_autonat_responses or DEFAULT_DISCOVERY_TARGET_RESPONSES,
  })

  local max_servers = options.max_autonat_servers or DEFAULT_DISCOVERY_MAX_SERVERS
  local target_responses = options.target_autonat_responses or DEFAULT_DISCOVERY_TARGET_RESPONSES
  local logged_no_candidates = false
  local function run_check(server, check_opts)
    local timeout = tonumber(check_opts.timeout)
    if
      not (
        options.ctx
        and type(options.ctx.await_any_task) == "function"
        and host
        and type(host.spawn_task) == "function"
        and type(host.cancel_task) == "function"
      )
    then
      return self:check(server, check_opts)
    end

    local check_task, check_task_err = host:spawn_task("autonat.check.discovery", function(task_ctx)
      local task_opts = {}
      for k, v in pairs(check_opts) do
        task_opts[k] = v
      end
      task_opts.stream_opts = task_opts.stream_opts or {}
      task_opts.stream_opts.ctx = task_opts.stream_opts.ctx or task_ctx
      return self:check(server, task_opts)
    end, { service = "autonat" })
    if not check_task then
      return nil, check_task_err
    end

    local timeout_task
    if timeout and timeout > 0 then
      local timeout_task_err
      timeout_task, timeout_task_err = host:spawn_task("autonat.check.timeout", function(timeout_ctx)
        local slept, sleep_err = timeout_ctx:sleep(timeout)
        if slept == nil and sleep_err then
          return nil, sleep_err
        end
        return true
      end, { service = "autonat" })
      if not timeout_task then
        host:cancel_task(check_task.id)
        return nil, timeout_task_err
      end
    end

    if timeout_task then
      local _, wait_err = options.ctx:await_any_task({ check_task, timeout_task })
      if wait_err then
        host:cancel_task(check_task.id)
        host:cancel_task(timeout_task.id)
        return nil, wait_err
      end
      if timeout_task.status == "completed" and check_task.status ~= "completed" and check_task.status ~= "failed" then
        host:cancel_task(check_task.id)
        return nil,
          error_mod.new("timeout", "autonat check timed out", {
            peer_id = server.peer_id,
            timeout = timeout,
          })
      end
      if check_task.status == "completed" then
        host:cancel_task(timeout_task.id)
      end
    else
      local _, wait_err = options.ctx:await_task(check_task)
      if wait_err then
        return nil, wait_err
      end
    end

    if check_task.status ~= "completed" then
      return nil,
        check_task.error or error_mod.new("state", "autonat check did not complete", {
          peer_id = server.peer_id,
          status = check_task.status,
        })
    end
    return check_task.result
  end

  while stats.checked < max_servers and (stats.responses < target_responses or stats.reachable == 0) do
    if options.stop_on_first_reachable and stats.reachable > 0 then
      break
    end
    local candidates = self:_discover_servers(max_servers, {
      checked_peers = checked_peers,
      backoff_peers = options.backoff_peers,
      peer_backoff_seconds = options.peer_backoff_seconds,
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
      if not logged_no_candidates then
        logged_no_candidates = true
        log.debug("autonat discovery no candidates", {
          checked = stats.checked,
          responses = stats.responses,
          reachable = stats.reachable,
        })
      end
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
      logged_no_candidates = false
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

        local result, check_err = run_check(server, check_opts)
        if result then
          log.debug("autonat discovery check completed", {
            peer_id = peer_id,
            response_status = result.response_status,
            dial_status = result.dial_status,
            reachable = result.reachable == true,
          })
          if result.response_status == "E_DIAL_REFUSED" then
            stats.refused = stats.refused + 1
          else
            stats.responses = stats.responses + 1
          end
          if result.reachable == true then
            stats.reachable = stats.reachable + 1
          end
        else
          log.debug("autonat discovery check failed", {
            peer_id = peer_id,
            cause = tostring(check_err),
          })
          if options.backoff_peers and should_backoff_peer(check_err) then
            options.backoff_peers[peer_id] = os.time()
            log.debug("autonat discovery peer backed off", {
              peer_id = peer_id,
              backoff_seconds = options.peer_backoff_seconds or DEFAULT_CHECKED_PEER_COOLDOWN,
              cause = tostring(check_err),
            })
          end
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
  log.debug("autonat discovery completed", stats)
  if persist_checked_peers then
    stats.checked_peers = checked_peers
  end
  return stats
end

--- Run AutoNAT server discovery and checks.
-- `opts` may include discovery limits/timeouts and check strategy knobs
-- such as `max_autonat_servers`, `target_autonat_responses`,
-- `check_timeout`, `stop_on_first_reachable`, and `drain_seconds`.
--- opts? table Discovery options.
--- table|nil task
--- table|nil err
function Client:start_discovery(opts)
  if not (self.host and type(self.host.spawn_task) == "function") then
    return nil, error_mod.new("state", "autonat start_discovery requires host task scheduler")
  end
  local options = opts or {}
  log.debug("autonat discovery task scheduled")
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

--- Start ongoing AutoNAT monitoring.
-- `opts` may include monitor cadence: `retry_interval_seconds`,
-- `healthy_interval_seconds`, `min_success_rounds`, and per-round
-- discovery/check options used by @{start_discovery}.
--- opts? table Monitor options.
--- table|nil task
--- table|nil err
function Client:start_monitor(opts)
  if not (self.host and type(self.host.spawn_task) == "function") then
    return nil, error_mod.new("state", "autonat start_monitor requires host task scheduler")
  end
  if
    self._monitor_task
    and self._monitor_task.status ~= "completed"
    and self._monitor_task.status ~= "failed"
    and self._monitor_task.status ~= "cancelled"
  then
    return self._monitor_task
  end
  local options = opts or {}
  local task, task_err = self.host:spawn_task("autonat.monitor", function(ctx)
    local backoff_peers = {}
    local min_success_rounds = options.min_success_rounds or DEFAULT_MONITOR_MIN_SUCCESS_ROUNDS
    local retry_interval = options.retry_interval_seconds or DEFAULT_MONITOR_RETRY_INTERVAL
    local healthy_interval = options.healthy_interval_seconds or DEFAULT_MONITOR_HEALTHY_INTERVAL
    local round_timeout = options.round_timeout_seconds or 30
    local initial_delay = options.initial_delay_seconds
    if initial_delay == nil then
      initial_delay = options.seed_wait_seconds
    end
    local success_rounds = 0
    local rounds = 0
    emit_event(self.host, "autonat:monitor:started", {
      min_success_rounds = min_success_rounds,
      retry_interval_seconds = retry_interval,
      healthy_interval_seconds = healthy_interval,
    })
    log.debug("autonat monitor started", {
      min_success_rounds = min_success_rounds,
      retry_interval_seconds = retry_interval,
      healthy_interval_seconds = healthy_interval,
      initial_delay_seconds = initial_delay,
    })
    if initial_delay and initial_delay > 0 then
      log.debug("autonat monitor initial delay", {
        delay_seconds = initial_delay,
      })
      local slept, sleep_err = ctx:sleep(initial_delay)
      if slept == nil and sleep_err then
        emit_event(self.host, "autonat:monitor:stopped", {
          round = rounds,
          cause = tostring(sleep_err),
        })
        return nil, sleep_err
      end
    end
    while true do
      rounds = rounds + 1
      emit_event(self.host, "autonat:monitor:round_started", { round = rounds })
      log.debug("autonat monitor round started", {
        round = rounds,
      })
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
        backoff_peers = backoff_peers,
        peer_backoff_seconds = options.peer_backoff_seconds
          or options.checked_peer_cooldown_seconds
          or DEFAULT_CHECKED_PEER_COOLDOWN,
        checked_peer_cooldown_seconds = options.checked_peer_cooldown_seconds or DEFAULT_CHECKED_PEER_COOLDOWN,
        required_addr_proto = options.required_addr_proto,
      })
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
      log.debug("autonat monitor round completed", {
        round = rounds,
        success_rounds = success_rounds,
        verified = verified,
        checked = stats.checked,
        responses = stats.responses,
        reachable = stats.reachable,
      })
      if verified then
        emit_event(self.host, "autonat:monitor:verified", {
          round = rounds,
          success_rounds = success_rounds,
          stats = stats,
        })
        log.debug("autonat monitor verified", {
          round = rounds,
          success_rounds = success_rounds,
        })
      end
      local next_sleep = verified and healthy_interval or retry_interval
      local ok, sleep_err = ctx:sleep(next_sleep)
      if ok == nil and sleep_err then
        emit_event(self.host, "autonat:monitor:stopped", {
          round = rounds,
          cause = tostring(sleep_err),
        })
        log.debug("autonat monitor stopped", {
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
  log.debug("autonat monitor task scheduled", {
    task_id = task.id,
  })
  return task
end

function Client:_handle_dial_back(stream)
  local message, read_err = autonat_proto.read_dial_back(stream, {
    max_message_size = self.max_message_size,
  })
  if not message then
    log.debug("autonat dial-back read failed", {
      cause = tostring(read_err),
    })
    return nil, read_err
  end
  if not self._pending[message.nonce] then
    log.debug("autonat dial-back rejected", {
      nonce = message.nonce,
      reason = "nonce_not_pending",
    })
    return nil, error_mod.new("protocol", "autonat dial-back nonce not pending")
  end
  self._verified[message.nonce] = true
  log.debug("autonat dial-back verified", {
    nonce = message.nonce,
    server_peer_id = self._pending[message.nonce] and self._pending[message.nonce].server_peer_id or nil,
  })
  local ok, write_err = autonat_proto.write_dial_back_response(stream, {
    status = autonat_proto.DIAL_BACK_STATUS.OK,
  })
  if not ok then
    log.debug("autonat dial-back response failed", {
      nonce = message.nonce,
      cause = tostring(write_err),
    })
    return nil, write_err
  end
  if type(stream.close) == "function" then
    pcall(function()
      stream:close()
    end)
  end
  return true
end

--- Start AutoNAT service handlers.
--- true|nil ok
--- table|nil err
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
  log.debug("autonat service started", {
    monitor_on_start = self._monitor_on_start == true,
  })
  return true
end

function Client:on_host_started()
  if self._monitor_on_start then
    local monitor_task, monitor_err = self:start_monitor(self._monitor_start_opts)
    if not monitor_task then
      return nil, monitor_err
    end
  end
  return true
end

--- Stop AutoNAT service activity.
--- true
function Client:stop()
  if self._monitor_task and self.host and type(self.host.cancel_task) == "function" then
    log.debug("autonat monitor task cancelling", {
      task_id = self._monitor_task.id,
    })
    self.host:cancel_task(self._monitor_task.id)
  end
  self._monitor_task = nil
  self.started = false
  log.debug("autonat service stopped")
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

--- Construct a new AutoNAT client/service instance.
-- `opts` may include protocol limits and behavior flags such as
-- `max_message_size`, `allow_dial_data`, `max_dial_data_bytes`,
-- `dial_data_chunk_size`, and `monitor_on_start`.
-- Additional monitor options: `monitor_start_opts` / `monitor_opts`.
--- host table Host instance.
--- opts? table
--- table|nil client
--- table|nil err
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
    _monitor_on_start = options.monitor_on_start == true,
    _monitor_start_opts = options.monitor_start_opts or options.monitor_opts,
    results = {},
    started = false,
  }, Client)
end

M.Client = Client
M.DIAL_REQUEST_ID = autonat_proto.DIAL_REQUEST_ID
M.DIAL_BACK_ID = autonat_proto.DIAL_BACK_ID

return M
