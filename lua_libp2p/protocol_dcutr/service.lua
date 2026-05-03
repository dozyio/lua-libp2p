local error_mod = require("lua_libp2p.error")
local multiaddr = require("lua_libp2p.multiaddr")
local dcutr = require("lua_libp2p.protocol_dcutr.protocol")

local M = {}
M.provides = { "dcutr" }
M.requires = { "identify" }

M.FAILURE_REASON = {
  NO_CANDIDATES = "no_candidates",
  STREAM_OPEN_FAILED = "stream_open_failed",
  CONNECT_WRITE_FAILED = "connect_write_failed",
  CONNECT_READ_FAILED = "connect_read_failed",
  UNEXPECTED_CONNECT_RESPONSE = "unexpected_connect_response",
  SYNC_WRITE_FAILED = "sync_write_failed",
  DIRECT_DIAL_FAILED = "direct_dial_failed",
  NO_OBS_ADDRS = "no_obs_addrs",
}

local function emit_event(host, name, payload)
  if host and type(host.emit) == "function" then
    host:emit(name, payload)
  end
end

local addr_has_proto

local function is_non_retryable_error(err)
  local text = tostring(err or "")
  if text == "" then
    return false
  end
  if string.find(text, "no common protocol", 1, true)
    or string.find(text, "unsupported", 1, true)
    or string.find(text, "protocol", 1, true)
    or string.find(text, "no non-relay observed addresses", 1, true)
    or string.find(text, "stream reset", 1, true)
    or string.find(text, "context deadline exceeded", 1, true)
  then
    return true
  end
  return false
end

local function decode_obs_addrs(bytes_addrs)
  local out = {}
  for _, addr in ipairs(bytes_addrs or {}) do
    if type(addr) == "string" then
      local parsed = multiaddr.from_bytes(addr)
      if parsed and parsed.text then
        out[#out + 1] = parsed.text
      end
    end
  end
  return out
end

local function unique_public_addrs(addrs)
  local out = {}
  local seen = {}
  for _, addr in ipairs(addrs or {}) do
    if type(addr) == "string"
      and addr:sub(1, 1) == "/"
      and multiaddr.is_public_addr(addr)
      and not addr_has_proto(addr, "p2p-circuit")
      and not seen[addr]
    then
      seen[addr] = true
      out[#out + 1] = addr
    end
  end
  return out
end

local function preferred_ip_proto_from_state(state)
  local remote_addr = state and state.remote_addr
  if type(remote_addr) ~= "string" then
    return nil
  end
  local parsed = multiaddr.parse(remote_addr)
  if not parsed then
    return nil
  end
  for _, component in ipairs(parsed.components or {}) do
    if component.protocol == "ip4" then
      return "ip4"
    end
    if component.protocol == "ip6" then
      return "ip6"
    end
  end
  return nil
end

addr_has_proto = function(addr, proto)
  local parsed = multiaddr.parse(addr)
  if not parsed then
    return false
  end
  for _, component in ipairs(parsed.components or {}) do
    if component.protocol == proto then
      return true
    end
  end
  return false
end

local function addr_uses_dns(addr)
  return addr_has_proto(addr, "dns") or addr_has_proto(addr, "dns4") or addr_has_proto(addr, "dns6")
end

local function candidate_score(addr, preferred_ip_proto)
  local score = 0
  if preferred_ip_proto and addr_has_proto(addr, preferred_ip_proto) then
    score = score + 100
  end
  if addr_has_proto(addr, "tcp") then
    score = score + 20
  end
  if not addr_uses_dns(addr) then
    score = score + 10
  end
  return score
end

local function filter_candidate_addrs(addrs, opts)
  local options = opts or {}
  local preferred_ip_proto = options.preferred_ip_proto
  local max_candidates = options.max_candidates or 8
  local seen = {}
  local scored = {}
  for _, addr in ipairs(addrs or {}) do
    if type(addr) == "string" and addr:sub(1, 1) == "/" then
      if seen[addr] then
        goto continue
      end
      seen[addr] = true
      if not multiaddr.is_public_addr(addr) then
        goto continue
      end
      if addr_has_proto(addr, "p2p-circuit") then
        goto continue
      end
      if not addr_has_proto(addr, "tcp") then
        goto continue
      end
      if preferred_ip_proto and not addr_has_proto(addr, preferred_ip_proto) then
        goto continue
      end
      scored[#scored + 1] = {
        addr = addr,
        score = candidate_score(addr, preferred_ip_proto),
      }
    end
    ::continue::
  end
  table.sort(scored, function(a, b)
    if a.score == b.score then
      return tostring(a.addr) < tostring(b.addr)
    end
    return a.score > b.score
  end)
  local out = {}
  for i = 1, math.min(max_candidates, #scored) do
    out[#out + 1] = scored[i].addr
  end
  return out
end

local function default_obs_addrs(host, opts)
  local options = opts or {}
  local allow_private_obs_addrs = options.allow_private_obs_addrs == true
  local max_obs_addrs = options.max_obs_addrs or 4
  local out = {}
  local seen = {}

  local function append_addr(addr)
    if type(addr) ~= "string" or addr:sub(1, 1) ~= "/" then
      return
    end
    if addr_has_proto(addr, "p2p-circuit") then
      return
    end
    if not allow_private_obs_addrs and not multiaddr.is_public_addr(addr) then
      return
    end
    if seen[addr] then
      return
    end
    seen[addr] = true
    out[#out + 1] = addr
    if max_obs_addrs > 0 and #out >= max_obs_addrs then
      return
    end
  end

  if host and host.address_manager and type(host.address_manager.get_public_address_mappings) == "function" then
    local mappings = host.address_manager:get_public_address_mappings() or {}
    for _, addr in ipairs(mappings) do
      append_addr(addr)
    end
  end

  if host and host.address_manager and type(host.address_manager.get_all_public_addrs) == "function" then
    local observed = host.address_manager:get_all_public_addrs() or {}
    for _, addr in ipairs(observed) do
      append_addr(addr)
    end
  end

  if host and type(host.get_multiaddrs) == "function" then
    local addrs = host:get_multiaddrs() or {}
    for _, addr in ipairs(addrs) do
      append_addr(addr)
    end
  end
  local public = unique_public_addrs(out)
  if #public > 0 then
    return public
  end

  if not allow_private_obs_addrs then
    return {}
  end

  local fallback = {}
  local seen = {}
  for _, addr in ipairs(out) do
    if type(addr) == "string"
      and addr:sub(1, 1) == "/"
      and not seen[addr]
      and not addr_has_proto(addr, "p2p-circuit")
    then
      seen[addr] = true
      fallback[#fallback + 1] = addr
    end
  end
  return fallback
end

function M.new(host, opts)
  local options = opts or {}
  local svc = {}
  svc._inflight = {}
  svc._pending_auto = {}
  svc._pending_auto_tasks = {}
  svc._allow_private_obs_addrs = options.allow_private_obs_addrs == true
  svc._max_obs_addrs = options.max_obs_addrs or 4

  local function remote_candidates(peer_id, state)
    if not (host.peerstore and type(host.peerstore.get_addrs) == "function") then
      return {}
    end
    local addrs = host.peerstore:get_addrs(peer_id) or {}
    return filter_candidate_addrs(addrs, {
      preferred_ip_proto = preferred_ip_proto_from_state(state),
      max_candidates = options.max_candidate_addrs,
    })
  end

  function svc:_try_start_pending(peer_id, task_ctx)
    local pending = svc._pending_auto[peer_id]
    if not pending then
      return true
    end
    if svc._inflight[peer_id] then
      return true
    end
    local candidates = remote_candidates(peer_id, pending.state)
    if options.try_unilateral_upgrade ~= false and #candidates > 0 and not pending.tried_unilateral then
      pending.tried_unilateral = true
      local unilateral = svc:_try_unilateral_upgrade(peer_id, pending.state, {
        timeout = options.dial_timeout,
        ctx = task_ctx,
      })
      if unilateral then
        svc._pending_auto[peer_id] = nil
        svc:_schedule_relay_close(pending.state, "unilateral_upgrade_success", nil, peer_id)
        svc:_mark_direct_preferred(peer_id)
        emit_event(host, "dcutr:migrated", {
          peer_id = peer_id,
          addr = unilateral.addr,
          direction = "inbound_unilateral",
        })
        return true
      end
    end

    local supports = nil
    if host.peerstore and type(host.peerstore.supports_protocol) == "function" then
      supports = host.peerstore:supports_protocol(peer_id, dcutr.ID)
      if not supports then
        emit_event(host, "dcutr:attempt:precheck", {
          peer_id = peer_id,
          remote_candidate_addrs = #candidates,
          remote_supports_dcutr = false,
        })
        return true
      end
    end
    local obs_precheck = default_obs_addrs(host, {
      allow_private_obs_addrs = svc._allow_private_obs_addrs,
      max_obs_addrs = svc._max_obs_addrs,
    })
    emit_event(host, "dcutr:attempt:precheck", {
      peer_id = peer_id,
      remote_candidate_addrs = #candidates,
      remote_supports_dcutr = supports == true,
      local_observed_addrs = #obs_precheck,
    })
    if #obs_precheck == 0 then
      return true
    end

    local task, task_err = svc:start_hole_punch(peer_id, {
      connection = pending.connection,
      state = pending.state,
      timeout = options.timeout,
      io_timeout = options.io_timeout,
      dial_timeout = options.dial_timeout,
      max_attempts = options.max_attempts,
      retry_delay_seconds = options.retry_delay_seconds,
    })
    if not task then
      emit_event(host, "dcutr:attempt:failed", {
        peer_id = peer_id,
        error = task_err,
      })
      return true
    end
    svc._pending_auto[peer_id] = nil
    svc._inflight[peer_id] = task
    if type(host.spawn_task) == "function" then
      host:spawn_task("services.dcutr.autoclear", function(ctx)
        host:wait_task(task, { ctx = ctx })
        svc._inflight[peer_id] = nil
        return true
      end, { service = "dcutr" })
    else
      svc._inflight[peer_id] = nil
    end
    return true
  end

  function svc:_schedule_pending_start(peer_id)
    if type(peer_id) ~= "string" or peer_id == "" then
      return true
    end
    if not svc._pending_auto[peer_id] or svc._pending_auto_tasks[peer_id] then
      return true
    end
    if type(host.spawn_task) ~= "function" then
      return svc:_try_start_pending(peer_id)
    end
    local marker = {}
    svc._pending_auto_tasks[peer_id] = marker
    local task, task_err = host:spawn_task("services.dcutr.auto_start", function(ctx)
      svc:_try_start_pending(peer_id, ctx)
      svc._pending_auto_tasks[peer_id] = nil
      return true
    end, { service = "dcutr" })
    if not task then
      svc._pending_auto_tasks[peer_id] = nil
      emit_event(host, "dcutr:attempt:failed", {
        peer_id = peer_id,
        error = task_err,
      })
      return true
    end
    if svc._pending_auto_tasks[peer_id] == marker then
      svc._pending_auto_tasks[peer_id] = task
    end
    return true
  end

  function svc:_mark_direct_preferred(peer_id)
    if type(peer_id) ~= "string" or peer_id == "" then
      return true
    end
    local manager = host and host.connection_manager
    if manager and type(manager.tag_peer) == "function" then
      manager:tag_peer(peer_id, "dcutr-direct", options.direct_peer_tag_value or 100)
    end
    return true
  end

  function svc:_close_relay_connection(connection_id, reason, expected_peer_id)
    local entry = type(host._find_connection_by_id) == "function" and host:_find_connection_by_id(connection_id) or nil
    if not entry then
      return true
    end
    local entry_state = entry.state or {}
    if type(expected_peer_id) == "string" and expected_peer_id ~= "" and entry_state.remote_peer_id ~= expected_peer_id then
      return true
    end
    local relay_state = entry_state.relay or {}
    if relay_state.limit_kind ~= "limited" then
      return true
    end
    if entry.conn and type(entry.conn.close) == "function" then
      entry.conn:close()
    end
    if type(host._unregister_connection) == "function" then
      return host:_unregister_connection(nil, entry, error_mod.new("closed", reason or "dcutr relay closed"))
    end
    return true
  end

  function svc:_schedule_relay_close(state, reason, task_ctx, expected_peer_id)
    if not state or not state.relay or state.relay.limit_kind ~= "limited" then
      return true
    end
    local connection_id = state.connection_id
    if type(connection_id) ~= "number" then
      return true
    end
    local grace_seconds = options.relay_grace_seconds or 5
    if grace_seconds <= 0 then
      svc:_close_relay_connection(connection_id, reason, expected_peer_id)
      return true
    end
    if type(host.spawn_task) ~= "function" then
      return true
    end
    host:spawn_task("services.dcutr.close_relay", function(ctx)
      local sleep_ctx = task_ctx or ctx
      if sleep_ctx and type(sleep_ctx.sleep) == "function" then
        sleep_ctx:sleep(grace_seconds)
      end
      svc:_close_relay_connection(connection_id, reason, expected_peer_id)
      emit_event(host, "dcutr:relay:closed", {
        connection_id = connection_id,
        reason = reason,
        peer_id = expected_peer_id,
      })
      return true
    end, { service = "dcutr" })
    return true
  end

  function svc:_attempt_direct_dials(target_peer_id, addrs, dial_opts)
    local errors = {}
    local opts = {}
    for k, v in pairs(dial_opts or {}) do
      opts[k] = v
    end
    opts.require_unlimited_connection = true
    opts.force = true
    for _, addr in ipairs(addrs or {}) do
      local dial_target = {
        peer_id = target_peer_id,
        addrs = { addr },
      }
      local conn, state, dial_err = host:dial(dial_target, opts)
      if conn and not (state and state.relay and state.relay.limit_kind == "limited") then
        emit_event(host, "dcutr:attempt:success", {
          peer_id = target_peer_id,
          addr = addr,
          state = state,
        })
        return { connection = conn, state = state, addr = addr }
      end
      errors[#errors + 1] = dial_err or error_mod.new("state", "direct dial returned limited connection", {
        peer_id = target_peer_id,
        addr = addr,
      })
    end
    return nil, errors
  end

  function svc:_try_unilateral_upgrade(peer_id, state, dial_opts)
    if not (host and host.peerstore and type(host.peerstore.get_addrs) == "function") then
      return nil
    end
    local peer_addrs = host.peerstore:get_addrs(peer_id) or {}
    local candidates = filter_candidate_addrs(peer_addrs, {
      preferred_ip_proto = preferred_ip_proto_from_state(state),
      max_candidates = options.max_candidate_addrs,
    })
    if #candidates == 0 then
      emit_event(host, "dcutr:unilateral:failed", {
        peer_id = peer_id,
        reason = M.FAILURE_REASON.NO_CANDIDATES,
      })
      return nil
    end
    local result = svc:_attempt_direct_dials(peer_id, candidates, dial_opts)
    if result then
      emit_event(host, "dcutr:unilateral:success", {
        peer_id = peer_id,
        addr = result.addr,
      })
    else
      emit_event(host, "dcutr:unilateral:failed", {
        peer_id = peer_id,
        reason = M.FAILURE_REASON.DIRECT_DIAL_FAILED,
      })
    end
    return result
  end

  function svc:_handle_upgrade_stream(stream, ctx)
    local remote_peer_id = ctx and ctx.connection and ctx.connection.remote_peer_id or nil
    local inbound_connect, connect_err = dcutr.read_message(stream)
    if not inbound_connect then
      return nil, connect_err
    end
    if inbound_connect.type ~= dcutr.TYPE.CONNECT then
      return nil, error_mod.new("protocol", "expected dcutr CONNECT")
    end
    local local_obs = default_obs_addrs(host, {
      allow_private_obs_addrs = svc._allow_private_obs_addrs,
      max_obs_addrs = svc._max_obs_addrs,
    })
    if #local_obs == 0 then
      return nil, error_mod.new("state", "no non-relay observed addresses for dcutr CONNECT response")
    end
    emit_event(host, "dcutr:connect:send", {
      peer_id = remote_peer_id,
      direction = "inbound_response",
      obs_addrs = local_obs,
      obs_addr_count = #local_obs,
    })
    local wrote, write_err = dcutr.write_message(stream, {
      type = dcutr.TYPE.CONNECT,
      obs_addrs = local_obs,
    })
    if not wrote then
      return nil, write_err
    end
    local sync_msg, sync_err = dcutr.read_message(stream)
    if not sync_msg then
      return nil, sync_err
    end
    if sync_msg.type ~= dcutr.TYPE.SYNC then
      return nil, error_mod.new("protocol", "expected dcutr SYNC")
    end
    emit_event(host, "dcutr:attempt:sync", { peer_id = remote_peer_id })
    local remote_addrs = filter_candidate_addrs(decode_obs_addrs(inbound_connect.obs_addrs), {
      preferred_ip_proto = preferred_ip_proto_from_state(ctx and ctx.state),
      max_candidates = options.max_candidate_addrs,
    })
    local result, dial_errs = svc:_attempt_direct_dials(remote_peer_id, remote_addrs, {
      timeout = options.dial_timeout or 4,
      ctx = ctx,
    })
    if not result then
      return nil, error_mod.new("io", "dcutr direct dial attempts failed", {
        peer_id = remote_peer_id,
        errors = dial_errs,
      })
    end
    svc:_schedule_relay_close(ctx and ctx.state, "inbound_upgrade_success", ctx, remote_peer_id)
    svc:_mark_direct_preferred(remote_peer_id)
    emit_event(host, "dcutr:migrated", {
      peer_id = remote_peer_id,
      addr = result.addr,
      direction = "inbound",
    })
    return result
  end

  function svc:start()
    local ok, err = host:handle(dcutr.ID, function(stream, ctx)
      return svc:_handle_upgrade_stream(stream, ctx)
    end, { run_on_limited_connection = true })
    if not ok then
      return nil, err
    end

    if options.auto_on_relay_connection ~= false and type(host.on) == "function" then
      local token, on_err = host:on("peer_connected", function(payload)
        local state = payload and payload.state or {}
        local relay_state = state and state.relay or nil
        local peer_id = payload and payload.peer_id or state.remote_peer_id
        if type(peer_id) ~= "string" or peer_id == "" then
          return true
        end
        if not relay_state or relay_state.limit_kind ~= "limited" then
          return true
        end
        if state.direction ~= "inbound" then
          return true
        end
        if svc._inflight[peer_id] then
          return true
        end
        local remote_candidate_addrs = 0
        if host.peerstore and type(host.peerstore.get_addrs) == "function" then
          local addrs = host.peerstore:get_addrs(peer_id) or {}
          remote_candidate_addrs = #filter_candidate_addrs(addrs, {
            preferred_ip_proto = preferred_ip_proto_from_state(state),
            max_candidates = options.max_candidate_addrs,
          })
        end
        svc._pending_auto[peer_id] = {
          connection = payload and payload.connection,
          state = state,
        }
        emit_event(host, "dcutr:attempt:precheck", {
          peer_id = peer_id,
          direction = state.direction,
          relay_limit_kind = relay_state and relay_state.limit_kind or nil,
          remote_candidate_addrs = remote_candidate_addrs,
        })
        return svc:_schedule_pending_start(peer_id)
      end)
      if not token then
        return nil, on_err
      end
      svc._peer_connected_token = token

      local protocols_token, protocols_err = host:on("peer_protocols_updated", function(payload)
        local peer_id = payload and payload.peer_id
        if type(peer_id) ~= "string" or peer_id == "" then
          return true
        end
        return svc:_schedule_pending_start(peer_id)
      end)
      if not protocols_token then
        return nil, protocols_err
      end
      svc._peer_protocols_updated_token = protocols_token

      local observed_token, observed_err = host:on("observed_addr", function()
        for peer_id in pairs(svc._pending_auto) do
          svc:_schedule_pending_start(peer_id)
        end
        return true
      end)
      if not observed_token then
        return nil, observed_err
      end
      svc._observed_addr_token = observed_token
    end

    return true
  end

  function svc:start_hole_punch(target, call_opts)
    local request_opts = call_opts or {}
    local function run_attempt(run_opts, attempt)
      local stream, selected, state_or_err
      if run_opts.connection and type(run_opts.connection.new_stream) == "function" then
        stream, selected, state_or_err = run_opts.connection:new_stream({ dcutr.ID })
        if run_opts.state and type(state_or_err) ~= "table" then
          state_or_err = run_opts.state
        end
      else
        stream, selected, _, state_or_err = host:new_stream(target, { dcutr.ID }, {
          timeout = run_opts.timeout,
          io_timeout = run_opts.io_timeout,
          ctx = run_opts.ctx,
          allow_limited_connection = true,
        })
      end
      if not stream then
        emit_event(host, "dcutr:attempt:failed", {
          peer_id = (run_opts.state and run_opts.state.remote_peer_id) or nil,
          attempt = attempt,
          reason = M.FAILURE_REASON.STREAM_OPEN_FAILED,
          error = state_or_err,
        })
        return nil, state_or_err
      end
      local remote_peer_id = state_or_err and state_or_err.remote_peer_id or nil
      emit_event(host, "dcutr:attempt:started", {
        peer_id = remote_peer_id,
        protocol = selected,
        attempt = attempt,
      })
      local connect_sent_at = os.clock()
      local outbound_obs = default_obs_addrs(host, {
        allow_private_obs_addrs = svc._allow_private_obs_addrs,
        max_obs_addrs = svc._max_obs_addrs,
      })
      if #outbound_obs == 0 then
        emit_event(host, "dcutr:attempt:failed", {
          peer_id = remote_peer_id,
          protocol = selected,
          attempt = attempt,
          reason = M.FAILURE_REASON.NO_OBS_ADDRS,
        })
        return nil, error_mod.new("state", "no non-relay observed addresses for dcutr CONNECT")
      end
      emit_event(host, "dcutr:connect:send", {
        peer_id = remote_peer_id,
        direction = "outbound_request",
        obs_addrs = outbound_obs,
        obs_addr_count = #outbound_obs,
      })
      local ok, write_err = dcutr.write_message(stream, {
        type = dcutr.TYPE.CONNECT,
        obs_addrs = outbound_obs,
      })
      if not ok then
        emit_event(host, "dcutr:attempt:failed", {
          peer_id = remote_peer_id,
          protocol = selected,
          attempt = attempt,
          reason = M.FAILURE_REASON.CONNECT_WRITE_FAILED,
          error = write_err,
        })
        return nil, write_err
      end
      local connect_resp, connect_err = dcutr.read_message(stream)
      if not connect_resp then
        emit_event(host, "dcutr:attempt:failed", {
          peer_id = remote_peer_id,
          protocol = selected,
          attempt = attempt,
          reason = M.FAILURE_REASON.CONNECT_READ_FAILED,
          error = connect_err,
        })
        return nil, connect_err
      end
      if connect_resp.type ~= dcutr.TYPE.CONNECT then
        emit_event(host, "dcutr:attempt:failed", {
          peer_id = remote_peer_id,
          protocol = selected,
          attempt = attempt,
          reason = M.FAILURE_REASON.UNEXPECTED_CONNECT_RESPONSE,
        })
        return nil, error_mod.new("protocol", "expected dcutr CONNECT response")
      end
      local rtt = math.max(0, os.clock() - connect_sent_at)
      local sync_ok, sync_err = dcutr.write_message(stream, { type = dcutr.TYPE.SYNC })
      if not sync_ok then
        emit_event(host, "dcutr:attempt:failed", {
          peer_id = remote_peer_id,
          protocol = selected,
          attempt = attempt,
          reason = M.FAILURE_REASON.SYNC_WRITE_FAILED,
          error = sync_err,
        })
        return nil, sync_err
      end
      local wait_seconds = rtt / 2
      if run_opts.ctx and type(run_opts.ctx.sleep) == "function" and wait_seconds > 0 then
        run_opts.ctx:sleep(wait_seconds)
      end
      local remote_addrs = filter_candidate_addrs(decode_obs_addrs(connect_resp.obs_addrs), {
        preferred_ip_proto = preferred_ip_proto_from_state(state_or_err),
        max_candidates = run_opts.max_candidate_addrs or options.max_candidate_addrs,
      })
      local dial_result, dial_errs = svc:_attempt_direct_dials(remote_peer_id, remote_addrs, {
        timeout = run_opts.dial_timeout or 4,
        ctx = run_opts.ctx,
      })
      if type(stream.close) == "function" then
        pcall(function() stream:close() end)
      end
      if not dial_result then
        emit_event(host, "dcutr:attempt:failed", {
          peer_id = remote_peer_id,
          protocol = selected,
          attempt = attempt,
          reason = M.FAILURE_REASON.DIRECT_DIAL_FAILED,
          errors = dial_errs,
        })
        return nil, error_mod.new("io", "dcutr hole punch failed", { errors = dial_errs })
      end
      return {
        peer_id = remote_peer_id,
        protocol = selected,
        rtt_seconds = rtt,
        connection = dial_result.connection,
        state = dial_result.state,
        addr = dial_result.addr,
        relay_state = state_or_err,
      }
    end

    local function run(run_opts)
      local max_attempts = run_opts.max_attempts or 3
      local retry_delay_seconds = run_opts.retry_delay_seconds or 0.25
      local last_err = nil
      for attempt = 1, max_attempts do
        local result, attempt_err = run_attempt(run_opts, attempt)
        if result then
          svc:_schedule_relay_close(result.relay_state, "outbound_upgrade_success", run_opts.ctx, result.peer_id)
          svc:_mark_direct_preferred(result.peer_id)
          emit_event(host, "dcutr:migrated", {
            peer_id = result.peer_id,
            addr = result.addr,
            direction = "outbound",
          })
          return result
        end
        last_err = attempt_err
        if is_non_retryable_error(attempt_err) then
          break
        end
        if attempt < max_attempts then
          emit_event(host, "dcutr:attempt:retry", {
            attempt = attempt,
            max_attempts = max_attempts,
            error = attempt_err,
          })
          if run_opts.ctx and type(run_opts.ctx.sleep) == "function" then
            run_opts.ctx:sleep(retry_delay_seconds)
          end
        end
      end
      return nil, last_err
    end

    local task, task_err = host:spawn_task("services.dcutr.start_hole_punch", function(ctx)
      local run_options = {}
      for k, v in pairs(request_opts) do
        run_options[k] = v
      end
      run_options.ctx = run_options.ctx or ctx
      return run(run_options)
    end, { service = "dcutr" })
    if not task then
      return nil, task_err
    end
    return task
  end

  return svc
end

return M
