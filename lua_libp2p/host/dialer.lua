--- Host outbound dial and stream opening internals.
-- @module lua_libp2p.host.dialer
local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log").subsystem("host")
local multiaddr = require("lua_libp2p.multiaddr")
local relay_proto = require("lua_libp2p.transport_circuit_relay_v2.protocol")
local upgrader = require("lua_libp2p.network.upgrader")

local M = {}

local function is_closed_error(err)
  if not error_mod.is_error(err) then
    return false
  end
  return err.kind == "closed"
end

local function now_seconds()
  local ok_socket, socket = pcall(require, "socket")
  if ok_socket and type(socket.gettime) == "function" then
    return socket.gettime()
  end
  return os.time()
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

local function resolve_target(target)
  if type(target) == "string" then
    if target:sub(1, 1) == "/" then
      return { addr = target }
    end
    return { peer_id = target }
  end

  if type(target) == "table" then
    return {
      peer_id = target.peer_id,
      addr = target.addr,
      addrs = target.addrs,
    }
  end

  return nil
end

local function prefer_dialable_addrs(host, addrs)
  if type(host.is_dialable_addr) ~= "function" then
    return addrs
  end
  local preferred = {}
  local fallback = {}
  for _, addr in ipairs(addrs or {}) do
    local ok, dialable = pcall(host.is_dialable_addr, host, addr)
    if ok and dialable then
      preferred[#preferred + 1] = addr
    else
      fallback[#fallback + 1] = addr
    end
  end
  if #preferred == 0 then
    return addrs
  end
  for _, addr in ipairs(fallback) do
    preferred[#preferred + 1] = addr
  end
  return preferred
end

function M.install(Host)
  --- Dial relay destination over explicit relay multiaddr.
  -- `opts.timeout`, `opts.io_timeout`, and `opts.ctx` control connect/IO timing.
  -- `opts.allow_limited_connection=true` allows limited relay state results.
  function Host:_dial_relay_raw(addr, destination_peer_id, opts)
    opts = opts or {}
    if not opts.ctx
      and not opts._internal_task_ctx
      and type(self.spawn_task) == "function"
      and type(self.run_until_task) == "function"
    then
      local task, task_err = self:spawn_task("host.dial_relay_raw", function(ctx)
        local task_opts = {}
        for k, v in pairs(opts) do
          task_opts[k] = v
        end
        task_opts.ctx = ctx
        task_opts._internal_task_ctx = true
        return self:_dial_relay_raw(addr, destination_peer_id, task_opts)
      end, { service = "host" })
      if not task then
        return nil, nil, task_err
      end
      return self:run_until_task(task, {
        timeout = opts.timeout,
        poll_interval = opts.poll_interval,
      })
    end

    local info, info_err = multiaddr.relay_info(addr)
    if not info then
      return nil, nil, info_err
    end
    local target_peer_id = destination_peer_id or info.destination_peer_id
    if type(target_peer_id) ~= "string" or target_peer_id == "" then
      return nil, nil, error_mod.new("input", "relayed dial target must include destination peer id")
    end
    log.debug("host relay dial started", {
      relay_peer_id = info.relay_peer_id,
      destination_peer_id = target_peer_id,
      relay_addr = info.relay_addr,
    })

    local stream, selected, relay_conn, relay_state_or_err = self:new_stream(info.relay_addr, { relay_proto.HOP_ID }, opts)
    if not stream then
      log.debug("host relay dial stream failed", {
        relay_peer_id = info.relay_peer_id,
        destination_peer_id = target_peer_id,
        cause = tostring(relay_state_or_err),
      })
      return nil, nil, relay_state_or_err
    end

    local connected, response_or_err = relay_proto.connect(stream, target_peer_id, opts)
    if not connected then
      if type(stream.close) == "function" then
        pcall(function()
          stream:close()
        end)
      end
      log.debug("host relay dial failed", {
        relay_peer_id = info.relay_peer_id,
        destination_peer_id = target_peer_id,
        protocol = selected,
        cause = tostring(response_or_err),
      })
      return nil, nil, response_or_err
    end
    local response = response_or_err or {}

    log.debug("host relay dial completed", {
      relay_peer_id = info.relay_peer_id,
      destination_peer_id = target_peer_id,
      protocol = selected,
      limit_kind = relay_proto.classify_limit(response.limit),
    })
    return stream, {
      relay_peer_id = info.relay_peer_id,
      relay_addr = info.relay_addr,
      destination_peer_id = target_peer_id,
      protocol = selected,
      relay_connection = relay_conn,
      relay_state = relay_state_or_err,
      limit = response.limit,
      limit_kind = relay_proto.classify_limit(response.limit),
    }
  end

  --- Dial peer/address directly (non-relay-specialized path).
  -- `opts.timeout`, `opts.io_timeout`, and `opts.ctx` control dial behavior.
  -- `opts.require_unlimited_connection=true` rejects limited relay results.
  function Host:_dial_direct(peer_or_addr, opts)
    opts = opts or {}
    local resolved = resolve_target(peer_or_addr)
    if not resolved then
      return nil, nil, error_mod.new("input", "dial target must be peer id, multiaddr, or target table")
    end

    if not resolved.peer_id and resolved.addr then
      resolved.peer_id = extract_peer_id_from_multiaddr(resolved.addr)
    end

    if opts.force ~= true then
      local existing = self:_find_connection(resolved.peer_id, opts)
      if existing then
        log.debug("host dial reused connection", {
          peer_id = resolved.peer_id,
          connection_id = existing.id,
          direction = existing.state and existing.state.direction or nil,
        })
        return existing.conn, existing.state
      end
    end

    local candidate_addrs
    if resolved.addr then
      candidate_addrs = { resolved.addr }
    else
      local addrs = resolved.addrs
      if type(addrs) ~= "table" then
        addrs = self.peerstore and self.peerstore:get_addrs(resolved.peer_id) or {}
      end
      if opts and type(opts.candidate_addrs) == "table" then
        candidate_addrs = opts.candidate_addrs
      elseif self.connection_manager and type(self.connection_manager.rank_addrs) == "function" then
        candidate_addrs = self.connection_manager:rank_addrs(addrs, opts)
      else
        candidate_addrs = addrs
      end
      candidate_addrs = prefer_dialable_addrs(self, candidate_addrs)
    end
    if #candidate_addrs == 0 then
      log.debug("host dial failed", {
        peer_id = resolved.peer_id,
        reason = "no_candidate_addrs",
      })
      return nil, nil, error_mod.new("input", "dial target must include an address when no connection exists")
    end

    log.debug("host dial started", {
      peer_id = resolved.peer_id,
      candidate_addrs = #candidate_addrs,
      force = opts.force == true,
    })
    local deadline = opts.dial_timeout and (now_seconds() + opts.dial_timeout) or nil
    local last_err
    for _, addr in ipairs(candidate_addrs) do
      if deadline and now_seconds() >= deadline then
        return nil, nil, error_mod.new("timeout", "dial timed out", { peer_id = resolved.peer_id })
      end
      local raw_conn, dial_err, relay_state
      log.debug("host dial address attempt", {
        peer_id = resolved.peer_id,
        addr = addr,
        relay = multiaddr.is_relay_addr(addr),
      })
      if multiaddr.is_relay_addr(addr) then
        raw_conn, relay_state, dial_err = self:_dial_relay_raw(addr, resolved.peer_id, opts)
      else
        local endpoint, endpoint_err = multiaddr.to_tcp_endpoint(addr)
        if not endpoint then
          last_err = endpoint_err
        else
          local timeout = opts.address_dial_timeout or opts.timeout or self._connect_timeout
          if deadline then
            timeout = math.max(0, math.min(timeout, deadline - now_seconds()))
          end
          raw_conn, dial_err = self._tcp_transport.dial({
            host = endpoint.host,
            port = endpoint.port,
          }, {
            timeout = timeout,
            io_timeout = opts.io_timeout or self._io_timeout,
            ctx = opts.ctx,
            nodelay = opts.nodelay ~= nil and opts.nodelay or (self._tcp_options and self._tcp_options.nodelay),
            keepalive = opts.keepalive ~= nil and opts.keepalive or (self._tcp_options and self._tcp_options.keepalive),
            keepalive_initial_delay = opts.keepalive_initial_delay ~= nil and opts.keepalive_initial_delay
              or (self._tcp_options and self._tcp_options.keepalive_initial_delay),
          })
        end
      end
      if raw_conn then
        local expected_remote = resolved.peer_id or extract_peer_id_from_multiaddr(addr)

        local conn, state, up_err = upgrader.upgrade_outbound(raw_conn, {
          local_keypair = self.identity,
          expected_remote_peer_id = expected_remote,
          security_protocols = self.security_transports,
          muxer_protocols = self.muxers,
          ctx = opts.ctx,
        })
        if not conn then
          if type(raw_conn.close) == "function" then
            raw_conn:close()
          end
          last_err = up_err
          log.debug("host outbound upgrade failed", {
            peer_id = expected_remote,
            addr = addr,
            cause = tostring(up_err),
          })
        else
          state.direction = state.direction or "outbound"
          if relay_state then
            relay_state.limit = relay_state.limit or nil
            relay_state.limit_kind = relay_proto.classify_limit(relay_state.limit)
            relay_state.direction = "outbound"
            state.relay = relay_state
          end
          local entry, register_err = self:_register_connection(conn, state)
          if entry then
            log.debug("host dial completed", {
              peer_id = entry.state and entry.state.remote_peer_id or expected_remote,
              addr = addr,
              connection_id = entry.id,
              security = state.security,
              muxer = state.muxer,
              relay = relay_state ~= nil,
            })
            return entry.conn, entry.state
          end
          conn:close()
          last_err = register_err
          log.debug("host connection registration failed", {
            peer_id = state.remote_peer_id,
            addr = addr,
            cause = tostring(register_err),
          })
        end
      elseif dial_err then
        last_err = dial_err
        log.debug("host dial address failed", {
          peer_id = resolved.peer_id,
          addr = addr,
          cause = tostring(dial_err),
        })
      end
    end

    log.debug("host dial failed", {
      peer_id = resolved.peer_id,
      candidate_addrs = #candidate_addrs,
      cause = tostring(last_err),
    })
    return nil, nil, last_err or error_mod.new("io", "all dial addresses failed")
  end

  --- Open (or reuse) a connection to a peer target.
  -- @tparam string|table peer_or_addr Peer id, multiaddr, or dial target table.
  -- @tparam[opt] table opts Dial options.
  -- Common options: `timeout`, `io_timeout`, `ctx`, `force`,
  -- `require_unlimited_connection`, `allow_limited_connection`,
  -- `bypass_connection_manager`.
  -- `opts.allow_limited_connection=true` permits returning limited relay links.
  -- @treturn table|nil conn
  -- @treturn[opt] table state
  -- @treturn[opt] table err
  function Host:dial(peer_or_addr, opts)
    local options = opts or {}
    log.debug("host dial requested", {
      target = type(peer_or_addr) == "table" and (peer_or_addr.peer_id or peer_or_addr.addr or peer_or_addr.multiaddr) or peer_or_addr,
      bypass_connection_manager = options.bypass_connection_manager == true,
      force = options.force == true,
    })
    if options.bypass_connection_manager or not self.connection_manager then
      return self:_dial_direct(peer_or_addr, options)
    end
    return self.connection_manager:open_connection(peer_or_addr, options)
  end

  --- Open a negotiated protocol stream.
  -- @tparam string|table peer_or_addr Peer id, multiaddr, or dial target table.
  -- @tparam table protocols Ordered multistream protocol IDs.
  -- @tparam[opt] table opts Stream and dial options.
  -- Common options: all `dial` options plus stream-level negotiation controls.
  -- `opts.protocol_hint`/`opts.allow_limited_connection` influence negotiation policy.
  -- @treturn table|nil stream
  -- @treturn[opt] string selected Selected protocol ID.
  -- @treturn[opt] table conn Underlying connection.
  -- @treturn[opt] table state_or_err Connection state or error.
  function Host:new_stream(peer_or_addr, protocols, opts)
    local function open_once(dial_opts)
      local conn, state, dial_err = self:dial(peer_or_addr, dial_opts)
      if not conn then
        log.debug("host stream dial failed", {
          target = type(peer_or_addr) == "table" and (peer_or_addr.peer_id or peer_or_addr.addr or peer_or_addr.multiaddr) or peer_or_addr,
          protocols = table.concat(protocols or {}, ","),
          cause = tostring(dial_err),
        })
        return nil, nil, nil, dial_err
      end

      if self:_connection_is_limited(state) then
        local allowed, allow_err = self:_protocols_allowed_on_limited_connection(protocols, dial_opts)
        if not allowed then
          log.debug("host stream blocked on limited connection", {
            peer_id = state and state.remote_peer_id or nil,
            connection_id = state and state.connection_id or nil,
            protocols = table.concat(protocols or {}, ","),
            cause = tostring(allow_err),
          })
          return nil, nil, nil, allow_err
        end
      end

      local stream_scope, resource_err = self:_open_stream_resource({ state = state }, "outbound")
      if resource_err then
        return nil, nil, nil, resource_err
      end

      local stream, selected, stream_err = conn:new_stream(protocols)
      if not stream then
        self:_close_stream_resource(stream_scope)
        log.debug("host stream open failed", {
          peer_id = state and state.remote_peer_id or nil,
          connection_id = state and state.connection_id or nil,
          protocols = table.concat(protocols or {}, ","),
          cause = tostring(stream_err),
        })
        return nil, nil, nil, stream_err, state
      end

      local set_ok, set_err = self:_set_stream_resource_protocol(stream_scope, selected)
      if not set_ok then
        if type(stream.reset_now) == "function" then
          pcall(function() stream:reset_now() end)
        elseif type(stream.close) == "function" then
          pcall(function() stream:close() end)
        end
        self:_close_stream_resource(stream_scope)
        log.debug("host stream resource protocol failed", {
          peer_id = state and state.remote_peer_id or nil,
          connection_id = state and state.connection_id or nil,
          protocol = selected,
          cause = tostring(set_err),
        })
        return nil, nil, nil, set_err, state
      end
      stream = self:_wrap_stream_resource(stream, stream_scope)

      log.debug("host stream opened", {
        peer_id = state and state.remote_peer_id or nil,
        connection_id = state and state.connection_id or nil,
        protocol = selected,
        direction = state and state.direction or nil,
      })

      return stream, selected, conn, state
    end

    local stream_opts = {}
    for k, v in pairs(opts or {}) do
      stream_opts[k] = v
    end
    if stream_opts.allow_limited_connection ~= true then
      local limited_allowed = self:_protocols_allowed_on_limited_connection(protocols, stream_opts)
      if not limited_allowed then
        stream_opts.require_unlimited_connection = true
      end
    end

    log.debug("host stream open requested", {
      target = type(peer_or_addr) == "table" and (peer_or_addr.peer_id or peer_or_addr.addr or peer_or_addr.multiaddr) or peer_or_addr,
      protocols = table.concat(protocols or {}, ","),
      allow_limited_connection = stream_opts.allow_limited_connection == true,
    })

    local stream, selected, conn, state_or_err, state = open_once(stream_opts)
    if stream then
      return stream, selected, conn, state_or_err
    end
    local stream_err = state_or_err
    if is_closed_error(stream_err) then
      local bad_state = state
      local bad_connection_id = bad_state and bad_state.connection_id or nil
      local bad_entry = bad_connection_id and self:_find_connection_by_id(bad_connection_id) or nil
      if bad_entry then
        self:_unregister_connection(nil, bad_entry, stream_err)
      end
      local retry_opts = {}
      for k, v in pairs(stream_opts) do
        retry_opts[k] = v
      end
      retry_opts.force = true
      local retry_stream, retry_selected, retry_conn, retry_state_or_err = open_once(retry_opts)
      if retry_stream then
        return retry_stream, retry_selected, retry_conn, retry_state_or_err
      end
      return nil, nil, nil, retry_state_or_err
    end
    return nil, nil, nil, stream_err
  end
end

return M
