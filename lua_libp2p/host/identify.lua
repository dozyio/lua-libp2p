--- Host identify protocol glue.
-- @module lua_libp2p.host.identify
local error_mod = require("lua_libp2p.error")
local identify = require("lua_libp2p.protocol_identify.protocol")
local key_pb = require("lua_libp2p.crypto.key_pb")
local log = require("lua_libp2p.log")
local multiaddr = require("lua_libp2p.multiaddr")

local M = {}

local DEFAULT_IDENTIFY_PROTOCOL_VERSION = "/lua-libp2p/0.1.0"
local DEFAULT_IDENTIFY_AGENT_VERSION = "lua-libp2p/0.1.0"

local function map_count(values)
  local n = 0
  for _ in pairs(values or {}) do
    n = n + 1
  end
  return n
end

local function list_set(values)
  local out = {}
  for _, value in ipairs(values or {}) do
    if type(value) == "string" and value ~= "" then
      out[value] = true
    end
  end
  return out
end

local function protocol_delta(before, after)
  local before_set = list_set(before)
  local added = {}
  for _, protocol_id in ipairs(after or {}) do
    if not before_set[protocol_id] then
      added[#added + 1] = protocol_id
    end
  end
  return added
end

local function identify_listen_addrs(message)
  local out = {}
  for _, addr in ipairs(message.listenAddrs or {}) do
    if type(addr) == "string" and addr ~= "" then
      if addr:sub(1, 1) == "/" then
        out[#out + 1] = addr
      else
        local parsed = multiaddr.from_bytes(addr)
        if parsed and parsed.text then
          out[#out + 1] = parsed.text
        end
      end
    end
  end
  return out
end

function M.init(host)
  host._identify_inflight = {}
end

function M.install(Host)
  function Host:_handle_identify(stream, ctx)
    local local_pub, pub_err = key_pb.encode_public_key(key_pb.KEY_TYPE.Ed25519, self.identity.public_key)
    if not local_pub then
      return nil, pub_err
    end

    local identify_opts = self._service_options.identify or {}
    local observed = nil
    if identify_opts.include_observed ~= false
      and ctx
      and ctx.connection
      and ctx.connection.raw
      and ctx.connection:raw().remote_multiaddr
    then
      observed = ctx.connection:raw():remote_multiaddr()
      if type(observed) ~= "string" or observed == "" or observed:sub(1, 1) ~= "/" or not multiaddr.parse(observed) then
        observed = nil
      end
    end

    local msg = {
      protocolVersion = identify_opts.protocol_version
        or identify_opts.protocolVersion
        or identify_opts.protocol
        or DEFAULT_IDENTIFY_PROTOCOL_VERSION,
      agentVersion = DEFAULT_IDENTIFY_AGENT_VERSION,
      publicKey = local_pub,
      listenAddrs = self:get_multiaddrs_raw(),
      observedAddr = observed,
      protocols = self:_list_protocol_handlers(),
    }

    if self._debug_connection_events then
      self:emit("identify:response:send", {
        peer_id = ctx and ctx.state and ctx.state.remote_peer_id or nil,
        connection_id = ctx and ctx.state and ctx.state.connection_id or nil,
        limited = ctx and ctx.state and self:_connection_is_limited(ctx.state) or false,
        relay_limit_kind = ctx and ctx.state and ctx.state.relay and ctx.state.relay.limit_kind or nil,
        observed_addr = observed,
        listen_addr_count = #msg.listenAddrs,
      })
    end

    local wrote, write_err = identify.write(stream, msg)
    if not wrote then
      return nil, write_err
    end

    if type(stream.close_write) == "function" then
      stream:close_write()
    end

    return true
  end

  --- Request identify exchange against target peer.
  -- `opts.timeout`, `opts.io_timeout`, `opts.ctx`, and
  -- `opts.allow_limited_connection` control stream dial/IO behavior.
  function Host:_request_identify(peer_id, opts)
    if type(peer_id) ~= "string" or peer_id == "" then
      return nil, error_mod.new("input", "identify request requires peer id")
    end

    local stream, selected, conn, state_or_err = self:new_stream(peer_id, { identify.ID }, opts)
    if not stream then
      log.debug("identify request stream open failed", {
        subsystem = "identify",
        peer_id = peer_id,
        cause = tostring(state_or_err),
      })
      return nil, state_or_err
    end
    local state = state_or_err

    local msg, read_err = identify.read(stream)
    if type(stream.close) == "function" then
      pcall(function()
        stream:close()
      end)
    elseif type(stream.reset_now) == "function" then
      pcall(function()
        stream:reset_now()
      end)
    end
    if not msg then
      log.debug("identify request read failed", {
        subsystem = "identify",
        peer_id = peer_id,
        cause = tostring(read_err),
        protocol = selected,
        security = state and state.security,
        muxer = state and state.muxer,
      })
      return nil, read_err
    end

    if self.peerstore then
      local before_protocols = self.peerstore:get_protocols(peer_id)
      self.peerstore:merge(peer_id, {
        addrs = identify_listen_addrs(msg),
        protocols = msg.protocols or {},
      })
      local after_protocols = self.peerstore:get_protocols(peer_id)
      local added_protocols = protocol_delta(before_protocols, after_protocols)
      if #added_protocols > 0 then
        local ok, emit_err = self:emit("peer_protocols_updated", {
          peer_id = peer_id,
          protocols = after_protocols,
          added_protocols = added_protocols,
          source = "identify",
        })
        if not ok then
          return nil, emit_err
        end
        local self_update_ok, self_update_err = self:_emit_self_peer_update_if_changed()
        if not self_update_ok then
          return nil, self_update_err
        end
      end
    end

    if self.address_manager and msg.observedAddr then
      local observed = identify_listen_addrs({ listenAddrs = { msg.observedAddr } })[1]
      if observed then
        self.address_manager:add_observed_addr(observed)
        local ok, emit_err = self:emit("observed_addr", {
          peer_id = peer_id,
          addr = observed,
          source = "identify",
        })
        if not ok then
          return nil, emit_err
        end
      end
    end

    return {
      message = msg,
      protocol = selected,
      connection = conn,
      state = state,
    }
  end

  function Host:_schedule_identify_for_peer(peer_id)
    if type(peer_id) ~= "string" or peer_id == "" then
      return true
    end
    if self._identify_inflight[peer_id] then
      return true
    end
    self._identify_inflight[peer_id] = true
    log.debug("identify on connect scheduled", {
      subsystem = "identify",
      peer_id = peer_id,
      inflight = map_count(self._identify_inflight),
    })

    self:_spawn_handler_task(function(_, ctx)
      local pid = ctx and ctx.peer_id
      log.debug("identify on connect running", {
        subsystem = "identify",
        peer_id = pid,
        inflight = map_count(self._identify_inflight),
      })
      local call_ok, result, identify_err = pcall(function()
        return self:_request_identify(pid, { ctx = ctx })
      end)
      if not call_ok then
        local panic = result
        result = nil
        identify_err = error_mod.new("protocol", "identify on connect panicked", { cause = panic })
      end
      self._identify_inflight[pid] = nil

      if not result then
        log.warn("identify on connect failed", {
          subsystem = "identify",
          peer_id = pid,
          cause = tostring(identify_err),
          inflight = map_count(self._identify_inflight),
        })
        local ok, emit_err = self:emit("peer_identify_failed", {
          peer_id = pid,
          cause = identify_err,
        })
        if not ok then
          return nil, emit_err
        end
        return true
      end

      log.debug("identify on connect completed", {
        subsystem = "identify",
        peer_id = pid,
        inflight = map_count(self._identify_inflight),
      })

      local ok, emit_err = self:emit("peer_identified", {
        peer_id = pid,
        protocol = result.protocol,
        message = result.message,
        connection = result.connection,
        state = result.state,
      })
      if not ok then
        return nil, emit_err
      end
      return true
    end, {
      protocol = "identify_connect",
      peer_id = peer_id,
    })

    return true
  end
end

return M
