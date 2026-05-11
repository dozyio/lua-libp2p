--- Host construction and runtime orchestration.
-- Hosts manage listeners, connections, protocol handlers, services, and
-- cooperative background tasks.
-- @module lua_libp2p.host
local address_manager = require("lua_libp2p.address_manager")
local connection_encrypter = require("lua_libp2p.connection_encrypter")
local connection_manager = require("lua_libp2p.connection_manager")
local keys = require("lua_libp2p.crypto.keys")
local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log").subsystem("host")
local host_advertise = require("lua_libp2p.host.advertise")
local host_address_policy = require("lua_libp2p.host.address_policy")
local host_bootstrap = require("lua_libp2p.host.bootstrap")
local host_connections = require("lua_libp2p.host.connections")
local host_dialer = require("lua_libp2p.host.dialer")
local host_events = require("lua_libp2p.host.events")
local host_identify = require("lua_libp2p.host.identify")
local host_listeners = require("lua_libp2p.host.listeners")
local host_perf = require("lua_libp2p.host.perf")
local host_protocols = require("lua_libp2p.host.protocols")
local host_service_manager = require("lua_libp2p.host.service_manager")
local host_tasks = require("lua_libp2p.host.tasks")
local host_runtime_luv = require("lua_libp2p.host.runtime_luv")
local host_runtime_luv_native = require("lua_libp2p.host.runtime_luv_native")
local muxer_registry = require("lua_libp2p.muxer")
local peerstore = require("lua_libp2p.peerstore")
local resource_manager = require("lua_libp2p.resource_manager")
local relay_proto = require("lua_libp2p.transport_circuit_relay_v2.protocol")
local table_utils = require("lua_libp2p.util.tables")
local upgrader = require("lua_libp2p.network.upgrader")
local tcp_luv = require("lua_libp2p.transport_tcp.luv")

local M = {}

local list_copy = table_utils.copy_list

local function map_copy(source)
  local out = {}
  for k, v in pairs(source or {}) do
    out[k] = v
  end
  return out
end

local emit_event = host_events.emit

local Host = {}
Host.__index = Host
host_advertise.install(Host)
host_address_policy.install(Host)
host_bootstrap.install(Host)
host_connections.install(Host)
host_dialer.install(Host)
host_events.install(Host)
host_identify.install(Host)
host_listeners.install(Host)
host_perf.install(Host)
host_protocols.install(Host)
host_tasks.install(Host)

local function sleep_seconds(seconds)
  local ok_socket, socket = pcall(require, "socket")
  if ok_socket and type(socket.sleep) == "function" then
    socket.sleep(seconds)
  end
end

local function now_seconds()
  local ok_socket, socket = pcall(require, "socket")
  if ok_socket and type(socket.gettime) == "function" then
    return socket.gettime()
  end
  return os.time()
end

local function debug_perf_add(host, key, elapsed_seconds)
  local perf = type(host) == "table" and rawget(host, "_debug_perf") or nil
  if type(perf) ~= "table" then
    return
  end
  perf[key .. "_calls"] = (perf[key .. "_calls"] or 0) + 1
  perf[key .. "_ms"] = (perf[key .. "_ms"] or 0) + (elapsed_seconds * 1000)
end

function Host:_bump_debug_counter(key, amount)
  if type(key) ~= "string" or key == "" then
    return false
  end
  local counters = rawget(self, "_debug_counters")
  if type(counters) ~= "table" then
    counters = {}
    rawset(self, "_debug_counters", counters)
  end
  counters[key] = (tonumber(counters[key]) or 0) + (amount or 1)
  return true
end

local function is_nonfatal_stream_error(err)
  if not (err and error_mod.is_error(err)) then
    return false
  end
  return err.kind == "timeout"
    or err.kind == "busy"
    or err.kind == "closed"
    or err.kind == "decode"
    or err.kind == "protocol"
    or err.kind == "resource"
    or err.kind == "unsupported"
end

local function flatten_error_fields(err, prefix, fields, depth)
  local key = prefix or "error"
  local out = fields or {}
  local remaining = depth or 4
  out[key] = tostring(err)
  if remaining <= 0 or not error_mod.is_error(err) then
    return out
  end
  out[key .. "_kind"] = err.kind
  if type(err.context) == "table" then
    for k, v in pairs(err.context) do
      if k == "cause" then
        out[key .. "_cause"] = tostring(v)
        if error_mod.is_error(v) then
          flatten_error_fields(v, key .. "_cause", out, remaining - 1)
        end
      elseif type(v) ~= "table" and type(v) ~= "function" and type(v) ~= "thread" then
        out[key .. "_" .. tostring(k)] = v
      end
    end
  end
  return out
end

local function raw_conn_log_fields(raw_conn, fields)
  local out = fields or {}
  if raw_conn and type(raw_conn.debug_id) == "function" then
    local ok, raw_id = pcall(raw_conn.debug_id, raw_conn)
    if ok then
      out.raw_id = raw_id
    end
  elseif raw_conn and raw_conn._debug_id ~= nil then
    out.raw_id = raw_conn._debug_id
  end
  if raw_conn and type(raw_conn.remote_multiaddr) == "function" then
    local ok, addr = pcall(raw_conn.remote_multiaddr, raw_conn)
    if ok then
      out.remote_addr = addr
    end
  end
  if raw_conn and type(raw_conn.local_multiaddr) == "function" then
    local ok, addr = pcall(raw_conn.local_multiaddr, raw_conn)
    if ok then
      out.local_addr = addr
    end
  end
  return out
end

local function runtime_luv_start(host)
  local ok, err = host_runtime_luv.start(host)
  if not ok then
    return nil, err
  end
  return true, nil, true
end

local function runtime_luv_stop(host)
  return host_runtime_luv.stop(host)
end

local function runtime_luv_poll_once(host, timeout)
  return host_runtime_luv.poll_once(host, timeout)
end

local function runtime_luv_sync_watchers(host)
  return host_runtime_luv.sync_watchers(host)
end

local RUNTIME_IMPLS = {
  luv = {
    start = runtime_luv_start,
    stop = runtime_luv_stop,
    poll_once = runtime_luv_poll_once,
    sync_watchers = runtime_luv_sync_watchers,
  },
}

local function default_runtime_name()
  return "luv"
end

local function contains_circuit_listen_addr(addrs)
  for _, addr in ipairs(addrs or {}) do
    if addr == "/p2p-circuit" then
      return true
    end
  end
  return false
end

local function contains_non_circuit_listen_addr(addrs)
  for _, addr in ipairs(addrs or {}) do
    if addr ~= "/p2p-circuit" then
      return true
    end
  end
  return false
end

local function tcp_options_from_config(cfg)
  local options = cfg.tcp or {}
  return {
    nodelay = options.nodelay,
    keepalive = options.keepalive,
    keepalive_initial_delay = options.keepalive_initial_delay,
    listen_backlog = options.listen_backlog or cfg.listen_backlog,
    accept_batch = options.accept_batch or cfg.accept_batch,
  }
end

local function resource_manager_from_config(cfg)
  if cfg.resource_manager == false then
    return nil
  end
  if type(cfg.resource_manager) == "table" and type(cfg.resource_manager.open_connection) == "function" then
    return cfg.resource_manager
  end
  local options = cfg.resource_manager_options
  if options == nil and type(cfg.resource_manager) == "table" then
    options = cfg.resource_manager
  end
  return resource_manager.new(options or {})
end

local function connection_manager_from_config(host, cfg)
  local spec = cfg.connection_manager
  if spec == false then
    return nil
  end
  if type(spec) == "table" and type(spec.can_open_connection) == "function" then
    return spec
  end
  if spec ~= nil then
    return connection_manager.new(host, spec)
  end
  return connection_manager.new(host, cfg.dial_queue or {})
end

function Host:new(config)
  local cfg = config or {}
  local runtime_name = cfg.runtime or "auto"
  if runtime_name == "auto" then
    runtime_name = default_runtime_name()
  end
  local runtime_impl = RUNTIME_IMPLS[runtime_name]
  if runtime_impl == nil then
    return nil,
      error_mod.new("input", "unsupported host runtime", {
        runtime = runtime_name,
        supported = { "auto", "luv" },
      })
  end
  local start_blocking = cfg.blocking
  if start_blocking == nil then
    start_blocking = runtime_name ~= "luv"
  end

  local keypair = cfg.identity
  if not keypair then
    local generated, gen_err = keys.generate_keypair(cfg.identity_type or "ed25519")
    if not generated then
      return nil, gen_err
    end
    keypair = generated
  end

  local local_peer, local_peer_err = keys.peer_id(keypair)
  if not local_peer then
    return nil, local_peer_err
  end

  local self_obj = setmetatable({
    identity = keypair,
    _peer_id = local_peer,
    peerstore = cfg.peerstore or peerstore.new(cfg.peerstore_options),
    address_manager = cfg.address_manager or address_manager.new({
      listen_addrs = cfg.listen_addrs or {},
      announce_addrs = cfg.announce_addrs or {},
      no_announce_addrs = cfg.no_announce_addrs or {},
      observed_addrs = cfg.observed_addrs or {},
      relay_addrs = cfg.relay_addrs or {},
      advertise_observed = cfg.advertise_observed,
    }),
    listen_addrs = list_copy(cfg.listen_addrs or {}),
    transports = list_copy(cfg.transports or { "tcp" }),
    security_transports = {},
    muxers = {},
    _security_protocols = {},
    _muxer_protocols = {},
    _services = {},
    services = {},
    components = {},
    capabilities = {},
    _tasks = {},
    _task_queue = {},
    _task_queue_head = 1,
    _task_queue_tail = 0,
    _sleeping_tasks = {},
    _task_read_waiters = {},
    _task_dial_waiters = {},
    _task_write_waiters = {},
    _task_completion_waiters = {},
    _next_task_id = 1,
    _task_resume_budget = cfg.task_resume_budget or host_tasks.DEFAULT_TASK_RESUME_BUDGET,
    _task_retention = cfg.task_retention or host_tasks.DEFAULT_TASK_RETENTION,
    _task_prune_interval = cfg.task_prune_interval or host_tasks.DEFAULT_TASK_PRUNE_INTERVAL,
    _pending_inbound = {},
    _pending_relay_inbound = {},
    _debug_counters = {},
    _debug_maps = {},
    _debug_connection_events = cfg.debug_connection_events == true,
    _identify_on_connect_handler = nil,
    _runtime = runtime_name,
    _runtime_impl = runtime_impl,
    _tcp_transport = tcp_luv,
    _luv_tick_timer = nil,
    _luv_watchers = {},
    _luv_ready = {},
    _runtime_last_error = nil,
    _start_blocking = start_blocking ~= false,
    _start_max_iterations = cfg.max_iterations,
    _start_poll_interval = cfg.poll_interval or 0.01,
    _on_started = cfg.on_started,
    _running = false,
    _connect_timeout = cfg.connect_timeout or 6,
    _io_timeout = cfg.io_timeout or 10,
    _accept_timeout = cfg.accept_timeout or 0,
    _tcp_options = tcp_options_from_config(cfg),
    resource_manager = resource_manager_from_config(cfg),
    _service_options = {
      identify = cfg.identify or {},
      autonat = cfg.autonat or {},
      autorelay = cfg.autorelay or {},
      kad_dht = cfg.kad_dht or {},
      perf = cfg.perf or {},
      upnp_nat = cfg.upnp_nat or {},
    },
  }, self)
  host_advertise.init(self_obj)
  local bootstrap_ok, bootstrap_err = host_bootstrap.init(self_obj, cfg.peer_discovery)
  if not bootstrap_ok then
    return nil, bootstrap_err
  end
  host_connections.init(self_obj)
  host_events.init(self_obj, cfg)
  host_identify.init(self_obj)
  host_listeners.init(self_obj)
  host_protocols.init(self_obj)
  self_obj.connection_manager = connection_manager_from_config(self_obj, cfg)

  local security_spec = cfg.security_transports or { noise = true }
  local security_protocols, security_err = connection_encrypter.normalize_protocols(security_spec)
  if not security_protocols then
    return nil, security_err
  end
  self_obj.security_transports = map_copy(security_spec)
  self_obj._security_protocols = security_protocols

  local muxer_spec = cfg.muxers or { yamux = true }
  local muxer_protocols, muxer_err = muxer_registry.normalize_protocols(muxer_spec)
  if not muxer_protocols then
    return nil, muxer_err
  end
  self_obj.muxers = map_copy(muxer_spec)
  self_obj._muxer_protocols = muxer_protocols

  if type(cfg.on) == "table" then
    for event_name, handler in pairs(cfg.on) do
      local ok, on_err = self_obj:on(event_name, handler)
      if not ok then
        return nil, on_err
      end
    end
  end

  log.debug("host constructed", {
    peer_id = self_obj:peer_id().id,
    runtime = runtime_name,
    listen_addrs = #self_obj.listen_addrs,
    transports = table.concat(self_obj.transports, ","),
    security_transports = table.concat(self_obj._security_protocols, ","),
    muxers = table.concat(self_obj._muxer_protocols, ","),
  })

  local services = cfg.services
  if
    contains_circuit_listen_addr(self_obj.listen_addrs)
    and not (type(services) == "table" and services.autorelay ~= nil)
  then
    return nil, error_mod.new("input", "/p2p-circuit listen addr requires autorelay service")
  end
  local services_ok, services_err = host_service_manager.install(self_obj, services)
  if not services_ok then
    return nil, services_err
  end

  if cfg.service ~= nil then
    return nil, error_mod.new("input", "service option is removed; use services map")
  end

  return self_obj
end

function Host:is_running()
  return self._running
end

function Host:_spawn_stream_negotiation_task(stream, conn, entry)
  if type(self.spawn_task) ~= "function" then
    return nil, error_mod.new("unsupported", "stream negotiation task requires scheduler")
  end
  return self:spawn_task("host.stream_negotiate", function()
    local router, router_err = self:_build_router()
    if not router then
      return nil, router_err
    end
    local stream_scope, resource_err = self:_open_stream_resource(entry, "inbound")
    if resource_err then
      if type(stream.reset_now) == "function" then
        pcall(function()
          stream:reset_now()
        end)
      elseif type(stream.close) == "function" then
        pcall(function()
          stream:close()
        end)
      end
      return nil, resource_err
    end
    local protocol_id, handler, handler_options, neg_err = router:negotiate(stream)
    if not protocol_id then
      self:_close_stream_resource(stream_scope)
      log.debug("host inbound stream negotiation failed", {
        peer_id = entry.state and entry.state.remote_peer_id or nil,
        connection_id = entry.id,
        cause = tostring(neg_err),
      })
      return nil, neg_err
    end
    local set_ok, set_err = self:_set_stream_resource_protocol(stream_scope, protocol_id)
    if not set_ok then
      if type(stream.reset_now) == "function" then
        pcall(function()
          stream:reset_now()
        end)
      elseif type(stream.close) == "function" then
        pcall(function()
          stream:close()
        end)
      end
      self:_close_stream_resource(stream_scope)
      log.debug("host inbound stream resource protocol failed", {
        peer_id = entry.state and entry.state.remote_peer_id or nil,
        connection_id = entry.id,
        protocol = protocol_id,
        cause = tostring(set_err),
      })
      return nil, set_err
    end
    stream = self:_wrap_stream_resource(stream, stream_scope)
    if
      self:_connection_is_limited(entry.state)
      and not self:_protocol_allowed_on_limited_connection(protocol_id, handler_options)
    then
      if type(stream.reset_now) == "function" then
        pcall(function()
          stream:reset_now()
        end)
      elseif type(stream.close) == "function" then
        pcall(function()
          stream:close()
        end)
      end
      self:_release_stream_resource(stream)
      log.debug("host inbound stream blocked on limited connection", {
        peer_id = entry.state and entry.state.remote_peer_id or nil,
        connection_id = entry.id,
        protocol = protocol_id,
      })
      return true
    end
    log.debug("host inbound stream negotiated", {
      peer_id = entry.state and entry.state.remote_peer_id or nil,
      connection_id = entry.id,
      protocol = protocol_id,
      limited = self:_connection_is_limited(entry.state),
    })
    if self._debug_connection_events then
      emit_event(self, "stream:negotiated", {
        peer_id = entry.state and entry.state.remote_peer_id or nil,
        connection_id = entry.id,
        protocol = protocol_id,
        limited = self:_connection_is_limited(entry.state),
        relay_limit_kind = entry.state and entry.state.relay and entry.state.relay.limit_kind or nil,
        direction = entry.state and entry.state.direction or nil,
        remote_addr = entry.state and entry.state.remote_addr or nil,
      })
    end
    if handler then
      log.debug("host handler task spawning", {
        peer_id = entry.state and entry.state.remote_peer_id or nil,
        connection_id = entry.id,
        protocol = protocol_id,
      })
      self:_spawn_handler_task(handler, {
        stream = stream,
        host = self,
        connection = conn,
        state = entry.state,
        protocol = protocol_id,
      })
    end
    return true
  end, { service = "host" })
end

function Host:add_service(_)
  return nil, error_mod.new("input", "add_service is removed; pass services map to host.new")
end

function Host:_handle_relay_stop(stream, ctx)
  local accepted, accept_err = relay_proto.accept_stop(stream, {
    max_message_size = self._max_relay_message_size,
  })
  if not accepted then
    return nil, accept_err
  end
  local relay_state = {
    relay_peer_id = ctx and ctx.state and ctx.state.remote_peer_id or nil,
    initiator_peer_id_bytes = accepted.initiator_peer_id_bytes,
    initiator_addrs = accepted.initiator_addrs,
    limit = accepted.limit,
    limit_kind = accepted.limit_kind,
    direction = "inbound",
  }
  local conn, state, up_err = upgrader.upgrade_inbound(stream, {
    local_keypair = self.identity,
    security_protocols = self.security_transports,
    muxer_protocols = self.muxers,
    ctx = ctx,
  })
  if not conn then
    return nil, up_err
  end
  state.direction = state.direction or "inbound"
  state.relay = relay_state
  local entry, register_err = self:_register_connection(conn, state)
  if not entry then
    conn:close()
    return nil, register_err
  end
  return true
end

function Host:peer_id()
  return self._peer_id
end

function Host:_open_connection_resource(direction, peer_id, opts)
  if not self.resource_manager or type(self.resource_manager.open_connection) ~= "function" then
    return nil
  end
  return self.resource_manager:open_connection(direction, peer_id, opts)
end

function Host:_close_connection_resource(scope)
  if scope and self.resource_manager and type(self.resource_manager.close_connection) == "function" then
    return self.resource_manager:close_connection(scope)
  end
  return true
end

function Host:_set_connection_resource_peer(scope, peer_id)
  if scope and peer_id and self.resource_manager and type(self.resource_manager.set_connection_peer) == "function" then
    return self.resource_manager:set_connection_peer(scope, peer_id)
  end
  return true
end

function Host:_open_stream_resource(entry, direction, protocol_id)
  if not self.resource_manager or type(self.resource_manager.open_stream) ~= "function" then
    return nil
  end
  local peer_id = entry and entry.state and entry.state.remote_peer_id or nil
  return self.resource_manager:open_stream(peer_id, direction, protocol_id)
end

function Host:_set_stream_resource_protocol(scope, protocol_id)
  if scope and self.resource_manager and type(self.resource_manager.set_stream_protocol) == "function" then
    return self.resource_manager:set_stream_protocol(scope, protocol_id)
  end
  return true
end

function Host:_close_stream_resource(scope)
  if scope and self.resource_manager and type(self.resource_manager.close_stream) == "function" then
    return self.resource_manager:close_stream(scope)
  end
  return true
end

function Host:_wrap_stream_resource(stream, scope)
  if not scope or type(stream) ~= "table" or stream._resource_managed_stream then
    return stream
  end
  local wrapper = {
    _inner = stream,
    _resource_scope = scope,
    _resource_host = self,
    _resource_managed_stream = true,
  }
  function wrapper:close(...)
    local inner = self._inner
    local result, err = true, nil
    if inner and type(inner.close) == "function" then
      result, err = inner:close(...)
    end
    self._resource_host:_release_stream_resource(self)
    return result, err
  end
  function wrapper:reset_now(...)
    local inner = self._inner
    local result, err = true, nil
    if inner and type(inner.reset_now) == "function" then
      result, err = inner:reset_now(...)
    elseif inner and type(inner.close) == "function" then
      result, err = inner:close(...)
    end
    self._resource_host:_release_stream_resource(self)
    return result, err
  end
  return setmetatable(wrapper, {
    __index = function(t, key)
      local inner = rawget(t, "_inner")
      local value = inner and inner[key]
      if type(value) == "function" then
        return function(_, ...)
          return value(inner, ...)
        end
      end
      return value
    end,
  })
end

function Host:_release_stream_resource(stream)
  if type(stream) == "table" and stream._resource_managed_stream and stream._resource_scope then
    local scope = stream._resource_scope
    stream._resource_scope = nil
    return self:_close_stream_resource(scope)
  end
  return true
end

function Host:_register_connection(conn, state)
  state = state or {}
  state.direction = state.direction or "unknown"
  local resource_scope = state.resource_scope
  if not resource_scope then
    local opened_scope, resource_err = self:_open_connection_resource(state.direction, state.remote_peer_id)
    if resource_err then
      return nil, resource_err
    end
    resource_scope = opened_scope
    state.resource_scope = resource_scope
  else
    local set_ok, set_err = self:_set_connection_resource_peer(resource_scope, state.remote_peer_id)
    if not set_ok then
      self:_close_connection_resource(resource_scope)
      return nil, set_err
    end
  end
  if self.connection_manager and type(self.connection_manager.can_open_connection) == "function" then
    local can_open, limit_err = self.connection_manager:can_open_connection(state)
    if not can_open then
      self:_close_connection_resource(resource_scope)
      return nil, limit_err
    end
  end
  local entry = host_connections.add(self, conn, state)
  local connection_id = entry.id
  local peer_id = entry.state.remote_peer_id

  local function rollback_registration()
    host_connections.remove(self, entry)
    self:_close_connection_resource(entry.state and entry.state.resource_scope)
    if
      entry._connection_manager_tracked
      and self.connection_manager
      and type(self.connection_manager.on_connection_closed) == "function"
    then
      self.connection_manager:on_connection_closed(entry)
    end
  end

  local ok, emit_err = emit_event(self, "peer_connected", {
    connection = entry.conn,
    connection_id = connection_id,
    state = entry.state,
    peer_id = peer_id,
  })
  if not ok then
    rollback_registration()
    log.debug("host connection event failed", {
      connection_id = connection_id,
      peer_id = peer_id,
      event = "peer_connected",
      cause = tostring(emit_err),
    })
    return nil, emit_err
  end

  local opened_ok, opened_err = emit_event(self, "connection_opened", {
    connection = entry.conn,
    connection_id = connection_id,
    state = entry.state,
    peer_id = peer_id,
  })
  if not opened_ok then
    rollback_registration()
    log.debug("host connection event failed", {
      connection_id = connection_id,
      peer_id = peer_id,
      event = "connection_opened",
      cause = tostring(opened_err),
    })
    return nil, opened_err
  end

  if self._running and self._runtime_impl and self._runtime_impl.sync_watchers then
    local sync_ok, sync_err = self._runtime_impl.sync_watchers(self)
    if not sync_ok then
      rollback_registration()
      return nil, sync_err
    end
  end

  if
    host_runtime_luv_native.is_native_host(self)
    and (type(entry.conn.pump_once) == "function" or type(entry.conn.process_one) == "function")
  then
    local _, pump_err = host_runtime_luv_native.start_connection_pump_task(self, entry, is_nonfatal_stream_error)
    if pump_err then
      rollback_registration()
      return nil, pump_err
    end
  end

  if self.connection_manager and type(self.connection_manager.on_connection_opened) == "function" then
    local tracked, track_err = self.connection_manager:on_connection_opened(entry)
    if not tracked then
      rollback_registration()
      return nil, track_err
    end
    entry._connection_manager_tracked = true
  end

  log.debug("connection opened", {
    peer_id = peer_id,
    connection_id = connection_id,
    direction = entry.state and entry.state.direction or nil,
    security = entry.state and entry.state.security or nil,
    muxer = entry.state and entry.state.muxer or nil,
    relay_limit_kind = entry.state and entry.state.relay and entry.state.relay.limit_kind or nil,
  })

  return entry
end

function Host:_unregister_connection(index, entry, cause)
  if not entry then
    return false
  end
  if entry.scheduler_pump_task and entry.scheduler_pump_task.status ~= "completed" then
    entry.scheduler_pump_task.on_finished = nil
    self:cancel_task(entry.scheduler_pump_task.id)
  end
  local peer_id = entry.state and entry.state.remote_peer_id
  host_connections.remove(self, entry, index)
  log.debug("connection closed", {
    peer_id = peer_id,
    connection_id = entry.id,
    direction = entry.state and entry.state.direction or nil,
    security = entry.state and entry.state.security or nil,
    muxer = entry.state and entry.state.muxer or nil,
    cause = tostring(cause),
  })
  if self.connection_manager and type(self.connection_manager.on_connection_closed) == "function" then
    self.connection_manager:on_connection_closed(entry)
  end
  self:_close_connection_resource(entry.state and entry.state.resource_scope)
  local peer_ok, peer_err = emit_event(self, "peer_disconnected", {
    connection = entry.conn,
    connection_id = entry.id,
    state = entry.state,
    peer_id = peer_id,
    cause = cause,
  })
  if not peer_ok then
    return nil, peer_err
  end
  local ok, emit_err = emit_event(self, "connection_closed", {
    connection = entry.conn,
    connection_id = entry.id,
    state = entry.state,
    peer_id = peer_id,
    cause = cause,
  })
  if not ok then
    return nil, emit_err
  end
  return true
end

function Host:stats()
  local connection_stats = nil
  if self.connection_manager and type(self.connection_manager.stats) == "function" then
    connection_stats = self.connection_manager:stats()
  end
  local resource_stats = nil
  if self.resource_manager and type(self.resource_manager.stats) == "function" then
    resource_stats = self.resource_manager:stats()
  end
  return {
    runtime = self._runtime,
    running = self._running == true,
    peer_id = self:peer_id().id,
    tasks = self:task_stats(),
    connections = connection_stats,
    resources = resource_stats,
  }
end

local function count_map(values)
  local n = 0
  for _ in pairs(values or {}) do
    n = n + 1
  end
  return n
end

local function top_count_string(map, limit)
  local items = {}
  for key, value in pairs(map or {}) do
    items[#items + 1] = { key = tostring(key), value = tonumber(value) or 0 }
  end
  table.sort(items, function(a, b)
    if a.value == b.value then
      return a.key < b.key
    end
    return a.value > b.value
  end)

  local parts = {}
  local max_items = math.min(limit or 5, #items)
  for i = 1, max_items do
    local item = items[i]
    parts[#parts + 1] = string.format("%s=%d", item.key, item.value)
  end
  if #parts == 0 then
    return "none"
  end
  return table.concat(parts, ",")
end

--- Return a reusable host diagnostic snapshot.
-- This intentionally exposes aggregate counters rather than private tables so
-- examples and operators can log health without reaching into host internals.
function Host:health_stats()
  local connmgr = self.connection_manager
  local dial_queue = 0
  local pending_by_key = 0
  if connmgr then
    dial_queue = #(connmgr.queue or {})
    pending_by_key = count_map(connmgr.pending_by_key)
  end

  local listener_pending = 0
  local listener_callbacks = 0
  local listener_accepted = 0
  local listener_accept_failed = 0
  local listener_callback_errors = 0
  for _, listener in ipairs(self._listeners or {}) do
    if listener and type(listener.stats) == "function" then
      local stats = listener:stats()
      listener_pending = listener_pending + (tonumber(stats.pending) or 0)
      listener_callbacks = listener_callbacks + (tonumber(stats.callback_total) or 0)
      listener_accepted = listener_accepted + (tonumber(stats.accepted_total) or 0)
      listener_accept_failed = listener_accept_failed + (tonumber(stats.accept_failed_total) or 0)
      listener_callback_errors = listener_callback_errors + (tonumber(stats.callback_error_total) or 0)
    end
  end

  local counters = rawget(self, "_debug_counters") or {}
  local debug_maps = rawget(self, "_debug_maps") or {}
  local task_stats = nil
  if type(self.task_stats) == "function" then
    task_stats = self:task_stats()
  end
  task_stats = task_stats or {}

  return {
    health = {
      tasks_total = count_map(self._tasks),
      queue = type(self._task_queue_depth) == "function" and self:_task_queue_depth() or #(self._task_queue or {}),
      wait_read = count_map(self._task_read_waiters),
      wait_dial = count_map(self._task_dial_waiters),
      wait_write = count_map(self._task_write_waiters),
      wait_done = count_map(self._task_completion_waiters),
      pending_inbound = #(self._pending_inbound or {}),
      pending_relay_inbound = #(self._pending_relay_inbound or {}),
      listener_pending = listener_pending,
      conns = #(self._connections or {}),
      dial_queue = dial_queue,
      pending_keys = pending_by_key,
      mem_kb = collectgarbage("count"),
    },
    listener = {
      callbacks = listener_callbacks,
      accepted = listener_accepted,
      accept_failed = listener_accept_failed,
      callback_errors = listener_callback_errors,
      pending = listener_pending,
    },
    inbound = {
      accept = tonumber(counters.inbound_accept) or 0,
      upgrade_started = tonumber(counters.inbound_upgrade_started) or 0,
      upgrade_completed = tonumber(counters.inbound_upgrade_completed) or 0,
      upgrade_failed = tonumber(counters.inbound_upgrade_failed) or 0,
      upgrade_task_ended = tonumber(counters.inbound_upgrade_task_ended) or 0,
      resource_reject = tonumber(counters.inbound_resource_reject) or 0,
      registration_failed = tonumber(counters.inbound_registration_failed) or 0,
      pump_terminal = tonumber(counters.connection_pump_terminal) or 0,
    },
    inbound_fail = {
      kinds = top_count_string(debug_maps.inbound_upgrade_failed_by_kind, 4),
      causes = top_count_string(debug_maps.inbound_upgrade_failed_by_cause, 4),
    },
    task_top = {
      status = top_count_string(task_stats.by_status, 4),
      service = top_count_string(task_stats.by_service, 4),
      names = top_count_string(task_stats.by_name, 6),
    },
  }
end

function Host:_set_runtime_error(runtime_name, err)
  self._runtime_last_error = err
  self._running = false
  local fields = flatten_error_fields(err, "cause", {
    runtime = runtime_name,
  })
  log.error("host runtime tick failed", fields)

  local ok, emit_err = emit_event(self, "host_runtime_error", {
    runtime = runtime_name,
    cause = err,
  })
  if not ok then
    log.error("host runtime error emit failed", {
      runtime = runtime_name,
      cause = tostring(emit_err),
    })
  end
end

function Host:_process_runtime_events(timeout, ready_map)
  local perf = rawget(self, "_debug_perf")
  local phase_started = perf and now_seconds() or nil

  if ready_map then
    for connection in pairs(self._task_read_waiters) do
      if ready_map[connection] then
        self:_wake_task_readers(connection)
      end
    end
    for connection in pairs(self._task_write_waiters) do
      if ready_map[connection] then
        self:_wake_task_writers(connection)
      end
    end
    for connection in pairs(self._task_dial_waiters) do
      if ready_map[connection] then
        self:_wake_task_dialers(connection)
      end
    end
  end
  if phase_started then
    debug_perf_add(self, "process_ready_waiters", now_seconds() - phase_started)
    phase_started = now_seconds()
  end

  local function drain_listener_accepts()
    for _, listener in ipairs(self._listeners) do
      local should_accept = true
      if ready_map then
        should_accept = ready_map[listener] == true
        if not should_accept and host_runtime_luv_native.is_native_host(self) then
          should_accept = true
        end
      end

      local max_accepts = host_runtime_luv_native.is_native_host(self) and self._max_accepts_per_tick or 1
      local accepted_this_tick = 0
      while should_accept and accepted_this_tick < max_accepts do
        local accept_timeout = timeout or self._accept_timeout
        if ready_map or accepted_this_tick > 0 then
          accept_timeout = 0
        end
        local raw_conn, accept_err = listener:accept(accept_timeout)
        if not raw_conn then
          if accept_err and error_mod.is_error(accept_err) and accept_err.kind ~= "timeout" then
            return nil, accept_err
          end
          break
        end
        accepted_this_tick = accepted_this_tick + 1
        self:_bump_debug_counter("inbound_accept")
        log.debug("host inbound raw connection accepted", raw_conn_log_fields(raw_conn))
        local resource_scope, resource_err = self:_open_connection_resource("inbound", nil, { transient = true })
        if resource_err then
          self:_bump_debug_counter("inbound_resource_reject")
          log.debug(
            "host inbound raw connection closing after resource failure",
            raw_conn_log_fields(raw_conn, {
              cause = tostring(resource_err),
            })
          )
          raw_conn:close()
          log.debug(
            "host inbound connection resource failed",
            flatten_error_fields(
              resource_err,
              "cause",
              raw_conn_log_fields(raw_conn, {
                cause = tostring(resource_err),
              })
            )
          )
          if not (error_mod.is_error(resource_err) and resource_err.kind == "resource") then
            return nil, resource_err
          end
          goto continue_accept_loop
        end
        log.debug("host inbound connection resource opened", raw_conn_log_fields(raw_conn))
        if host_runtime_luv_native.is_native_host(self) then
          self._pending_inbound[#self._pending_inbound + 1] = { raw_conn = raw_conn, resource_scope = resource_scope }
          log.debug(
            "host inbound upgrade pending",
            raw_conn_log_fields(raw_conn, {
              pending_inbound = #self._pending_inbound,
            })
          )
          goto continue_accept_loop
        end

        if raw_conn and type(raw_conn.begin_read_tx) == "function" then
          raw_conn:begin_read_tx()
        end
        local conn, state, up_err = upgrader.upgrade_inbound(raw_conn, {
          local_keypair = self.identity,
          security_protocols = self.security_transports,
          muxer_protocols = self.muxers,
        })
        if not conn then
          self:_bump_debug_counter("inbound_upgrade_failed")
          if
            up_err
            and error_mod.is_error(up_err)
            and up_err.kind == "timeout"
            and self._runtime == "luv"
            and self._tcp_transport
            and self._tcp_transport.BACKEND == "luv-native"
          then
            if raw_conn and type(raw_conn.rollback_read_tx) == "function" then
              raw_conn:rollback_read_tx()
            end
            self._pending_inbound[#self._pending_inbound + 1] = { raw_conn = raw_conn, resource_scope = resource_scope }
            goto continue_accept_loop
          end

          if raw_conn and type(raw_conn.rollback_read_tx) == "function" then
            raw_conn:rollback_read_tx()
          end
          self:_close_connection_resource(resource_scope)
          log.debug(
            "host inbound raw connection closing after upgrade failure",
            raw_conn_log_fields(raw_conn, {
              cause = tostring(up_err),
            })
          )
          raw_conn:close()
          log.debug(
            "host inbound upgrade failed",
            raw_conn_log_fields(raw_conn, {
              cause = tostring(up_err),
            })
          )
          if not is_nonfatal_stream_error(up_err) then
            return nil, up_err
          end
          goto continue_accept_loop
        end
        if raw_conn and type(raw_conn.commit_read_tx) == "function" then
          raw_conn:commit_read_tx()
        end
        state.direction = state.direction or "inbound"
        state.resource_scope = resource_scope
        local entry, register_err = self:_register_connection(conn, state)
        if not entry then
          self:_bump_debug_counter("inbound_registration_failed")
          conn:close()
          log.debug("host inbound registration failed", {
            cause = tostring(register_err),
          })
          return nil, register_err
        end

        ::continue_accept_loop::
      end
    end

    return true
  end

  if host_runtime_luv_native.is_native_host(self) then
    local listeners_ok, listeners_err = drain_listener_accepts()
    if not listeners_ok then
      return nil, listeners_err
    end
    if phase_started then
      debug_perf_add(self, "process_listeners", now_seconds() - phase_started)
      phase_started = now_seconds()
    end
  end

  local function remove_pending_relay_inbound(index, pending)
    if self._pending_relay_inbound[index] == pending then
      table.remove(self._pending_relay_inbound, index)
      return true
    end
    for j = #self._pending_relay_inbound, 1, -1 do
      if self._pending_relay_inbound[j] == pending then
        table.remove(self._pending_relay_inbound, j)
        return true
      end
    end
    return false
  end

  for i = #self._pending_relay_inbound, 1, -1 do
    local pending = self._pending_relay_inbound[i]
    if not pending then
      goto continue_pending_relay_inbound
    end
    local raw_conn = pending.raw_conn
    local conn, state, up_err = upgrader.upgrade_inbound(raw_conn, {
      local_keypair = self.identity,
      security_protocols = self.security_transports,
      muxer_protocols = self.muxers,
    })
    if conn then
      remove_pending_relay_inbound(i, pending)
      state.direction = state.direction or "inbound"
      state.relay = pending.relay
      state.resource_scope = pending.resource_scope
      local entry, register_err = self:_register_connection(conn, state)
      if not entry then
        conn:close()
        log.debug("host relay inbound registration failed", {
          cause = tostring(register_err),
        })
        return nil, register_err
      end
    elseif not (up_err and error_mod.is_error(up_err) and up_err.kind == "timeout") then
      remove_pending_relay_inbound(i, pending)
      self:_close_connection_resource(pending.resource_scope)
      if raw_conn and type(raw_conn.close) == "function" then
        raw_conn:close()
      end
      log.debug("host relay inbound upgrade failed", {
        cause = tostring(up_err),
      })
      if not is_nonfatal_stream_error(up_err) then
        return nil, up_err
      end
    end
    ::continue_pending_relay_inbound::
  end
  if phase_started then
    debug_perf_add(self, "process_pending_relay_inbound", now_seconds() - phase_started)
    phase_started = now_seconds()
  end

  for i = #self._pending_inbound, 1, -1 do
    local pending_entry = self._pending_inbound[i]
    local raw_conn = host_runtime_luv_native.pending_raw(pending_entry)

    if host_runtime_luv_native.is_native_host(self) then
      local status, _, resume_err, normalized_entry =
        host_runtime_luv_native.resume_inbound_upgrade(self, pending_entry, is_nonfatal_stream_error)
      self._pending_inbound[i] = normalized_entry
      if status == "pending" then
        goto continue_pending_inbound
      end
      table.remove(self._pending_inbound, i)
      if status == "error" then
        return nil, resume_err
      end
      goto continue_pending_inbound
    end

    if raw_conn and type(raw_conn.begin_read_tx) == "function" then
      raw_conn:begin_read_tx()
    end
    local conn, state, up_err = upgrader.upgrade_inbound(raw_conn, {
      local_keypair = self.identity,
      security_protocols = self.security_transports,
      muxer_protocols = self.muxers,
    })
    if conn then
      if raw_conn and type(raw_conn.commit_read_tx) == "function" then
        raw_conn:commit_read_tx()
      end
      state.direction = state.direction or "inbound"
      if type(pending_entry) == "table" then
        state.resource_scope = pending_entry.resource_scope
      end
      table.remove(self._pending_inbound, i)
      local entry, register_err = self:_register_connection(conn, state)
      if not entry then
        conn:close()
        return nil, register_err
      end
    elseif up_err and error_mod.is_error(up_err) and up_err.kind == "timeout" then
      if raw_conn and type(raw_conn.rollback_read_tx) == "function" then
        raw_conn:rollback_read_tx()
      end
      -- keep pending; retry on next poll tick
    elseif is_nonfatal_stream_error(up_err) then
      if raw_conn and type(raw_conn.rollback_read_tx) == "function" then
        raw_conn:rollback_read_tx()
      end
      table.remove(self._pending_inbound, i)
      if type(pending_entry) == "table" then
        self:_close_connection_resource(pending_entry.resource_scope)
      end
      raw_conn:close()
    else
      if raw_conn and type(raw_conn.rollback_read_tx) == "function" then
        raw_conn:rollback_read_tx()
      end
      table.remove(self._pending_inbound, i)
      if type(pending_entry) == "table" then
        self:_close_connection_resource(pending_entry.resource_scope)
      end
      raw_conn:close()
      return nil, up_err
    end

    ::continue_pending_inbound::
  end
  if phase_started then
    debug_perf_add(self, "process_pending_inbound", now_seconds() - phase_started)
    phase_started = now_seconds()
  end

  if not host_runtime_luv_native.is_native_host(self) then
    local listeners_ok, listeners_err = drain_listener_accepts()
    if not listeners_ok then
      return nil, listeners_err
    end
  end
  if phase_started then
    debug_perf_add(self, "process_listeners", now_seconds() - phase_started)
    phase_started = now_seconds()
  end

  local router = nil
  if host_runtime_luv_native.is_native_host(self) then
    local router_err
    router, router_err = self:_build_router()
    if not router then
      return nil, router_err
    end
  end
  if phase_started then
    debug_perf_add(self, "process_router", now_seconds() - phase_started)
    phase_started = now_seconds()
  end

  for i = #self._connections, 1, -1 do
    local entry = self._connections[i]
    local conn = entry.conn
    local should_process = true
    if ready_map then
      should_process = ready_map[entry] == true
    end

    if should_process then
      if host_runtime_luv_native.is_native_host(self) then
        local ok, native_err = host_runtime_luv_native.process_connection(self, entry, router, is_nonfatal_stream_error)
        if not ok then
          return nil, native_err
        end
        goto continue_connections
      end

      local _, process_err
      if type(conn.pump_once) == "function" then
        _, process_err = conn:pump_once()
      else
        _, process_err = conn:process_one()
      end
      if process_err then
        if is_nonfatal_stream_error(process_err) then
          if process_err.kind ~= "timeout" then
            conn:close()
            local unregistered, unregister_err = self:_unregister_connection(i, entry, process_err)
            if not unregistered then
              return nil, unregister_err
            end
            if self._runtime_impl and self._runtime_impl.sync_watchers then
              local sync_ok, sync_err = self._runtime_impl.sync_watchers(self)
              if not sync_ok then
                return nil, sync_err
              end
            end
          end
          goto continue_connections
        end
        return nil, process_err
      end
    end

    local stream, stream_err
    if type(conn.accept_stream_raw) == "function" then
      stream, stream_err = conn:accept_stream_raw()
      if stream_err then
        if is_nonfatal_stream_error(stream_err) then
          goto continue_connections
        end
        return nil, stream_err
      end
      if stream then
        log.debug("host inbound raw stream accepted", {
          peer_id = entry.state and entry.state.remote_peer_id or nil,
          connection_id = entry.id,
        })
        local _, neg_task_err = self:_spawn_stream_negotiation_task(stream, conn, entry)
        if neg_task_err then
          return nil, neg_task_err
        end
      end
    else
      local protocol_id, handler, handler_options
      stream, protocol_id, handler, handler_options, stream_err = conn:accept_stream(router)
      if stream_err then
        if is_nonfatal_stream_error(stream_err) then
          goto continue_connections
        end
        return nil, stream_err
      end
      if stream and handler then
        log.debug("host inbound stream accepted", {
          peer_id = entry.state and entry.state.remote_peer_id or nil,
          connection_id = entry.id,
          protocol = protocol_id,
        })
        local stream_scope, resource_err = self:_open_stream_resource(entry, "inbound", protocol_id)
        if resource_err then
          if type(stream.reset_now) == "function" then
            pcall(function()
              stream:reset_now()
            end)
          elseif type(stream.close) == "function" then
            pcall(function()
              stream:close()
            end)
          end
          log.debug("host inbound stream resource failed", {
            peer_id = entry.state and entry.state.remote_peer_id or nil,
            connection_id = entry.id,
            protocol = protocol_id,
            cause = tostring(resource_err),
          })
          return nil, resource_err
        end
        stream = self:_wrap_stream_resource(stream, stream_scope)
        if
          self:_connection_is_limited(entry.state)
          and not self:_protocol_allowed_on_limited_connection(protocol_id, handler_options)
        then
          if type(stream.reset_now) == "function" then
            pcall(function()
              stream:reset_now()
            end)
          elseif type(stream.close) == "function" then
            pcall(function()
              stream:close()
            end)
          end
          log.debug("host inbound stream blocked on limited connection", {
            peer_id = entry.state and entry.state.remote_peer_id or nil,
            connection_id = entry.id,
            protocol = protocol_id,
          })
          goto continue_connections
        end
        self:_spawn_handler_task(handler, {
          stream = stream,
          host = self,
          connection = conn,
          state = entry.state,
          protocol = protocol_id,
        })
      end
    end

    ::continue_connections::
  end
  if phase_started then
    debug_perf_add(self, "process_connections", now_seconds() - phase_started)
    phase_started = now_seconds()
  end

  local bg_ok, bg_err = self:_run_background_tasks()
  if not bg_ok then
    return nil, bg_err
  end
  if phase_started then
    debug_perf_add(self, "process_background", now_seconds() - phase_started)
  end

  return true
end

function Host:_poll_once_with_ready_map(timeout, ready_map)
  return self:_process_runtime_events(timeout, ready_map)
end

function Host:_poll_once(timeout)
  if self._runtime_impl and self._runtime_impl.poll_once then
    return self._runtime_impl.poll_once(self, timeout)
  end
  return nil, error_mod.new("state", "host runtime has no poll_once implementation", { runtime = self._runtime })
end

---Start the host runtime.
---Binds listeners when needed and enters the runtime loop for blocking hosts.
---@return true|nil ok
---@return table|nil err
function Host:start()
  log.debug("host start requested", {
    peer_id = self:peer_id().id,
    runtime = self._runtime,
    listeners = #self._listeners,
    blocking = self._start_blocking,
  })
  if #self._listeners == 0 then
    local ok, bind_err = self:_bind_listeners()
    if not ok then
      return nil, bind_err
    end
    if not self._listeners[1] and (not self.autorelay or contains_non_circuit_listen_addr(self.listen_addrs)) then
      return nil, error_mod.new("state", "no listeners bound")
    end
  end

  if not self._running then
    self._running = true
  end
  if self._bootstrap_discovery and self._bootstrap_discovery.dial_on_start ~= false then
    log.debug("host bootstrap discovery scheduling", {
      timeout = self._bootstrap_discovery.timeout or 1,
    })
    local boot_ok, boot_err = self:_schedule_bootstrap_discovery(self._bootstrap_discovery.timeout or 1)
    if not boot_ok then
      return nil, boot_err
    end
  end
  local services_started_ok, services_started_err = host_service_manager.on_host_started(self)
  if not services_started_ok then
    return nil, services_started_err
  end
  local self_update_ok, self_update_err = self:_emit_self_peer_update_if_changed()
  if not self_update_ok then
    return nil, self_update_err
  end

  if type(self._on_started) == "function" then
    self._on_started(self)
  end

  log.debug("host started", {
    peer_id = self:peer_id().id,
    runtime = self._runtime,
    listeners = #self._listeners,
    listen_addrs = #self.listen_addrs,
    services = #(self._service_order or {}),
  })

  if self._runtime_impl and self._runtime_impl.start then
    local runtime_ok, runtime_err, handled = self._runtime_impl.start(self)
    if not runtime_ok then
      return nil, runtime_err
    end
    if handled then
      return true
    end
  end

  local blocking = self._start_blocking
  if blocking then
    local iterations = 0
    local max_iterations = self._start_max_iterations
    local poll_interval = self._start_poll_interval
    while self._running do
      local ok, err = self:_poll_once(self._accept_timeout)
      if not ok then
        self._running = false
        return nil, err
      end
      iterations = iterations + 1
      if max_iterations and iterations >= max_iterations then
        break
      end
      if poll_interval > 0 then
        sleep_seconds(poll_interval)
      end
    end
  end

  return true
end

---Stop and close the host.
---@return true|nil ok
---@return table|nil err
function Host:stop()
  log.debug("host stop requested", {
    peer_id = self:peer_id().id,
  })
  self._running = false
  return self:close()
end

---Close listeners, connections, tasks, and services.
---@return true|nil ok
---@return table|nil err
function Host:close()
  log.debug("host close started", {
    peer_id = self:peer_id().id,
    connections = #self._connections,
    listeners = #self._listeners,
    tasks = (function()
      local n = 0
      for _ in pairs(self._tasks) do
        n = n + 1
      end
      return n
    end)(),
  })
  self._running = false
  if self._runtime_impl and self._runtime_impl.stop then
    self._runtime_impl.stop(self)
  end
  local closing = host_connections.snapshot(self)
  for _, entry in ipairs(closing) do
    entry.conn:close()
    if self._connections_by_id[entry.id] then
      local ok, unregister_err = self:_unregister_connection(nil, entry, error_mod.new("closed", "host closed"))
      if not ok then
        return nil, unregister_err
      end
    end
  end
  host_connections.reset(self)
  for _, task in pairs(self._tasks) do
    self:_cleanup_task_waiters(task)
    if task.stream then
      self:_release_stream_resource(task.stream)
      task.stream = nil
    end
  end
  self._tasks = {}
  self._task_queue = {}
  self._task_queue_head = 1
  self._task_queue_tail = 0
  self._sleeping_tasks = {}
  self._task_read_waiters = {}
  self._task_dial_waiters = {}
  self._task_write_waiters = {}
  self._task_completion_waiters = {}

  for _, pending_entry in ipairs(self._pending_inbound) do
    local raw_conn = host_runtime_luv_native.pending_raw(pending_entry)
    if type(pending_entry) == "table" then
      self:_close_connection_resource(pending_entry.resource_scope)
    end
    if raw_conn and type(raw_conn.close) == "function" then
      raw_conn:close()
    end
  end
  self._pending_inbound = {}

  for _, pending in ipairs(self._pending_relay_inbound) do
    self:_close_connection_resource(pending.resource_scope)
    if pending.raw_conn and type(pending.raw_conn.close) == "function" then
      pending.raw_conn:close()
    end
  end
  self._pending_relay_inbound = {}

  self:_close_listeners()

  log.debug("host closed", {
    peer_id = self:peer_id().id,
  })

  return true
end

---@alias Libp2pTransportName 'tcp'
---@alias Libp2pHostEventHandler fun(event: table): any

---@class Libp2pHostConfig
---@field identity? Libp2pIdentityKeypair Local identity keypair. Generated when omitted.
---@field identity_type? Libp2pIdentityType Key type used when generating identity. Default: `ed25519`.
---@field listen_addrs? string[]
---@field announce_addrs? string[]
---@field no_announce_addrs? string[]
---@field observed_addrs? string[]
---@field relay_addrs? string[]
---@field advertise_observed? boolean
---@field transports? Libp2pTransportName[]
---@field security_transports? Libp2pSecurityTransportConfig
---@field muxers? Libp2pMuxerConfig
---@field services? Libp2pServicesConfig
---@field peer_discovery? Libp2pServicesConfig
---@field peerstore? Libp2pPeerstoreInstance
---@field peerstore_options? Libp2pPeerstoreConfig
---@field address_manager? Libp2pAddressManagerInstance
---@field resource_manager? Libp2pResourceManagerConfig|Libp2pResourceManagerInstance|false
---@field resource_manager_options? Libp2pResourceManagerConfig
---@field connection_manager? Libp2pConnectionManagerConfig|Libp2pConnectionManagerInstance|false Options map, prebuilt manager object, or false.
---@field runtime? 'auto'|'luv'
---@field blocking? boolean
---@field connect_timeout? number
---@field io_timeout? number
---@field accept_timeout? number
---@field max_iterations? integer
---@field poll_interval? number
---@field task_resume_budget? integer
---@field task_retention? number
---@field task_prune_interval? number
---@field debug_connection_events? boolean
---@field on? table<string, Libp2pHostEventHandler>
---@field on_started? fun(host: Libp2pHost)
---@field tcp? Libp2pTcpConfig
---@field identify? Libp2pIdentifyConfig
---@field autonat? Libp2pAutoNatConfig
---@field autorelay? Libp2pAutoRelayConfig
---@field kad_dht? Libp2pKadDhtConfig
---@field perf? Libp2pPerfConfig
---@field upnp_nat? Libp2pUpnpNatConfig

---@class Libp2pHost
---@field security_transports Libp2pSecurityTransportConfig
---@field muxers Libp2pMuxerConfig
---@field connection_manager table|nil
---@field services table<string, table>

---Construct a new host instance.
---@param config? Libp2pHostConfig Host configuration.
---@return Libp2pHost|nil host
---@return table|nil err
function M.new(config)
  return Host:new(config)
end

return M
