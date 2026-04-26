local address_manager = require("lua_libp2p.address_manager")
local bootstrap = require("lua_libp2p.bootstrap")
local keys = require("lua_libp2p.crypto.keys")
local discovery = require("lua_libp2p.discovery")
local discovery_bootstrap = require("lua_libp2p.discovery.bootstrap")
local dnsaddr = require("lua_libp2p.dnsaddr")
local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log")
local host_runtime_luv = require("lua_libp2p.host_runtime_luv")
local host_runtime_luv_native = require("lua_libp2p.host_runtime_luv_native")
local host_runtime_poll = require("lua_libp2p.host_runtime_poll")
local kad_dht = require("lua_libp2p.kad_dht")
local multiaddr = require("lua_libp2p.multiaddr")
local peerid = require("lua_libp2p.peerid")
local peerstore = require("lua_libp2p.peerstore")
local identify = require("lua_libp2p.protocol.identify")
local key_pb = require("lua_libp2p.crypto.key_pb")
local perf = require("lua_libp2p.protocol.perf")
local ping = require("lua_libp2p.protocol.ping")
local relay_autorelay = require("lua_libp2p.relay.autorelay")
local relay_proto = require("lua_libp2p.protocol.circuit_relay_v2")
local upgrader = require("lua_libp2p.network.upgrader")
local tcp_poll = require("lua_libp2p.transport.tcp")
local tcp_luv = require("lua_libp2p.transport.tcp_luv")

local M = {}

local DEFAULT_IDENTIFY_PROTOCOL_VERSION = "/lua-libp2p/0.1.0"
local DEFAULT_IDENTIFY_AGENT_VERSION = "lua-libp2p/0.1.0"
local DEFAULT_EVENT_QUEUE_MAX = 256
local DEFAULT_BOOTSTRAPPERS = bootstrap.DEFAULT_BOOTSTRAPPERS

local function list_copy(values)
  local out = {}
  for i, v in ipairs(values or {}) do
    out[i] = v
  end
  return out
end

local function list_equal(a, b)
  if #a ~= #b then
    return false
  end
  for i = 1, #a do
    if a[i] ~= b[i] then
      return false
    end
  end
  return true
end

local function emit_event(self, name, payload)
  local event = {
    name = name,
    payload = payload,
    ts = os.time(),
  }

  for _, sub in pairs(self._event_subscribers) do
    if sub.event_name == nil or sub.event_name == name then
      local queue = sub.queue
      queue[#queue + 1] = event
      local max_size = sub.max_queue or DEFAULT_EVENT_QUEUE_MAX
      while #queue > max_size do
        table.remove(queue, 1)
      end
    end
  end

  local handlers = self._event_handlers[name]
  if type(handlers) == "table" then
    for _, handler in ipairs(handlers) do
      local ok, err = pcall(handler, payload, event)
      if not ok then
        return nil, error_mod.new("protocol", "host event handler panicked", {
          event = name,
          cause = err,
        })
      end
    end
  end

  return true
end

local function bind_listeners(self, addrs)
  local targets = addrs and list_copy(addrs) or self.listen_addrs
  if #targets == 0 then
    targets = { "/ip4/127.0.0.1/tcp/0" }
  end

  local next_listeners = {}
  local next_addrs = {}

  local function close_all(listeners)
    for _, listener in ipairs(listeners) do
      if listener and type(listener.close) == "function" then
        listener:close()
      end
    end
  end

  for _, addr in ipairs(targets) do
    if addr == "/p2p-circuit" then
      goto continue_target
    end
    local listener, listen_err = self._tcp_transport.listen({
      multiaddr = addr,
      accept_timeout = self._accept_timeout,
      io_timeout = self._io_timeout,
    })
    if not listener then
      close_all(next_listeners)
      return nil, listen_err
    end
    next_listeners[#next_listeners + 1] = listener

    local resolved, resolved_err = listener:multiaddr()
    if not resolved then
      close_all(next_listeners)
      return nil, resolved_err
    end
    next_addrs[#next_addrs + 1] = resolved
    ::continue_target::
  end

  close_all(self._listeners)
  self._listeners = {}
  for i, listener in ipairs(next_listeners) do
    self._listeners[i] = listener
  end

  self.listen_addrs = {}
  for i, resolved in ipairs(next_addrs) do
    self.listen_addrs[i] = resolved
  end
  if self.address_manager then
    self.address_manager:set_listen_addrs(self.listen_addrs)
  end

  if self._running and self._runtime_impl and self._runtime_impl.sync_watchers then
    local ok, sync_err = self._runtime_impl.sync_watchers(self)
    if not ok then
      return nil, sync_err
    end
  end

  return true
end

local Host = {}
Host.__index = Host

local function sleep_seconds(seconds)
  local ok_socket, socket = pcall(require, "socket")
  if ok_socket and type(socket.sleep) == "function" then
    socket.sleep(seconds)
  end
end

local function is_nonfatal_stream_error(err)
  if not (err and error_mod.is_error(err)) then
    return false
  end
  return err.kind == "timeout"
    or err.kind == "closed"
    or err.kind == "decode"
    or err.kind == "protocol"
    or err.kind == "unsupported"
end

local function identify_listen_addrs(message)
  local out = {}
  for _, addr in ipairs((message and message.listenAddrs) or {}) do
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

local function list_contains(values, needle)
  for _, value in ipairs(values or {}) do
    if value == needle then
      return true
    end
  end
  return false
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
  return host:_poll_once_luv(timeout)
end

local function runtime_luv_sync_watchers(host)
  return host_runtime_luv.sync_watchers(host)
end

local RUNTIME_IMPLS = {
  poll = {
    start = host_runtime_poll.start,
    stop = host_runtime_poll.stop,
    poll_once = host_runtime_poll.poll_once,
  },
  luv = {
    start = runtime_luv_start,
    stop = runtime_luv_stop,
    poll_once = runtime_luv_poll_once,
    sync_watchers = runtime_luv_sync_watchers,
  },
}

local function default_runtime_name()
  local ok_luv = pcall(require, "luv")
  if ok_luv then
    return "luv"
  end
  return "poll"
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

local function has_terminal_peer_id(addr)
  local parsed = multiaddr.parse(addr)
  if not parsed or type(parsed.components) ~= "table" or #parsed.components == 0 then
    return false
  end
  local last = parsed.components[#parsed.components]
  return last.protocol == "p2p" and type(last.value) == "string" and last.value ~= ""
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

local function first_dialable_tcp_addr(addrs)
  for _, addr in ipairs(addrs or {}) do
    if is_dialable_tcp_addr(addr) then
      return addr
    end
  end
  return nil
end

local function first_relay_addr(addrs)
  for _, addr in ipairs(addrs or {}) do
    if multiaddr.is_relay_addr(addr) then
      return addr
    end
  end
  return nil
end

local function contains_circuit_listen_addr(addrs)
  for _, addr in ipairs(addrs or {}) do
    if addr == "/p2p-circuit" then
      return true
    end
  end
  return false
end

local function list_has(values, needle)
  for _, value in ipairs(values or {}) do
    if value == needle then
      return true
    end
  end
  return false
end

local function build_peer_discovery(config)
  if config == nil or config == false then
    return nil
  end
  if type(config) == "table" and type(config.discover) == "function" then
    return config
  end
  if type(config) ~= "table" then
    return nil, nil, error_mod.new("input", "peer_discovery must be a config table or discovery object")
  end

  local sources = {}
  local bootstrap_config = nil
  if type(config.sources) == "table" then
    for _, source in ipairs(config.sources) do
      sources[#sources + 1] = source
    end
  end
  if config.bootstrap ~= nil then
    local bootstrap_opts = config.bootstrap
    if type(bootstrap_opts) == "table" and bootstrap_opts[1] ~= nil and bootstrap_opts.list == nil then
      bootstrap_opts = { list = bootstrap_opts }
    end
    bootstrap_opts = bootstrap_opts or {}
    if bootstrap_opts.list == nil then
      bootstrap_opts.list = DEFAULT_BOOTSTRAPPERS
    end
    if bootstrap_opts.dnsaddr_resolver == nil then
      bootstrap_opts.dnsaddr_resolver = dnsaddr.default_resolver
    end
    bootstrap_config = bootstrap_opts
    local source, source_err = discovery_bootstrap.new(bootstrap_opts)
    if not source then
      return nil, nil, source_err
    end
    sources[#sources + 1] = source
  end
  if #sources == 0 then
    return nil, nil, error_mod.new("input", "peer_discovery config must include sources or bootstrap")
  end
  local discoverer, discoverer_err = discovery.new({ sources = sources })
  if not discoverer then
    return nil, nil, discoverer_err
  end
  return discoverer, bootstrap_config
end

local function resolve_target(target)
  if type(target) == "string" then
    if target:sub(1, 1) == "/" then
      return { addr = target }
    end
    return { peer_id = target }
  end

  if type(target) == "table" then
    local addr = target.addr
    if not addr and type(target.addrs) == "table" then
      addr = first_dialable_tcp_addr(target.addrs) or first_relay_addr(target.addrs)
    end
    return {
      peer_id = target.peer_id,
      addr = addr,
    }
  end

  return nil
end

function Host:new(config)
  local cfg = config or {}
  local runtime_name = cfg.runtime or cfg.runtime_backend or "auto"
  if runtime_name == "auto" then
    runtime_name = default_runtime_name()
  end
  local runtime_impl = RUNTIME_IMPLS[runtime_name]
  if runtime_impl == nil then
    return nil, error_mod.new("input", "unsupported host runtime", {
      runtime = runtime_name,
      supported = { "auto", "poll", "luv" },
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
    peer_discovery = nil,
    _bootstrap_discovery = nil,
    _bootstrap_discovery_due_at = nil,
    _bootstrap_discovery_done = false,
    _pending_bootstrap_dials = {},
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
    security_transports = list_copy(cfg.security_transports or {}),
    muxers = list_copy(cfg.muxers or {}),
    _handlers = {},
    _services = {},
    _handler_tasks = {},
    _listeners = {},
    _connections = {},
    _connections_by_peer = {},
    _connections_by_id = {},
    _next_connection_id = 1,
    _pending_inbound = {},
    _pending_relay_inbound = {},
    _event_handlers = {},
    _event_queue_max = cfg.event_queue_max or DEFAULT_EVENT_QUEUE_MAX,
    _last_advertised_addrs = nil,
    _event_subscribers = {},
    _next_subscriber_id = 1,
    _identify_on_connect_handler = nil,
    _identify_inflight = {},
    _runtime = runtime_name,
    _runtime_impl = runtime_impl,
    _tcp_transport = runtime_name == "luv" and tcp_luv or tcp_poll,
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
    _service_options = {
      identify = cfg.identify or {},
      autorelay = cfg.autorelay or {},
      kad_dht = cfg.kad_dht or {},
      perf = cfg.perf or {},
    },
  }, self)

  local peer_discovery, bootstrap_config, peer_discovery_err = build_peer_discovery(cfg.peer_discovery)
  if peer_discovery_err then
    return nil, peer_discovery_err
  end
  self_obj.peer_discovery = peer_discovery
  if bootstrap_config then
    self_obj._bootstrap_discovery = {
      config = bootstrap_config,
      dial_on_start = bootstrap_config.dial_on_start ~= false,
      timeout = bootstrap_config.timeout or bootstrap_config.delay or 1,
      tag_name = bootstrap_config.tag_name or "bootstrap",
      tag_value = bootstrap_config.tag_value or 50,
      tag_ttl = bootstrap_config.tag_ttl == nil and 120 or bootstrap_config.tag_ttl,
    }
  end

  if #self_obj.security_transports == 0 then
    self_obj.security_transports = { "/noise" }
  end
  if #self_obj.muxers == 0 then
    self_obj.muxers = { "/yamux/1.0.0" }
  end

  if type(cfg.on) == "table" then
    for event_name, handler in pairs(cfg.on) do
      local ok, on_err = self_obj:on(event_name, handler)
      if not ok then
        return nil, on_err
      end
    end
  end

  local services = cfg.services or cfg.service
  if contains_circuit_listen_addr(self_obj.listen_addrs) and not list_has(services or {}, "autorelay") then
    return nil, error_mod.new("input", "/p2p-circuit listen addr requires autorelay service")
  end
  if services ~= nil then
    if type(services) ~= "table" then
      return nil, error_mod.new("input", "services must be a list")
    end
    for _, service_name in ipairs(services) do
      local ok, err = self_obj:add_service(service_name)
      if not ok then
        return nil, err
      end
    end
  end

  return self_obj
end

function Host:is_running()
  return self._running
end

function Host:handle(protocol_id, handler)
  if type(protocol_id) ~= "string" or protocol_id == "" then
    return nil, error_mod.new("input", "protocol id must be non-empty")
  end
  if type(handler) ~= "function" then
    return nil, error_mod.new("input", "handler must be a function")
  end
  self._handlers[protocol_id] = handler
  return true
end

function Host:_spawn_handler_task(handler, ctx)
  local task = {
    protocol = ctx.protocol,
    peer_id = ctx.peer_id,
    connection = ctx.connection,
    state = ctx.state,
  }
  task.co = coroutine.create(function()
    return handler(ctx.stream, ctx)
  end)
  self._handler_tasks[#self._handler_tasks + 1] = task
  return task
end

function Host:_run_handler_tasks()
  for i = #self._handler_tasks, 1, -1 do
    local task = self._handler_tasks[i]
    local ok, result, err = coroutine.resume(task.co)
    if not ok then
      if task.protocol == "identify_connect" then
        local identify_err = error_mod.new("protocol", "identify on connect panicked", {
          peer_id = task.peer_id,
          cause = result,
          traceback = debug.traceback(task.co, tostring(result)),
        })
        if task.peer_id then
          self._identify_inflight[task.peer_id] = nil
        end
        table.remove(self._handler_tasks, i)
        log.warn("identify on connect failed", {
          subsystem = "identify",
          peer_id = task.peer_id,
          cause = tostring(identify_err),
          panic = tostring(result),
          queue_size = map_count(self._identify_inflight),
        })
        local emit_ok, emit_err = emit_event(self, "peer_identify_failed", {
          peer_id = task.peer_id,
          cause = identify_err,
        })
        if not emit_ok then
          return nil, emit_err
        end
        goto continue_tasks
      end
      return nil, error_mod.new("protocol", "handler task panicked", {
        protocol = task.protocol,
        cause = result,
        traceback = debug.traceback(task.co, tostring(result)),
      })
    end

    if coroutine.status(task.co) == "dead" then
      table.remove(self._handler_tasks, i)
      if result == nil and err then
        if is_nonfatal_stream_error(err) then
          if task.connection and type(task.connection.close) == "function" then
            task.connection:close()
          end
          goto continue_tasks
        end
        return nil, err
      end
    end

    ::continue_tasks::
  end

  return true
end

local function list_protocol_handlers(handlers)
  local out = {}
  for protocol_id in pairs(handlers) do
    out[#out + 1] = protocol_id
  end
  table.sort(out)
  return out
end

local function map_count(values)
  local n = 0
  for _ in pairs(values or {}) do
    n = n + 1
  end
  return n
end

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
    protocols = list_protocol_handlers(self._handlers),
  }

  local wrote, write_err = identify.write(stream, msg)
  if not wrote then
    return nil, write_err
  end

  if type(stream.close_write) == "function" then
    stream:close_write()
  end

  return true
end

function Host:_request_identify(peer_id, opts)
  if type(peer_id) ~= "string" or peer_id == "" then
    return nil, error_mod.new("input", "identify request requires peer id")
  end

  local stream, selected, conn, state_or_err = self:new_stream(peer_id, { identify.ID }, opts)
  if not stream then
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
      local ok, emit_err = emit_event(self, "peer_protocols_updated", {
        peer_id = peer_id,
        protocols = after_protocols,
        added_protocols = added_protocols,
        source = "identify",
      })
      if not ok then
        return nil, emit_err
      end
    end
  end

  if self.address_manager and msg.observedAddr then
    local observed = identify_listen_addrs({ listenAddrs = { msg.observedAddr } })[1]
    if observed then
      self.address_manager:add_observed_addr(observed)
      local ok, emit_err = emit_event(self, "observed_addr", {
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
    queue_size = map_count(self._identify_inflight),
  })

  self:_spawn_handler_task(function(_, ctx)
    local pid = ctx and ctx.peer_id
    local call_ok, result, identify_err = pcall(function()
      return self:_request_identify(pid)
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
        queue_size = map_count(self._identify_inflight),
      })
      local ok, emit_err = emit_event(self, "peer_identify_failed", {
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
      queue_size = map_count(self._identify_inflight),
    })

    local ok, emit_err = emit_event(self, "peer_identified", {
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

function Host:add_service(name)
  if type(name) ~= "string" or name == "" then
    return nil, error_mod.new("input", "service name must be non-empty")
  end
  if self._services[name] then
    return true
  end

  if name == "identify" then
    local ok, err = self:handle(identify.ID, function(stream, ctx)
      return self:_handle_identify(stream, ctx)
    end)
    if not ok then
      return nil, err
    end

    local identify_opts = self._service_options.identify or {}

    local identify_hook_ok, identify_hook_err = identify.enable_run_on_connection_open(self, identify_opts)
    if not identify_hook_ok then
      return nil, identify_hook_err
    end

    if identify_opts.include_push ~= false then
      ok, err = self:handle(identify.PUSH_ID, function(stream, ctx)
        return self:_handle_identify(stream, ctx)
      end)
      if not ok then
        return nil, err
      end
    end

    self._services[name] = true
    return true
  end

  if name == "ping" then
    local ok, err = self:handle(ping.ID, function(stream)
      return ping.handle(stream)
    end)
    if not ok then
      return nil, err
    end
    self._services[name] = true
    return true
  end

  if name == "perf" then
    local perf_opts = self._service_options.perf or {}
    local ok, err = self:handle(perf.ID, function(stream)
      return perf.handle(stream, {
        write_block_size = perf_opts.write_block_size,
        yield_every_bytes = perf_opts.yield_every_bytes,
      })
    end)
    if not ok then
      return nil, err
    end
    self._services[name] = true
    return true
  end

  if name == "autorelay" then
    local autorelay_opts = self._service_options.autorelay or {}
    local svc, svc_err = relay_autorelay.new(self, autorelay_opts)
    if not svc then
      return nil, svc_err
    end
    local started, start_err = svc:start()
    if not started then
      return nil, start_err
    end
    self.autorelay = svc
    self._services[name] = true
    return true
  end

  if name == "kad_dht" then
    local dht_opts = self._service_options.kad_dht or {}
    if dht_opts.mode == nil then
      dht_opts.mode = "client"
    end
    if dht_opts.peer_discovery == nil then
      dht_opts.peer_discovery = self.peer_discovery
    end
    local dht, dht_err = kad_dht.new(self, dht_opts)
    if not dht then
      return nil, dht_err
    end
    local started, start_err = dht:start()
    if not started then
      return nil, start_err
    end
    self.kad_dht = dht
    self._services[name] = true
    return true
  end

  return nil, error_mod.new("input", "unsupported service", { service = name })
end

function Host:_handle_relay_stop(stream, ctx)
  local accepted, accept_err = relay_proto.accept_stop(stream, {
    max_message_size = self._max_relay_message_size,
  })
  if not accepted then
    return nil, accept_err
  end
  self._pending_relay_inbound[#self._pending_relay_inbound + 1] = {
    raw_conn = stream,
    relay = {
      relay_peer_id = ctx and ctx.state and ctx.state.remote_peer_id or nil,
      initiator_peer_id_bytes = accepted.initiator_peer_id_bytes,
      initiator_addrs = accepted.initiator_addrs,
      limit = accepted.limit,
      limit_kind = accepted.limit_kind,
      direction = "inbound",
    },
  }
  return true
end

function Host:_build_router()
  local router = require("lua_libp2p.protocol.mss").new_router()
  for protocol_id, handler in pairs(self._handlers) do
    local ok, err = router:register(protocol_id, handler)
    if not ok then
      return nil, err
    end
  end
  return router
end

function Host:get_listen_addrs()
  if self.address_manager then
    self.address_manager:set_listen_addrs(self.listen_addrs)
  end
  return list_copy(self.listen_addrs)
end

function Host:peer_id()
  return self._peer_id
end

function Host:get_multiaddrs_raw()
  if self.address_manager then
    self.address_manager:set_listen_addrs(self.listen_addrs)
    return self.address_manager:get_advertise_addrs()
  end
  return list_copy(self.listen_addrs)
end

function Host:_emit_self_peer_update_if_changed()
  local addrs = self:get_multiaddrs_raw()
  local protocols = list_protocol_handlers(self._handlers)
  if self._last_advertised_addrs and list_equal(self._last_advertised_addrs, addrs) then
    return true
  end
  self._last_advertised_addrs = list_copy(addrs)
  log.info("self peer addresses updated", {
    subsystem = "host",
    peer_id = self:peer_id().id,
    addrs = #addrs,
    multiaddrs = table.concat(addrs, ","),
    protocols = table.concat(protocols, ","),
  })
  return emit_event(self, "self_peer_update", {
    peer_id = self:peer_id().id,
    addrs = list_copy(addrs),
    protocols = protocols,
  })
end

function Host:get_multiaddrs()
  local out = {}
  local pid = self._peer_id and self._peer_id.id or nil
  for _, addr in ipairs(self:get_multiaddrs_raw()) do
    if has_terminal_peer_id(addr) then
      out[#out + 1] = addr
    elseif pid then
      out[#out + 1] = addr .. "/p2p/" .. pid
    else
      out[#out + 1] = addr
    end
  end
  return out
end

function Host:_register_connection(conn, state)
  local connection_id = self._next_connection_id
  self._next_connection_id = self._next_connection_id + 1
  local entry = {
    id = connection_id,
    conn = conn,
    state = state or {},
    opened_at = os.time(),
  }
  entry.state.connection_id = connection_id
  self._connections[#self._connections + 1] = entry
  self._connections_by_id[connection_id] = entry

  local peer_id = entry.state.remote_peer_id
  if peer_id then
    self._connections_by_peer[peer_id] = self._connections_by_peer[peer_id] or {}
    self._connections_by_peer[peer_id][connection_id] = entry
  end

  local ok, emit_err = emit_event(self, "peer_connected", {
    connection = entry.conn,
    connection_id = connection_id,
    state = entry.state,
    peer_id = peer_id,
  })
  if not ok then
    return nil, emit_err
  end

  local opened_ok, opened_err = emit_event(self, "connection_opened", {
    connection = entry.conn,
    connection_id = connection_id,
    state = entry.state,
    peer_id = peer_id,
  })
  if not opened_ok then
    return nil, opened_err
  end

  if self._running and self._runtime_impl and self._runtime_impl.sync_watchers then
    local sync_ok, sync_err = self._runtime_impl.sync_watchers(self)
    if not sync_ok then
      return nil, sync_err
    end
  end

  return entry
end

function Host:_unregister_connection(index, entry, cause)
  if not entry then
    return false
  end
  local actual_index = index
  if self._connections[actual_index] ~= entry then
    actual_index = nil
    for i = #self._connections, 1, -1 do
      if self._connections[i] == entry then
        actual_index = i
        break
      end
    end
  end
  if actual_index then
    table.remove(self._connections, actual_index)
  end
  if entry.id ~= nil then
    self._connections_by_id[entry.id] = nil
  end
  local peer_id = entry.state and entry.state.remote_peer_id
  if peer_id and self._connections_by_peer[peer_id] then
    if entry.id ~= nil then
      self._connections_by_peer[peer_id][entry.id] = nil
    end
    if next(self._connections_by_peer[peer_id]) == nil then
      self._connections_by_peer[peer_id] = nil
    end
  end
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

function Host:on(event_name, handler)
  if type(event_name) ~= "string" or event_name == "" then
    return nil, error_mod.new("input", "event name must be non-empty")
  end
  if type(handler) ~= "function" then
    return nil, error_mod.new("input", "event handler must be a function")
  end

  local handlers = self._event_handlers[event_name]
  if not handlers then
    handlers = {}
    self._event_handlers[event_name] = handlers
  end
  handlers[#handlers + 1] = handler
  return true
end

function Host:emit(event_name, payload)
  if type(event_name) ~= "string" or event_name == "" then
    return nil, error_mod.new("input", "event name must be non-empty")
  end
  return emit_event(self, event_name, payload)
end

function Host:off(event_name, handler)
  local handlers = self._event_handlers[event_name]
  if type(handlers) ~= "table" then
    return false
  end
  for i = 1, #handlers do
    if handlers[i] == handler then
      table.remove(handlers, i)
      return true
    end
  end
  return false
end

function Host:on_protocol(protocol_id, handler)
  if type(protocol_id) ~= "string" or protocol_id == "" then
    return nil, error_mod.new("input", "protocol id must be non-empty")
  end
  if type(handler) ~= "function" then
    return nil, error_mod.new("input", "protocol handler must be a function")
  end

  local function wrapped(payload, event)
    if list_contains(payload and payload.protocols, protocol_id) or list_contains(payload and payload.added_protocols, protocol_id) then
      return handler(payload.peer_id, payload, event)
    end
    return true
  end

  local ok, err = self:on("peer_protocols_updated", wrapped)
  if not ok then
    return nil, err
  end

  if self.peerstore and type(self.peerstore.all) == "function" then
    for _, peer in ipairs(self.peerstore:all()) do
      if list_contains(peer.protocols, protocol_id) then
        local call_ok, call_err = pcall(handler, peer.peer_id, {
          peer_id = peer.peer_id,
          protocols = peer.protocols,
          added_protocols = { protocol_id },
          source = "peerstore",
        }, nil)
        if not call_ok then
          return nil, error_mod.new("protocol", "protocol handler panicked", { cause = call_err })
        end
      end
    end
  end

  return wrapped
end

function Host:subscribe(event_name_or_opts, opts)
  local event_name = nil
  local options = opts or {}

  if type(event_name_or_opts) == "table" then
    options = event_name_or_opts
    event_name = options.event_name or options.event
  elseif event_name_or_opts ~= nil then
    event_name = event_name_or_opts
  end

  if event_name ~= nil and (type(event_name) ~= "string" or event_name == "") then
    return nil, error_mod.new("input", "event name must be non-empty")
  end

  local sub = {
    id = self._next_subscriber_id,
    event_name = event_name,
    queue = {},
    max_queue = options.max_queue or self._event_queue_max or DEFAULT_EVENT_QUEUE_MAX,
  }
  self._next_subscriber_id = self._next_subscriber_id + 1
  self._event_subscribers[sub.id] = sub
  return sub
end

function Host:unsubscribe(subscriber)
  local id = subscriber
  if type(subscriber) == "table" then
    id = subscriber.id
  end
  if type(id) ~= "number" then
    return false
  end
  if self._event_subscribers[id] == nil then
    return false
  end
  self._event_subscribers[id] = nil
  return true
end

function Host:next_event(subscriber)
  if type(subscriber) ~= "table" or type(subscriber.id) ~= "number" then
    return nil, error_mod.new("input", "subscriber handle is required")
  end
  local current = self._event_subscribers[subscriber.id]
  if current == nil then
    return nil, error_mod.new("state", "subscriber is not registered")
  end
  if #current.queue == 0 then
    return nil
  end
  return table.remove(current.queue, 1)
end

function Host:_set_runtime_error(runtime_name, err)
  self._runtime_last_error = err
  self._running = false
  local fields = flatten_error_fields(err, "cause", {
    subsystem = "host",
    runtime = runtime_name,
  })
  log.error("host runtime tick failed", fields)

  local ok, emit_err = emit_event(self, "host_runtime_error", {
    runtime = runtime_name,
    cause = err,
  })
  if not ok then
    log.error("host runtime error emit failed", {
      subsystem = "host",
      runtime = runtime_name,
      cause = tostring(emit_err),
    })
  end
end

function Host:_find_connection(peer_id)
  if not peer_id then
    return nil
  end
  local by_peer = self._connections_by_peer[peer_id]
  if not by_peer then
    return nil
  end
  for _, entry in pairs(by_peer) do
    return entry
  end
  return nil
end

function Host:_find_connection_by_id(connection_id)
  return self._connections_by_id[connection_id]
end

function Host:_dial_relay_raw(addr, destination_peer_id, opts)
  local info, info_err = multiaddr.relay_info(addr)
  if not info then
    return nil, nil, info_err
  end
  local target_peer_id = destination_peer_id or info.destination_peer_id
  if type(target_peer_id) ~= "string" or target_peer_id == "" then
    return nil, nil, error_mod.new("input", "relayed dial target must include destination peer id")
  end

  local stream, selected, relay_conn, relay_state_or_err = self:new_stream(info.relay_addr, { relay_proto.HOP_ID }, opts)
  if not stream then
    return nil, nil, relay_state_or_err
  end

  local connected, response_or_err = relay_proto.connect(stream, target_peer_id, opts)
  if not connected then
    if type(stream.close) == "function" then
      pcall(function()
        stream:close()
      end)
    end
    return nil, nil, response_or_err
  end
  local response = response_or_err or {}

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

function Host:dial(peer_or_addr, opts)
  local resolved = resolve_target(peer_or_addr)
  if not resolved then
    return nil, nil, error_mod.new("input", "dial target must be peer id, multiaddr, or target table")
  end

  if not resolved.peer_id and resolved.addr then
    resolved.peer_id = extract_peer_id_from_multiaddr(resolved.addr)
  end

  local existing = self:_find_connection(resolved.peer_id)
  if existing then
    return existing.conn, existing.state
  end

  local addr = resolved.addr
  if not addr then
    local addrs = self.peerstore and self.peerstore:get_addrs(resolved.peer_id) or {}
    addr = first_dialable_tcp_addr(addrs) or first_relay_addr(addrs)
  end
  if not addr then
    return nil, nil, error_mod.new("input", "dial target must include an address when no connection exists")
  end

  local raw_conn, dial_err, relay_state
  if multiaddr.is_relay_addr(addr) then
    raw_conn, relay_state, dial_err = self:_dial_relay_raw(addr, resolved.peer_id, opts)
  else
    local endpoint, endpoint_err = multiaddr.to_tcp_endpoint(addr)
    if not endpoint then
      return nil, nil, endpoint_err
    end

    raw_conn, dial_err = self._tcp_transport.dial({
      host = endpoint.host,
      port = endpoint.port,
    }, {
      timeout = (opts and opts.timeout) or self._connect_timeout,
      io_timeout = (opts and opts.io_timeout) or self._io_timeout,
    })
  end
  if not raw_conn then
    return nil, nil, dial_err
  end

  local expected_remote = resolved.peer_id or extract_peer_id_from_multiaddr(addr)

  local conn, state, up_err = upgrader.upgrade_outbound(raw_conn, {
    local_keypair = self.identity,
    expected_remote_peer_id = expected_remote,
    security_protocols = self.security_transports,
    muxer_protocols = self.muxers,
  })
  if not conn then
    if type(raw_conn.close) == "function" then
      raw_conn:close()
    end
    return nil, nil, up_err
  end
  if relay_state then
    relay_state.limit = relay_state.limit or nil
    relay_state.limit_kind = relay_proto.classify_limit(relay_state.limit)
    relay_state.direction = "outbound"
    state.relay = relay_state
  end

  local entry, register_err = self:_register_connection(conn, state)
  if not entry then
    conn:close()
    return nil, nil, register_err
  end
  return entry.conn, entry.state
end

function Host:new_stream(peer_or_addr, protocols, opts)
  local conn, state, dial_err = self:dial(peer_or_addr, opts)
  if not conn then
    return nil, nil, nil, dial_err
  end

  local stream, selected, stream_err = conn:new_stream(protocols)
  if not stream then
    return nil, nil, nil, stream_err
  end

  return stream, selected, conn, state
end

function Host:_poll_once_with_ready_map(timeout, ready_map)

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
      state.relay = pending.relay
      local entry, register_err = self:_register_connection(conn, state)
      if not entry then
        conn:close()
        return nil, register_err
      end
    elseif up_err and error_mod.is_error(up_err) and up_err.kind == "timeout" then
      -- keep pending; retry on next poll tick
    elseif is_nonfatal_stream_error(up_err) then
      remove_pending_relay_inbound(i, pending)
      if raw_conn and type(raw_conn.close) == "function" then
        raw_conn:close()
      end
    else
      remove_pending_relay_inbound(i, pending)
      if raw_conn and type(raw_conn.close) == "function" then
        raw_conn:close()
      end
      return nil, up_err
    end
    ::continue_pending_relay_inbound::
  end

  for i = #self._pending_inbound, 1, -1 do
    local pending_entry = self._pending_inbound[i]
    local raw_conn = host_runtime_luv_native.pending_raw(pending_entry)

    if host_runtime_luv_native.is_native_host(self) then
      local status, _, resume_err, normalized_entry = host_runtime_luv_native.resume_inbound_upgrade(
        self,
        pending_entry,
        is_nonfatal_stream_error
      )
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
      raw_conn:close()
    else
      if raw_conn and type(raw_conn.rollback_read_tx) == "function" then
        raw_conn:rollback_read_tx()
      end
      table.remove(self._pending_inbound, i)
      raw_conn:close()
      return nil, up_err
    end

    ::continue_pending_inbound::
  end

  for _, listener in ipairs(self._listeners) do
    local should_accept = true
    if ready_map then
      should_accept = ready_map[listener] == true
    end

    local raw_conn, accept_err
    if should_accept then
      local accept_timeout = timeout or self._accept_timeout
      if ready_map then
        accept_timeout = 0
      end
      raw_conn, accept_err = listener:accept(accept_timeout)
    end
    if raw_conn then
      if host_runtime_luv_native.is_native_host(self) then
        self._pending_inbound[#self._pending_inbound + 1] = { raw_conn = raw_conn }
        goto continue_listeners
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
        if up_err and error_mod.is_error(up_err)
          and up_err.kind == "timeout"
          and self._runtime == "luv"
          and self._tcp_transport
          and self._tcp_transport.BACKEND == "luv-native"
        then
          if raw_conn and type(raw_conn.rollback_read_tx) == "function" then
            raw_conn:rollback_read_tx()
          end
          self._pending_inbound[#self._pending_inbound + 1] = raw_conn
          goto continue_listeners
        end

        if raw_conn and type(raw_conn.rollback_read_tx) == "function" then
          raw_conn:rollback_read_tx()
        end
        raw_conn:close()
        if is_nonfatal_stream_error(up_err) then
          goto continue_listeners
        end
        return nil, up_err
      end
      if raw_conn and type(raw_conn.commit_read_tx) == "function" then
        raw_conn:commit_read_tx()
      end
      local entry, register_err = self:_register_connection(conn, state)
      if not entry then
        conn:close()
        return nil, register_err
      end
      break
    elseif accept_err and error_mod.is_error(accept_err) and accept_err.kind ~= "timeout" then
      return nil, accept_err
    end

    ::continue_listeners::
  end

  local router, router_err = self:_build_router()
  if not router then
    return nil, router_err
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
        local ok, native_err = host_runtime_luv_native.process_connection(
          self,
          entry,
          router,
          is_nonfatal_stream_error
        )
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

    local stream, protocol_id, handler, stream_err = conn:accept_stream(router)
    if stream_err then
      if is_nonfatal_stream_error(stream_err) then
        goto continue_connections
      end
      return nil, stream_err
    end
    if stream and handler then
      self:_spawn_handler_task(handler, {
        stream = stream,
        host = self,
        connection = conn,
        state = entry.state,
        protocol = protocol_id,
      })
    end

    ::continue_connections::
  end

  local ok, task_err = self:_run_handler_tasks()
  if not ok then
    return nil, task_err
  end

  return true
end

function Host:_poll_once_poll(timeout)
  local ready_map, ready_err = host_runtime_poll.build_ready_map(self, timeout)
  if not ready_map and ready_err then
    return nil, ready_err
  end
  return self:_poll_once_with_ready_map(timeout, ready_map)
end

function Host:_poll_once_luv(timeout)
  local ok_luv, uv = pcall(require, "luv")
  if ok_luv then
    local ok, err = pcall(function()
      return uv.run("nowait")
    end)
    if not ok and string.find(tostring(err or ""), "loop already running", 1, true) == nil then
      return nil, error_mod.new("io", "luv poll failed", { cause = err })
    end
  end
  local ready_map = self._luv_ready
  self._luv_ready = {}
  return self:_poll_once_with_ready_map(timeout, ready_map)
end

function Host:_run_bootstrap_discovery_if_due()
  local cfg = self._bootstrap_discovery
  if not cfg or self._bootstrap_discovery_done or cfg.dial_on_start == false then
    return true
  end
  if self._bootstrap_discovery_due_at and os.time() < self._bootstrap_discovery_due_at then
    return true
  end
  self._bootstrap_discovery_done = true
  if not self.peer_discovery then
    return true
  end

  local peers, discover_err = self.peer_discovery:discover({
    dialable_only = true,
    ignore_resolve_errors = true,
    ignore_source_errors = true,
  })
  if not peers then
    return nil, discover_err
  end
  for _, peer in ipairs(peers) do
    if type(peer) == "table" and type(peer.peer_id) == "string" and peer.peer_id ~= "" then
      if self.peerstore then
        self.peerstore:merge(peer.peer_id, { addrs = peer.addrs or {} })
        self.peerstore:tag(peer.peer_id, cfg.tag_name, {
          value = cfg.tag_value,
          ttl = cfg.tag_ttl,
        })
      end
      self._pending_bootstrap_dials[#self._pending_bootstrap_dials + 1] = {
        peer_id = peer.peer_id,
        addrs = peer.addrs,
      }
    end
  end
  return true
end

function Host:_run_pending_bootstrap_dials(max_dials)
  local limit = max_dials or 1
  for _ = 1, limit do
    local target = table.remove(self._pending_bootstrap_dials, 1)
    if not target then
      return true
    end
    local conn = self:_find_connection(target.peer_id)
    if not conn then
      local ok = pcall(function()
        self:dial(target, {
          timeout = self._connect_timeout,
          io_timeout = self._io_timeout,
        })
      end)
      if not ok then
        -- Bootstrap dials are opportunistic; DHT bootstrap can still query seeds explicitly.
      end
    end
  end
  return true
end

function Host:poll_once(timeout)
  local self_update_ok, self_update_err = self:_emit_self_peer_update_if_changed()
  if not self_update_ok then
    return nil, self_update_err
  end
  if self.autorelay and type(self.autorelay.tick) == "function" then
    local relay_ok, relay_err = self.autorelay:tick()
    if not relay_ok then
      return nil, relay_err
    end
  end
  local boot_ok, boot_err = self:_run_bootstrap_discovery_if_due()
  if not boot_ok then
    return nil, boot_err
  end
  self:_run_pending_bootstrap_dials(1)
  if self._runtime_impl and self._runtime_impl.poll_once then
    return self._runtime_impl.poll_once(self, timeout)
  end
  return self:_poll_once_poll(timeout)
end

function Host:start()
  if #self._listeners == 0 then
    local ok, bind_err = bind_listeners(self)
    if not ok then
      return nil, bind_err
    end
    if not self._listeners[1] then
      return nil, error_mod.new("state", "no listeners bound")
    end
  end

  if not self._running then
    self._running = true
  end
  if self._bootstrap_discovery and self._bootstrap_discovery.dial_on_start ~= false then
    self._bootstrap_discovery_due_at = os.time() + (self._bootstrap_discovery.timeout or 1)
  end

  if type(self._on_started) == "function" then
    self._on_started(self)
  end

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
      local ok, err = self:poll_once(self._accept_timeout)
      if not ok then
        self._running = false
        return nil, err
      end
      iterations = iterations + 1
      if max_iterations and iterations >= max_iterations then
        break
      end
      if poll_interval > 0 and #self._handler_tasks == 0 then
        sleep_seconds(poll_interval)
      end
    end
  end

  return true
end

function Host:stop()
  self._running = false
  return self:close()
end

function Host:close()
  self._running = false
  if self._runtime_impl and self._runtime_impl.stop then
    self._runtime_impl.stop(self)
  end
  local closing = {}
  for i, entry in ipairs(self._connections) do
    closing[i] = entry
  end
  for _, entry in ipairs(closing) do
    entry.conn:close()
    if self._connections_by_id[entry.id] then
      local ok, unregister_err = self:_unregister_connection(nil, entry, error_mod.new("closed", "host closed"))
      if not ok then
        return nil, unregister_err
      end
    end
  end
  self._connections = {}
  self._connections_by_peer = {}
  self._connections_by_id = {}
  self._handler_tasks = {}

  for _, raw_conn in ipairs(self._pending_inbound) do
    raw_conn:close()
  end
  self._pending_inbound = {}

  for _, listener in ipairs(self._listeners) do
    listener:close()
  end
  self._listeners = {}

  return true
end

function M.new(config)
  return Host:new(config)
end

return M
