--- Host construction and runtime orchestration.
-- Hosts manage listeners, connections, protocol handlers, services, and
-- cooperative background tasks.
-- @module lua_libp2p.host
local address_manager = require("lua_libp2p.address_manager")
local connection_manager = require("lua_libp2p.connection_manager")
local keys = require("lua_libp2p.crypto.keys")
local discovery = require("lua_libp2p.discovery")
local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log")
local host_runtime_luv = require("lua_libp2p.host_runtime_luv")
local host_runtime_luv_native = require("lua_libp2p.host_runtime_luv_native")
local multiaddr = require("lua_libp2p.multiaddr")
local peerstore = require("lua_libp2p.peerstore")
local identify = require("lua_libp2p.protocol_identify.protocol")
local key_pb = require("lua_libp2p.crypto.key_pb")
local relay_proto = require("lua_libp2p.transport_circuit_relay_v2.protocol")
local upgrader = require("lua_libp2p.network.upgrader")
local tcp_luv = require("lua_libp2p.transport_tcp.luv")

local M = {}

local DEFAULT_IDENTIFY_PROTOCOL_VERSION = "/lua-libp2p/0.1.0"
local DEFAULT_IDENTIFY_AGENT_VERSION = "lua-libp2p/0.1.0"
local DEFAULT_EVENT_QUEUE_MAX = 256
local DEFAULT_TASK_RESUME_BUDGET = 128
local function now_seconds()
  local ok_socket, socket = pcall(require, "socket")
  if ok_socket and type(socket.gettime) == "function" then
    return socket.gettime()
  end
  return os.time()
end

local function list_copy(values)
  local out = {}
  for i, v in ipairs(values or {}) do
    out[i] = v
  end
  return out
end

local function pack_returns(...)
  return { n = select("#", ...), ... }
end

local function unpack_returns(values)
  if type(values) ~= "table" then
    return nil
  end
  return table.unpack(values, 1, values.n or #values)
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

local function make_task_context(task)
  local ctx = {}
  function ctx:id()
    return task.id
  end
  function ctx:name()
    return task.name
  end
  function ctx:cancelled()
    return task.cancelled == true
  end
  function ctx:checkpoint()
    if task.cancelled then
      return nil, error_mod.new("cancelled", "task cancelled", { task_id = task.id, name = task.name })
    end
    return coroutine.yield({ type = "checkpoint" })
  end
  function ctx:sleep(seconds)
    if type(seconds) ~= "number" or seconds < 0 then
      return nil, error_mod.new("input", "sleep seconds must be a non-negative number")
    end
    if task.cancelled then
      return nil, error_mod.new("cancelled", "task cancelled", { task_id = task.id, name = task.name })
    end
    return coroutine.yield({ type = "sleep", until_at = now_seconds() + seconds })
  end
  function ctx:wait_read(connection)
    if not connection then
      return nil, error_mod.new("input", "wait_read requires connection")
    end
    if task.cancelled then
      return nil, error_mod.new("cancelled", "task cancelled", { task_id = task.id, name = task.name })
    end
    return coroutine.yield({ type = "read", connection = connection })
  end
  function ctx:wait_dial(connection)
    if not connection then
      return nil, error_mod.new("input", "wait_dial requires connection")
    end
    if task.cancelled then
      return nil, error_mod.new("cancelled", "task cancelled", { task_id = task.id, name = task.name })
    end
    return coroutine.yield({ type = "dial", connection = connection })
  end
  function ctx:wait_write(connection)
    if not connection then
      return nil, error_mod.new("input", "wait_write requires connection")
    end
    if task.cancelled then
      return nil, error_mod.new("cancelled", "task cancelled", { task_id = task.id, name = task.name })
    end
    return coroutine.yield({ type = "write", connection = connection })
  end
  function ctx:await_task(child_task)
    if type(child_task) ~= "table" then
      return nil, error_mod.new("input", "await_task requires task table")
    end
    if task.cancelled then
      return nil, error_mod.new("cancelled", "task cancelled", { task_id = task.id, name = task.name })
    end
    if child_task.status ~= "completed" and child_task.status ~= "failed" and child_task.status ~= "cancelled" then
      local ok, err = coroutine.yield({ type = "task_wait", tasks = { child_task } })
      if ok == nil and err then
        return nil, err
      end
    end
    if child_task.status ~= "completed" then
      return nil, child_task.error or error_mod.new("state", "task did not complete", {
        task_id = child_task.id,
        name = child_task.name,
        status = child_task.status,
      })
    end
    if child_task.results then
      return unpack_returns(child_task.results)
    end
    return child_task.result
  end
  function ctx:await_any_task(child_tasks)
    if type(child_tasks) ~= "table" then
      return nil, error_mod.new("input", "await_any_task requires a task list")
    end
    if #child_tasks == 0 then
      return nil, error_mod.new("input", "await_any_task requires at least one task")
    end
    if task.cancelled then
      return nil, error_mod.new("cancelled", "task cancelled", { task_id = task.id, name = task.name })
    end
    for _, child_task in ipairs(child_tasks) do
      if type(child_task) == "table"
        and (child_task.status == "completed" or child_task.status == "failed" or child_task.status == "cancelled")
      then
        return true
      end
    end
    return coroutine.yield({ type = "task_wait", tasks = child_tasks })
  end
  return ctx
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
    or err.kind == "busy"
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

local function normalize_protocol_list(protocols)
  if type(protocols) == "string" then
    return { protocols }
  end
  return protocols
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

  local function append_source(source_name, source_spec)
    local source
    if type(source_spec) == "table" and type(source_spec.discover) == "function" then
      source = source_spec
    elseif type(source_spec) == "table" and source_spec.module ~= nil then
      local module = source_spec.module
      if type(module) ~= "table" or type(module.new) ~= "function" then
        return nil, error_mod.new("input", "peer_discovery module entry must expose .new(opts)", {
          source = source_name,
        })
      end
      local built_source, built_err = module.new(source_spec.config or {})
      if not built_source then
        return nil, built_err
      end
      source = built_source
    elseif type(source_spec) == "table" and type(source_spec.new) == "function" then
      local built_source, built_err = source_spec.new({})
      if not built_source then
        return nil, built_err
      end
      source = built_source
    else
      return nil, error_mod.new("input", "peer_discovery entries must be source objects or module specs", {
        source = source_name,
      })
    end

    if type(source) ~= "table" or type(source.discover) ~= "function" then
      return nil, error_mod.new("input", "peer_discovery source must provide discover(opts)", {
        source = source_name,
      })
    end
    if source_name == "bootstrap" and type(source._bootstrap_config) == "table" then
      bootstrap_config = source._bootstrap_config
    end
    sources[#sources + 1] = source
    return true
  end

  if type(config.sources) == "table" then
    for idx, source in ipairs(config.sources) do
      local ok, err = append_source("source_" .. tostring(idx), source)
      if not ok then
        return nil, nil, err
      end
    end
  else
    for source_name, source_spec in pairs(config) do
      if source_name ~= "sources" then
        local ok, err = append_source(source_name, source_spec)
        if not ok then
          return nil, nil, err
        end
      end
    end
  end

  if #sources == 0 then
    return nil, nil, error_mod.new("input", "peer_discovery config must include at least one source")
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
    return {
      peer_id = target.peer_id,
      addr = target.addr,
      addrs = target.addrs,
    }
  end

  return nil
end

local function normalize_capability_list(values, default_value)
  local out = {}
  local source = values
  if source == nil then
    source = { default_value }
  end
  if type(source) ~= "table" then
    return nil, error_mod.new("input", "service capability metadata must be a list")
  end
  local seen = {}
  for _, capability in ipairs(source) do
    if type(capability) ~= "string" or capability == "" then
      return nil, error_mod.new("input", "service capability names must be non-empty strings")
    end
    if not seen[capability] then
      seen[capability] = true
      out[#out + 1] = capability
    end
  end
  return out
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
    peer_discovery = nil,
    _bootstrap_discovery = nil,
    _bootstrap_discovery_due_at = nil,
    _bootstrap_discovery_done = false,
    _relay_candidate_replenish = {
      pending = false,
      cooldown_seconds = 30,
      next_allowed_at = 0,
      walk_task = nil,
      walk_inflight = false,
    },
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
    _handler_options = {},
    _autorelay_not_enough_relays = nil,
    _services = {},
    services = {},
    components = {},
    capabilities = {},
    _handler_tasks = {},
    _tasks = {},
    _task_queue = {},
    _task_read_waiters = {},
    _task_dial_waiters = {},
    _task_write_waiters = {},
    _task_completion_waiters = {},
    _next_task_id = 1,
    _task_resume_budget = cfg.task_resume_budget or DEFAULT_TASK_RESUME_BUDGET,
    _scheduler_connection_pump = runtime_name == "luv" and tcp_luv.BACKEND == "luv-native",
    _listeners = {},
    _connections = {},
    _connections_by_peer = {},
    _connections_by_id = {},
    _next_connection_id = 1,
    _pending_inbound = {},
    _pending_relay_inbound = {},
    _event_handlers = {},
    _debug_connection_events = cfg.debug_connection_events == true,
    _event_queue_max = cfg.event_queue_max or DEFAULT_EVENT_QUEUE_MAX,
    _last_advertised_addrs = nil,
    _event_subscribers = {},
    _next_subscriber_id = 1,
    _identify_on_connect_handler = nil,
    _identify_inflight = {},
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
    _service_options = {
      identify = cfg.identify or {},
      autonat = cfg.autonat or {},
      autorelay = cfg.autorelay or {},
      kad_dht = cfg.kad_dht or {},
      perf = cfg.perf or {},
      upnp_nat = cfg.upnp_nat or {},
    },
  }, self)
  self_obj.connection_manager = cfg.connection_manager
    or connection_manager.new(self_obj, cfg.dial_queue or cfg.connection_manager_options or {})

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

  local services = cfg.services
  if contains_circuit_listen_addr(self_obj.listen_addrs) and not (type(services) == "table" and services.autorelay ~= nil) then
    return nil, error_mod.new("input", "/p2p-circuit listen addr requires autorelay service")
  end
  if services ~= nil then
    if type(services) ~= "table" then
      return nil, error_mod.new("input", "services must be a map")
    end
    local service_defs = {}
    local capability_providers = {}

    for service_name, service_spec in pairs(services) do
      if type(service_name) ~= "string" or service_name == "" then
        return nil, error_mod.new("input", "service names must be non-empty strings")
      end
      local module = service_spec
      local service_opts = {}
      local service_provides = nil
      local service_requires = nil
      if type(service_spec) == "table" and service_spec.module ~= nil then
        module = service_spec.module
        service_opts = service_spec.config or {}
        service_provides = service_spec.provides
        service_requires = service_spec.requires
      end
      if type(module) ~= "table" or type(module.new) ~= "function" then
        return nil, error_mod.new("input", "service entry must be module with .new(host, config)", {
          service = service_name,
        })
      end

      local provides, provides_err = normalize_capability_list(service_provides or module.provides, service_name)
      if not provides then
        return nil, error_mod.wrap("input", "invalid service provides metadata", provides_err, {
          service = service_name,
        })
      end
      local requires, requires_err = normalize_capability_list(service_requires or module.requires, nil)
      if not requires then
        return nil, error_mod.wrap("input", "invalid service requires metadata", requires_err, {
          service = service_name,
        })
      end

      local instance, new_err = module.new(self_obj, service_opts, service_name)
      if not instance then
        return nil, new_err
      end

      local def = {
        name = service_name,
        module = module,
        opts = service_opts,
        instance = instance,
        provides = provides,
        requires = requires,
        started = false,
      }
      service_defs[#service_defs + 1] = def

      for _, capability in ipairs(provides) do
        local existing = capability_providers[capability]
        if existing and existing ~= service_name then
          return nil, error_mod.new("input", "duplicate service capability provider", {
            capability = capability,
            existing_service = existing,
            conflicting_service = service_name,
          })
        end
        capability_providers[capability] = service_name
        self_obj.components[capability] = instance
        self_obj.capabilities[capability] = instance
      end

      self_obj._service_options[service_name] = service_opts
      self_obj.services[service_name] = instance
      self_obj._services[service_name] = instance
      if service_name == "autorelay" then
        self_obj.autorelay = instance
      elseif service_name == "autonat" then
        self_obj.autonat = instance
      elseif service_name == "upnp_nat" then
        self_obj.upnp_nat = instance
      elseif service_name == "kad_dht" then
        self_obj.kad_dht = instance
      end
    end

    for _, def in ipairs(service_defs) do
      for _, required in ipairs(def.requires) do
        if capability_providers[required] == nil then
          return nil, error_mod.new("state", "service dependency is missing", {
            service = def.name,
            required_capability = required,
          })
        end
      end
    end

    local started_count = 0
    while started_count < #service_defs do
      local progressed = false
      for _, def in ipairs(service_defs) do
        if not def.started then
          local can_start = true
          for _, required in ipairs(def.requires) do
            local provider_name = capability_providers[required]
            if provider_name ~= def.name then
              local provider_started = false
              for _, candidate in ipairs(service_defs) do
                if candidate.name == provider_name then
                  provider_started = candidate.started
                  break
                end
              end
              if not provider_started then
                can_start = false
                break
              end
            end
          end

          if can_start then
            if type(def.instance.start) == "function" then
              local started, start_err = def.instance:start()
              if not started then
                return nil, start_err
              end
            end
            def.started = true
            started_count = started_count + 1
            progressed = true
          end
        end
      end

      if not progressed then
        local blocked = {}
        for _, def in ipairs(service_defs) do
          if not def.started then
            blocked[#blocked + 1] = def.name
          end
        end
        return nil, error_mod.new("state", "service dependency cycle or unresolved dependency", {
          blocked_services = blocked,
        })
      end
    end

    for capability, provider_name in pairs(capability_providers) do
      local provider = self_obj._services[provider_name]
      if self_obj[capability] == nil then
        self_obj[capability] = provider
      end
    end

    if self_obj.autorelay and type(self_obj.on) == "function" then
      self_obj._autorelay_not_enough_relays = function(payload)
        self_obj:_request_relay_candidate_replenish(payload and payload.reason or "relay_not_enough")
        return true
      end
      local token, on_err = self_obj:on("relay:not-enough-relays", self_obj._autorelay_not_enough_relays)
      if not token then
        return nil, on_err
      end
    end
  end

  if cfg.service ~= nil then
    return nil, error_mod.new("input", "service option is removed; use services map")
  end

  return self_obj
end

function Host:is_running()
  return self._running
end

--- Register a stream handler for a protocol ID.
-- @tparam string protocol_id Protocol multistream ID.
-- @tparam function handler Stream handler `(stream, ctx)`.
-- @tparam[opt] table opts Handler options.
-- `opts.run_on_limited_connection=true` allows this protocol on limited relay links.
-- @treturn true|nil ok True on success, otherwise nil.
-- @treturn[opt] table err Structured error on failure.
function Host:handle(protocol_id, handler, opts)
  if type(protocol_id) ~= "string" or protocol_id == "" then
    return nil, error_mod.new("input", "protocol id must be non-empty")
  end
  if type(handler) ~= "function" then
    return nil, error_mod.new("input", "handler must be a function")
  end
  local options = opts or {}
  self._handlers[protocol_id] = handler
  self._handler_options[protocol_id] = options
  return true
end

function Host:_connection_is_limited(state)
  if type(state) ~= "table" then
    return false
  end
  if state.limited == true then
    return true
  end
  return type(state.relay) == "table" and state.relay.limit_kind == "limited"
end

--- Check limited-connection policy for one protocol.
-- `opts.allow_limited_connection=true` overrides default restrictions.
function Host:_protocol_allowed_on_limited_connection(protocol_id, opts)
  if type(protocol_id) ~= "string" or protocol_id == "" then
    return false
  end
  local options = opts or {}
  if options.allow_limited_connection == true
    or options.run_on_limited_connection == true
  then
    return true
  end
  local handler_options = self._handler_options[protocol_id]
  if type(handler_options) == "table" and handler_options.run_on_limited_connection == true then
    return true
  end
  return false
end

--- Filter protocol list by limited-connection policy.
-- `opts.allow_limited_connection=true` allows all protocols.
function Host:_protocols_allowed_on_limited_connection(protocols, opts)
  for _, protocol_id in ipairs(normalize_protocol_list(protocols) or {}) do
    if not self:_protocol_allowed_on_limited_connection(protocol_id, opts) then
      return nil, error_mod.new("permission", "protocol is not allowed over limited connection", {
        protocol = protocol_id,
      })
    end
  end
  return true
end

function Host:_spawn_handler_task(handler, ctx)
  if self._scheduler_connection_pump
    and type(self.spawn_task) == "function"
  then
    return self:spawn_task("handler." .. tostring(ctx.protocol or "unknown"), function(task_ctx)
      local handler_ctx = {}
      for k, v in pairs(ctx or {}) do
        handler_ctx[k] = v
      end
      for k, v in pairs(task_ctx) do
        if handler_ctx[k] == nil then
          handler_ctx[k] = v
        end
      end
      local result, err = handler(ctx.stream, handler_ctx)
      if result == nil and err then
        if is_nonfatal_stream_error(err) then
          if ctx.connection and type(ctx.connection.close) == "function" then
            ctx.connection:close()
          end
          return true
        end
        return nil, err
      end
      return result
    end, {
      service = "handler",
      protocol = ctx.protocol,
      peer_id = ctx.peer_id,
    })
  end

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

function Host:_spawn_stream_negotiation_task(stream, conn, entry)
  if type(self.spawn_task) ~= "function" then
    return nil, error_mod.new("unsupported", "stream negotiation task requires scheduler")
  end
  return self:spawn_task("host.stream_negotiate", function()
    local router, router_err = self:_build_router()
    if not router then
      return nil, router_err
    end
    local protocol_id, handler, handler_options, neg_err = router:negotiate(stream)
    if not protocol_id then
      return nil, neg_err
    end
    if self:_connection_is_limited(entry.state)
      and not self:_protocol_allowed_on_limited_connection(protocol_id, handler_options)
    then
      if type(stream.reset_now) == "function" then
        pcall(function() stream:reset_now() end)
      elseif type(stream.close) == "function" then
        pcall(function() stream:close() end)
      end
      return true
    end
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

local function map_count(values)
  local n = 0
  for _ in pairs(values or {}) do
    n = n + 1
  end
  return n
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
          inflight = map_count(self._identify_inflight),
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
    protocols = list_protocol_handlers(self._handlers),
  }

  if self._debug_connection_events then
    emit_event(self, "identify:response:send", {
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
      inflight = map_count(self._identify_inflight),
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

function Host:_build_router()
  local router = require("lua_libp2p.multistream_select.protocol").new_router()
  for protocol_id, handler in pairs(self._handlers) do
    local ok, err = router:register(protocol_id, handler, self._handler_options[protocol_id])
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
  state = state or {}
  state.direction = state.direction or "unknown"
  if self.connection_manager and type(self.connection_manager.can_open_connection) == "function" then
    local can_open, limit_err = self.connection_manager:can_open_connection(state)
    if not can_open then
      return nil, limit_err
    end
  end
  local connection_id = self._next_connection_id
  self._next_connection_id = self._next_connection_id + 1
  local entry = {
    id = connection_id,
    conn = conn,
    state = state,
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

  local function rollback_registration()
    for i = #self._connections, 1, -1 do
      if self._connections[i] == entry then
        table.remove(self._connections, i)
        break
      end
    end
    self._connections_by_id[connection_id] = nil
    if peer_id and self._connections_by_peer[peer_id] then
      self._connections_by_peer[peer_id][connection_id] = nil
      if next(self._connections_by_peer[peer_id]) == nil then
        self._connections_by_peer[peer_id] = nil
      end
    end
    if entry._connection_manager_tracked
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
    return nil, opened_err
  end

  if self._running and self._runtime_impl and self._runtime_impl.sync_watchers then
    local sync_ok, sync_err = self._runtime_impl.sync_watchers(self)
    if not sync_ok then
      rollback_registration()
      return nil, sync_err
    end
  end

  if self._scheduler_connection_pump
    and host_runtime_luv_native.is_native_host(self)
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
  if entry.scheduler_pump_task and entry.scheduler_pump_task.status ~= "completed" then
    self:cancel_task(entry.scheduler_pump_task.id)
  end
  if entry.id ~= nil then
    self._connections_by_id[entry.id] = nil
  end
  local peer_id = entry.state and entry.state.remote_peer_id
  log.debug("connection closed", {
    peer_id = peer_id,
    connection_id = entry.id,
    direction = entry.state and entry.state.direction or nil,
    security = entry.state and entry.state.security or nil,
    muxer = entry.state and entry.state.muxer or nil,
    cause = tostring(cause),
    subsystem = "host",
  })
  if peer_id and self._connections_by_peer[peer_id] then
    if entry.id ~= nil then
      self._connections_by_peer[peer_id][entry.id] = nil
    end
    if next(self._connections_by_peer[peer_id]) == nil then
      self._connections_by_peer[peer_id] = nil
    end
  end
  if self.connection_manager and type(self.connection_manager.on_connection_closed) == "function" then
    self.connection_manager:on_connection_closed(entry)
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

--- Subscribe to host events.
-- @tparam string event_name Event key to subscribe to.
-- @tparam function handler Callback `(payload, event)`.
-- Common events/payload keys:
-- - `connection_opened`: `{ peer_id, connection_id, direction, remote_addr, limited }`
-- - `connection_closed`: `{ peer_id, connection_id, cause }`
-- - `peer_identified`: `{ peer_id, protocols, observed_addr }`
-- - `stream:negotiated`: `{ peer_id, connection_id, protocol, limited }`
-- - `task:started|task:completed|task:failed|task:cancelled`: `{ task_id, name, service }`
-- @treturn true|nil ok
-- @treturn[opt] table err
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

--- Emit a host event.
-- @tparam string event_name Event key.
-- @tparam[opt] table payload Event payload table.
-- @treturn true|nil ok
-- @treturn[opt] table err
function Host:emit(event_name, payload)
  if type(event_name) ~= "string" or event_name == "" then
    return nil, error_mod.new("input", "event name must be non-empty")
  end
  return emit_event(self, event_name, payload)
end

--- Remove a previously registered event handler.
-- @tparam string event_name Event key.
-- @tparam function handler Previously registered callback.
-- @treturn boolean removed
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

--- Spawn a cooperative scheduler task.
-- Task callback receives a context with `sleep`, `await_task`, and IO wait helpers.
-- @tparam string name Task name.
-- @tparam function fn Task callback `(ctx)`.
-- @tparam[opt] table opts Task metadata.
-- `opts` may include `service`, `peer_id`, and `metadata` values for diagnostics.
-- @treturn table|nil task
-- @treturn[opt] table err
function Host:spawn_task(name, fn, opts)
  if type(name) ~= "string" or name == "" then
    return nil, error_mod.new("input", "task name must be non-empty")
  end
  if type(fn) ~= "function" then
    return nil, error_mod.new("input", "task function is required")
  end
  local options = opts or {}
  local id = self._next_task_id
  self._next_task_id = id + 1
  local task = {
    id = id,
    name = name,
    service = options.service,
    status = "ready",
    started_at = now_seconds(),
    updated_at = now_seconds(),
    wake_at = nil,
    result = nil,
    error = nil,
    cancelled = false,
  }
  local ctx = make_task_context(task)
  task.co = coroutine.create(function()
    return fn(ctx)
  end)
  self._tasks[id] = task
  if options.priority == "front" then
    table.insert(self._task_queue, 1, id)
  else
    self._task_queue[#self._task_queue + 1] = id
  end
  emit_event(self, "task:started", {
    task_id = id,
    name = name,
    service = task.service,
  })
  return task
end

function Host:get_task(task_id)
  return self._tasks[task_id]
end

function Host:list_tasks()
  local out = {}
  for _, task in pairs(self._tasks) do
    out[#out + 1] = task
  end
  table.sort(out, function(a, b) return a.id < b.id end)
  return out
end

function Host:task_stats()
  local stats = {
    total = 0,
    queue_depth = #self._task_queue,
    identify_inflight = map_count(self._identify_inflight),
    by_status = {},
    by_name = {},
    by_service = {},
  }
  for _, task in pairs(self._tasks) do
    stats.total = stats.total + 1
    local status = task.status or "unknown"
    stats.by_status[status] = (stats.by_status[status] or 0) + 1
    local name = task.name or "unknown"
    stats.by_name[name] = (stats.by_name[name] or 0) + 1
    local service = task.service or "unknown"
    stats.by_service[service] = (stats.by_service[service] or 0) + 1
  end
  return stats
end

function Host:stats()
  local connection_stats = nil
  if self.connection_manager and type(self.connection_manager.stats) == "function" then
    connection_stats = self.connection_manager:stats()
  end
  return {
    runtime = self._runtime,
    running = self._running == true,
    peer_id = self:peer_id().id,
    tasks = self:task_stats(),
    connections = connection_stats,
  }
end

function Host:_cleanup_task_waiters(task)
  if not task then
    return false
  end
  local waiting_on = task.waiting_on
  if waiting_on then
    local waiter_maps = {
      waiting_read = self._task_read_waiters,
      waiting_dial = self._task_dial_waiters,
      waiting_write = self._task_write_waiters,
    }
    local waiters = waiter_maps[task.status] and waiter_maps[task.status][waiting_on]
    if waiters then
      waiters[task.id] = nil
      if next(waiters) == nil then
        waiter_maps[task.status][waiting_on] = nil
      end
    end
  end
  if type(task.read_unwatch) == "function" then
    pcall(task.read_unwatch)
  end
  if type(task.dial_unwatch) == "function" then
    pcall(task.dial_unwatch)
  end
  if type(task.write_unwatch) == "function" then
    pcall(task.write_unwatch)
  end
  if type(task.task_unwatch) == "function" then
    pcall(task.task_unwatch)
  end
  task.read_unwatch = nil
  task.dial_unwatch = nil
  task.write_unwatch = nil
  task.task_unwatch = nil
  task.waiting_on = nil
  task._read_woke_before_unwatch = nil
  task._dial_woke_before_unwatch = nil
  task._write_woke_before_unwatch = nil
  return true
end

function Host:cancel_task(task_id)
  local task = self._tasks[task_id]
  if not task then
    return false
  end
  if task.status == "completed" or task.status == "failed" or task.status == "cancelled" then
    return false
  end
  self:_cleanup_task_waiters(task)
  task.cancelled = true
  task.status = "cancelled"
  task.updated_at = now_seconds()
  emit_event(self, "task:cancelled", {
    task_id = task.id,
    name = task.name,
    service = task.service,
  })
  self:_wake_task_completion_waiters(task)
  return true
end

function Host:_enqueue_task(task)
  if not task or task.status ~= "ready" then
    return false
  end
  self._task_queue[#self._task_queue + 1] = task.id
  return true
end

--- Run poll loop until task reaches terminal state.
-- `opts.timeout` (`number`) sets wall-clock timeout seconds.
-- `opts.poll_interval` (`number`, default `0.01`) controls poll cadence.
-- @tparam table task
-- @tparam[opt] table opts
-- @return Task result values on completion.
function Host:run_until_task(task, opts)
  if type(task) ~= "table" then
    return nil, error_mod.new("input", "task table is required")
  end
  local options = opts or {}
  local poll_interval = options.poll_interval
  if poll_interval == nil then
    poll_interval = 0.01
  end
  local timeout = options.timeout
  local deadline = timeout and (now_seconds() + timeout) or nil

  while task.status ~= "completed" and task.status ~= "failed" and task.status ~= "cancelled" do
    if deadline and now_seconds() >= deadline then
      return nil, error_mod.new("timeout", "task wait timed out", {
        task_id = task.id,
        name = task.name,
      })
    end
    local ok, err = self:poll_once(poll_interval)
    if not ok then
      return nil, err
    end
  end
  if task.status ~= "completed" then
    return nil, task.error or error_mod.new("state", "task did not complete", {
      task_id = task.id,
      name = task.name,
      status = task.status,
    })
  end
  if task.results then
    return unpack_returns(task.results)
  end
  return task.result
end

--- Wait for a task to complete.
-- Uses `ctx:await_task` when called from within another task.
-- @tparam table task Task returned by `spawn_task`.
-- @tparam[opt] table opts Wait options.
-- `opts` supports `ctx`, `timeout`, and `poll_interval`.
-- @return Task callback return values on success.
function Host:wait_task(task, opts)
  if type(task) ~= "table" then
    return nil, error_mod.new("input", "task table is required")
  end
  local options = opts or {}
  if options.ctx and type(options.ctx.await_task) == "function" then
    return options.ctx:await_task(task)
  end
  return self:run_until_task(task, options)
end

--- Cooperative sleep helper.
-- Uses task context sleep when `opts.ctx` is present; otherwise runs a helper task.
-- @tparam number seconds Non-negative sleep duration.
-- @tparam[opt] table opts Wait options.
-- `opts` supports `ctx`, `timeout`, and `poll_interval`.
-- @treturn true|nil ok
-- @treturn[opt] table err
function Host:sleep(seconds, opts)
  if type(seconds) ~= "number" or seconds < 0 then
    return nil, error_mod.new("input", "sleep duration must be a non-negative number")
  end
  local options = opts or {}
  if options.ctx and type(options.ctx.sleep) == "function" then
    local slept, sleep_err = options.ctx:sleep(seconds)
    if slept == nil and sleep_err then
      return nil, sleep_err
    end
    return true
  end
  local task, task_err = self:spawn_task("host.sleep", function(ctx)
    local slept, sleep_err = ctx:sleep(seconds)
    if slept == nil and sleep_err then
      return nil, sleep_err
    end
    return true
  end, { service = "host" })
  if not task then
    return nil, task_err
  end
  return self:run_until_task(task, options)
end

function Host:_wait_task_read(task, connection)
  if not task or not connection then
    return false
  end
  local waiters = self._task_read_waiters[connection]
  if not waiters then
    waiters = {}
    self._task_read_waiters[connection] = waiters
  end
  waiters[task.id] = true
  task.status = "waiting_read"
  task.waiting_on = connection
  task.updated_at = now_seconds()
  if task.read_unwatch == nil and type(connection.watch_luv_readable) == "function" then
    local ok, unwatch = pcall(connection.watch_luv_readable, connection, function()
      self:_wake_task_readers(connection)
    end)
    if ok and type(unwatch) == "function" then
      if task._read_woke_before_unwatch then
        task._read_woke_before_unwatch = nil
        pcall(unwatch)
      elseif task.status == "waiting_read" then
        task.read_unwatch = unwatch
      else
        pcall(unwatch)
      end
    end
  end
  return true
end

function Host:_wake_task_readers(connection)
  local waiters = self._task_read_waiters[connection]
  if not waiters then
    return false
  end
  self._task_read_waiters[connection] = nil
  for task_id in pairs(waiters) do
    local task = self._tasks[task_id]
    if task and task.status == "waiting_read" and not task.cancelled then
      if type(task.read_unwatch) == "function" then
        pcall(task.read_unwatch)
        task.read_unwatch = nil
      else
        task._read_woke_before_unwatch = true
      end
      if type(task.dial_unwatch) == "function" then
        pcall(task.dial_unwatch)
        task.dial_unwatch = nil
      end
      if type(task.write_unwatch) == "function" then
        pcall(task.write_unwatch)
        task.write_unwatch = nil
      end
      task.status = "ready"
      task.waiting_on = nil
      task.updated_at = now_seconds()
      self:_enqueue_task(task)
    end
  end
  return true
end

function Host:_wait_task_dial(task, connection)
  if not task or not connection then
    return false
  end
  local waiters = self._task_dial_waiters[connection]
  if not waiters then
    waiters = {}
    self._task_dial_waiters[connection] = waiters
  end
  waiters[task.id] = true
  task.status = "waiting_dial"
  task.waiting_on = connection
  task.updated_at = now_seconds()
  if task.dial_unwatch == nil and type(connection.watch_luv_connect) == "function" then
    local ok, unwatch = pcall(connection.watch_luv_connect, connection, function()
      self:_wake_task_dialers(connection)
    end)
    if ok and type(unwatch) == "function" then
      if task._dial_woke_before_unwatch then
        task._dial_woke_before_unwatch = nil
        pcall(unwatch)
      elseif task.status == "waiting_dial" then
        task.dial_unwatch = unwatch
      else
        pcall(unwatch)
      end
    end
  end
  return true
end

function Host:_wake_task_dialers(connection)
  local waiters = self._task_dial_waiters[connection]
  if not waiters then
    return false
  end
  self._task_dial_waiters[connection] = nil
  for task_id in pairs(waiters) do
    local task = self._tasks[task_id]
    if task and task.status == "waiting_dial" and not task.cancelled then
      if type(task.dial_unwatch) == "function" then
        pcall(task.dial_unwatch)
        task.dial_unwatch = nil
      else
        task._dial_woke_before_unwatch = true
      end
      if type(task.write_unwatch) == "function" then
        pcall(task.write_unwatch)
        task.write_unwatch = nil
      end
      task.status = "ready"
      task.waiting_on = nil
      task.updated_at = now_seconds()
      self:_enqueue_task(task)
    end
  end
  return true
end

function Host:_wait_task_write(task, connection)
  if not task or not connection then
    return false
  end
  local waiters = self._task_write_waiters[connection]
  if not waiters then
    waiters = {}
    self._task_write_waiters[connection] = waiters
  end
  waiters[task.id] = true
  task.status = "waiting_write"
  task.waiting_on = connection
  task.updated_at = now_seconds()
  if task.write_unwatch == nil and type(connection.watch_luv_write) == "function" then
    local ok, unwatch = pcall(connection.watch_luv_write, connection, function()
      self:_wake_task_writers(connection)
    end)
    if ok and type(unwatch) == "function" then
      if task._write_woke_before_unwatch then
        task._write_woke_before_unwatch = nil
        pcall(unwatch)
      elseif task.status == "waiting_write" then
        task.write_unwatch = unwatch
      else
        pcall(unwatch)
      end
    end
  end
  return true
end

function Host:_wake_task_writers(connection)
  local waiters = self._task_write_waiters[connection]
  if not waiters then
    return false
  end
  self._task_write_waiters[connection] = nil
  for task_id in pairs(waiters) do
    local task = self._tasks[task_id]
    if task and task.status == "waiting_write" and not task.cancelled then
      if type(task.write_unwatch) == "function" then
        pcall(task.write_unwatch)
        task.write_unwatch = nil
      else
        task._write_woke_before_unwatch = true
      end
      task.status = "ready"
      task.waiting_on = nil
      task.updated_at = now_seconds()
      self:_enqueue_task(task)
    end
  end
  return true
end

function Host:_wait_task_completion(task, child_tasks)
  if not task or type(child_tasks) ~= "table" then
    return false
  end
  local watched_ids = {}
  for _, child_task in ipairs(child_tasks) do
    if type(child_task) == "table" and type(child_task.id) == "number" then
      if child_task.status == "completed" or child_task.status == "failed" or child_task.status == "cancelled" then
        task.status = "ready"
        task.updated_at = now_seconds()
        self:_enqueue_task(task)
        return true
      end
      local waiters = self._task_completion_waiters[child_task.id]
      if not waiters then
        waiters = {}
        self._task_completion_waiters[child_task.id] = waiters
      end
      waiters[task.id] = true
      watched_ids[#watched_ids + 1] = child_task.id
    end
  end
  if #watched_ids == 0 then
    task.status = "ready"
    task.updated_at = now_seconds()
    self:_enqueue_task(task)
    return true
  end
  task.status = "waiting_task"
  task.waiting_on = "task"
  task.updated_at = now_seconds()
  task.task_unwatch = function()
    for _, child_id in ipairs(watched_ids) do
      local waiters = self._task_completion_waiters[child_id]
      if waiters then
        waiters[task.id] = nil
        if next(waiters) == nil then
          self._task_completion_waiters[child_id] = nil
        end
      end
    end
    return true
  end
  return true
end

function Host:_wake_task_completion_waiters(child_task)
  if not child_task or type(child_task.id) ~= "number" then
    return false
  end
  local waiters = self._task_completion_waiters[child_task.id]
  if not waiters then
    return false
  end
  self._task_completion_waiters[child_task.id] = nil
  for task_id in pairs(waiters) do
    local task = self._tasks[task_id]
    if task and task.status == "waiting_task" and not task.cancelled then
      if type(task.task_unwatch) == "function" then
        pcall(task.task_unwatch)
        task.task_unwatch = nil
      end
      task.status = "ready"
      task.waiting_on = nil
      task.updated_at = now_seconds()
      self:_enqueue_task(task)
    end
  end
  return true
end

--- Run periodic host background tasks.
-- `opts.now` can override current epoch seconds for testability.
function Host:_run_background_tasks(opts)
  local options = opts or {}
  local budget = options.max_resumes or self._task_resume_budget or DEFAULT_TASK_RESUME_BUDGET
  local now = now_seconds()

  for _, task in pairs(self._tasks) do
    if task.status == "sleeping" and task.wake_at and task.wake_at <= now then
      task.status = "ready"
      task.wake_at = nil
      task.updated_at = now
      self:_enqueue_task(task)
    end
  end

  local resumes = 0
  while resumes < budget and #self._task_queue > 0 do
    local task_id = table.remove(self._task_queue, 1)
    local task = self._tasks[task_id]
    if not task or task.status ~= "ready" then
      goto continue_tasks
    end
    if task.cancelled then
      self:_cleanup_task_waiters(task)
      task.status = "cancelled"
      task.updated_at = now_seconds()
      goto continue_tasks
    end

    resumes = resumes + 1
    task.status = "running"
    task.updated_at = now_seconds()
    local resumed = pack_returns(coroutine.resume(task.co))
    local ok = resumed[1]
    local result_or_yield = resumed[2]
    local extra = resumed[3]
    task.updated_at = now_seconds()
    if not ok then
      self:_cleanup_task_waiters(task)
      task.status = "failed"
      task.error = error_mod.new("protocol", "task panicked", {
        task_id = task.id,
        name = task.name,
        cause = result_or_yield,
        traceback = debug.traceback(task.co, tostring(result_or_yield)),
      })
      emit_event(self, "task:failed", {
        task_id = task.id,
        name = task.name,
        service = task.service,
        error = task.error,
      })
      self:_wake_task_completion_waiters(task)
      goto continue_tasks
    end
    if coroutine.status(task.co) == "dead" then
      self:_cleanup_task_waiters(task)
      if result_or_yield == nil and extra then
        task.status = "failed"
        task.error = extra
        emit_event(self, "task:failed", {
          task_id = task.id,
          name = task.name,
          service = task.service,
          error = task.error,
        })
        self:_wake_task_completion_waiters(task)
      else
        task.status = "completed"
        task.result = result_or_yield
        task.results = { n = math.max((resumed.n or 1) - 1, 0) }
        for result_index = 2, resumed.n or 1 do
          task.results[result_index - 1] = resumed[result_index]
        end
        emit_event(self, "task:completed", {
          task_id = task.id,
          name = task.name,
          service = task.service,
          result = task.result,
        })
        self:_wake_task_completion_waiters(task)
      end
      goto continue_tasks
    end

    local yielded = result_or_yield
    if type(yielded) == "table" and yielded.type == "sleep" then
      task.status = "sleeping"
      task.wake_at = yielded.until_at or now_seconds()
    elseif type(yielded) == "table" and yielded.type == "read" then
      self:_wait_task_read(task, yielded.connection or yielded.conn or yielded.watchable)
    elseif type(yielded) == "table" and yielded.type == "dial" then
      self:_wait_task_dial(task, yielded.connection or yielded.conn or yielded.watchable)
    elseif type(yielded) == "table" and yielded.type == "write" then
      self:_wait_task_write(task, yielded.connection or yielded.conn or yielded.watchable)
    elseif type(yielded) == "table" and yielded.type == "task_wait" then
      self:_wait_task_completion(task, yielded.tasks or yielded.task or yielded.child_tasks)
    else
      task.status = "ready"
      self:_enqueue_task(task)
    end

    ::continue_tasks::
  end

  return true
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

--- Create an event queue subscription.
-- `event_name_or_opts` may be a string event name, or table `{ event_name|event, max_queue }`.
-- `opts.max_queue` (`number`) overrides per-subscriber queue bound.
-- @param[opt] event_name_or_opts Event name or options table.
-- @tparam[opt] table opts Options when first arg is event name.
-- @treturn table|nil subscriber
-- @treturn[opt] table err
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

--- Remove a queue subscription.
-- @param subscriber Subscriber table or numeric id.
-- @treturn boolean removed
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

--- Pop next queued event from a subscription.
-- Returns `nil` when queue is empty.
-- @tparam table subscriber Subscriber handle returned by @{subscribe}.
-- @treturn table|nil event
-- @treturn[opt] table err
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

--- Locate existing connection for peer.
-- `opts.allow_limited_connection=true` permits returning limited relay links.
function Host:_find_connection(peer_id, opts)
  if not peer_id then
    return nil
  end
  local options = opts or {}
  local by_peer = self._connections_by_peer[peer_id]
  if not by_peer then
    return nil
  end
  local limited = nil
  for _, entry in pairs(by_peer) do
    if self:_connection_is_limited(entry.state) then
      limited = limited or entry
    else
      return entry
    end
  end
  if options.require_unlimited_connection then
    return nil
  end
  return limited
end

function Host:_find_connection_by_id(connection_id)
  return self._connections_by_id[connection_id]
end

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
  end
  if #candidate_addrs == 0 then
    return nil, nil, error_mod.new("input", "dial target must include an address when no connection exists")
  end

  local deadline = opts.dial_timeout and (now_seconds() + opts.dial_timeout) or nil
  local last_err
  for _, addr in ipairs(candidate_addrs) do
    if deadline and now_seconds() >= deadline then
      return nil, nil, error_mod.new("timeout", "dial timed out", { peer_id = resolved.peer_id })
    end
    local raw_conn, dial_err, relay_state
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
          return entry.conn, entry.state
        end
        conn:close()
        last_err = register_err
      end
    elseif dial_err then
      last_err = dial_err
    end
  end

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

  local conn, state, dial_err = self:dial(peer_or_addr, stream_opts)
  if not conn then
    return nil, nil, nil, dial_err
  end

  if self:_connection_is_limited(state) then
    local allowed, allow_err = self:_protocols_allowed_on_limited_connection(protocols, stream_opts)
    if not allowed then
      return nil, nil, nil, allow_err
    end
  end

  local stream, selected, stream_err = conn:new_stream(protocols)
  if not stream then
    return nil, nil, nil, stream_err
  end

  return stream, selected, conn, state
end

function Host:_poll_once_with_ready_map(timeout, ready_map)
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
      local entry, register_err = self:_register_connection(conn, state)
      if not entry then
        conn:close()
        return nil, register_err
      end
    elseif not (up_err and error_mod.is_error(up_err) and up_err.kind == "timeout") then
      remove_pending_relay_inbound(i, pending)
      if raw_conn and type(raw_conn.close) == "function" then
        raw_conn:close()
      end
      if not is_nonfatal_stream_error(up_err) then
        return nil, up_err
      end
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
      state.direction = state.direction or "inbound"
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
      if not should_accept and host_runtime_luv_native.is_native_host(self) then
        should_accept = true
      end
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
      state.direction = state.direction or "inbound"
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

  local router = nil
  if host_runtime_luv_native.is_native_host(self) then
    local router_err
    router, router_err = self:_build_router()
    if not router then
      return nil, router_err
    end
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
        if self:_connection_is_limited(entry.state)
          and not self:_protocol_allowed_on_limited_connection(protocol_id, handler_options)
        then
          if type(stream.reset_now) == "function" then
            pcall(function() stream:reset_now() end)
          elseif type(stream.close) == "function" then
            pcall(function() stream:close() end)
          end
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

  local ok, task_err = self:_run_handler_tasks()
  if not ok then
    return nil, task_err
  end

  local bg_ok, bg_err = self:_run_background_tasks()
  if not bg_ok then
    return nil, bg_err
  end

  return true
end

function Host:_poll_once_luv(timeout)
  local ok_luv, uv = pcall(require, "luv")
  if ok_luv and self._runtime_impl and self._runtime_impl.sync_watchers then
    local sync_ok, sync_err = self._runtime_impl.sync_watchers(self)
    if not sync_ok then
      return nil, sync_err
    end
  end
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
  log.info("bootstrap discovery completed", {
    discovered = #peers,
    subsystem = "host",
  })
  for _, peer in ipairs(peers) do
    if type(peer) == "table" and type(peer.peer_id) == "string" and peer.peer_id ~= "" then
      if self.peerstore then
        self.peerstore:merge(peer.peer_id, { addrs = peer.addrs or {} })
        self.peerstore:tag(peer.peer_id, cfg.tag_name, {
          value = cfg.tag_value,
          ttl = cfg.tag_ttl,
        })
      end
      self:spawn_task("host.bootstrap_dial", function(ctx)
        if self:_find_connection(peer.peer_id) then
          log.debug("bootstrap dial skipped existing connection", {
            peer_id = peer.peer_id,
            subsystem = "host",
          })
          return true
        end
        log.debug("bootstrap dial attempt", {
          peer_id = peer.peer_id,
          addr_count = #(peer.addrs or {}),
          addr = (peer.addrs and peer.addrs[1]) or nil,
          subsystem = "host",
        })
        local ok, dial_conn, _, dial_err = pcall(function()
          return self:dial({
            peer_id = peer.peer_id,
            addrs = peer.addrs,
          }, {
            timeout = self._connect_timeout,
            io_timeout = self._io_timeout,
            ctx = ctx,
          })
        end)
        if ok and dial_conn then
          log.info("bootstrap dial succeeded", {
            peer_id = peer.peer_id,
            subsystem = "host",
          })
        else
          local cause = ok and dial_err or dial_conn
          log.warn("bootstrap dial failed", {
            peer_id = peer.peer_id,
            cause = tostring(cause),
            subsystem = "host",
          })
        end
        -- Bootstrap dials are opportunistic; identify events seed interested services.
        return ok == true and dial_conn ~= nil
      end, {
        service = "host",
        peer_id = peer.peer_id,
      })
    end
  end
  return true
end

function Host:_request_relay_candidate_replenish(reason)
  if not self._relay_candidate_replenish then
    return false
  end
  self._relay_candidate_replenish.pending = true
  self._relay_candidate_replenish.reason = reason
  return true
end

function Host:_run_relay_candidate_replenish_if_due(now)
  local state = self._relay_candidate_replenish
  if not state or not state.pending then
    return true
  end
  local current = now or os.time()
  if current < (state.next_allowed_at or 0) then
    return true
  end

  local ktable_peers = 0
  if self.kad_dht and self.kad_dht.routing_table and type(self.kad_dht.routing_table.all_peers) == "function" then
    ktable_peers = #(self.kad_dht.routing_table:all_peers() or {})
  end

  if ktable_peers == 0 and self._bootstrap_discovery and self._bootstrap_discovery.dial_on_start ~= false then
    self._bootstrap_discovery_done = false
    self._bootstrap_discovery_due_at = current
    state.pending = false
    state.next_allowed_at = current + (state.cooldown_seconds or 30)
    return true
  end

  if self.kad_dht and type(self.kad_dht.random_walk) == "function" then
    local walk = state.walk_task
    if walk and walk.status ~= "completed" and walk.status ~= "failed" and walk.status ~= "cancelled" then
      return true
    end
    local walk_op, walk_err = self.kad_dht:random_walk({
      alpha = self.kad_dht.alpha,
      disjoint_paths = self.kad_dht.disjoint_paths,
    })
    if not walk_op then
      return nil, walk_err
    end
    state.walk_task = walk_op:task()
    state.pending = false
    state.next_allowed_at = current + (state.cooldown_seconds or 30)
    return true
  end

  state.pending = false
  state.next_allowed_at = current + (state.cooldown_seconds or 30)
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
  local replenish_ok, replenish_err = self:_run_relay_candidate_replenish_if_due(os.time())
  if not replenish_ok then
    return nil, replenish_err
  end
  if self._runtime_impl and self._runtime_impl.poll_once then
    return self._runtime_impl.poll_once(self, timeout)
  end
  return nil, error_mod.new("state", "host runtime has no poll_once implementation", { runtime = self._runtime })
end

--- Start the host runtime.
-- Binds listeners when needed and enters the runtime loop for blocking hosts.
-- @treturn true|nil ok
-- @treturn[opt] table err
function Host:start()
  if #self._listeners == 0 then
    local ok, bind_err = bind_listeners(self)
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

--- Stop and close the host.
-- @treturn true|nil ok
-- @treturn[opt] table err
function Host:stop()
  self._running = false
  return self:close()
end

function Host:close()
  self._running = false
  if self._autorelay_not_enough_relays and type(self.off) == "function" then
    self:off("relay:not-enough-relays", self._autorelay_not_enough_relays)
    self._autorelay_not_enough_relays = nil
  end
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
  for _, task in pairs(self._tasks) do
    self:_cleanup_task_waiters(task)
  end
  self._handler_tasks = {}
  self._tasks = {}
  self._task_queue = {}
  self._task_read_waiters = {}
  self._task_dial_waiters = {}
  self._task_write_waiters = {}
  self._task_completion_waiters = {}

  for _, pending_entry in ipairs(self._pending_inbound) do
    local raw_conn = host_runtime_luv_native.pending_raw(pending_entry)
    if raw_conn and type(raw_conn.close) == "function" then
      raw_conn:close()
    end
  end
  self._pending_inbound = {}

  for _, listener in ipairs(self._listeners) do
    listener:close()
  end
  self._listeners = {}

  return true
end

--- Construct a new host instance.
-- @tparam[opt] table config Host configuration.
-- Common config keys: `identity`, `listen_addrs`, `services`, `peer_discovery`,
-- `runtime`, `blocking`, `security_transports`, `muxers`, `transports`.
-- Additional keys: `announce_addrs`, `no_announce_addrs`, `observed_addrs`,
-- `relay_addrs`, `peerstore`, `address_manager`, `connect_timeout`, and runtime tuning.
-- Scheduler/runtime tuning keys include: `event_queue_max`, `task_resume_budget`,
-- `accept_timeout`, `max_iterations`, `poll_interval`, and `on_started`.
-- Network behavior keys include: `dial_timeout`, `connect_timeout`, `connection_manager`,
-- `autonat`, `kad_dht`, `autorelay`, `upnp_nat`, `pcp`, `nat_pmp`, and `dcutr`.
-- @treturn table|nil host
-- @treturn[opt] table err
function M.new(config)
  return Host:new(config)
end

return M
