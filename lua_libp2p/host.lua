local ed25519 = require("lua_libp2p.crypto.ed25519")
local error_mod = require("lua_libp2p.error")
local multiaddr = require("lua_libp2p.multiaddr")
local peerid = require("lua_libp2p.peerid")
local identify = require("lua_libp2p.protocol.identify")
local key_pb = require("lua_libp2p.crypto.key_pb")
local perf = require("lua_libp2p.protocol.perf")
local ping = require("lua_libp2p.protocol.ping")
local upgrader = require("lua_libp2p.network.upgrader")
local tcp = require("lua_libp2p.transport.tcp")

local ok_socket, socket = pcall(require, "socket")

local M = {}

local DEFAULT_IDENTIFY_PROTOCOL_VERSION = "/lua-libp2p/0.1.0"
local DEFAULT_IDENTIFY_AGENT_VERSION = "lua-libp2p/0.1.0"

local function list_copy(values)
  local out = {}
  for i, v in ipairs(values or {}) do
    out[i] = v
  end
  return out
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
    local listener, listen_err = tcp.listen({
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

  return true
end

local Host = {}
Host.__index = Host

local function sleep_seconds(seconds)
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

local function unwrap_socket(value, seen)
  if type(value) ~= "table" then
    return nil
  end
  local visited = seen or {}
  if visited[value] then
    return nil
  end
  visited[value] = true

  if type(value.socket) == "function" then
    local ok, sock = pcall(value.socket, value)
    if ok and sock then
      return sock
    end
  end

  if value._socket then
    return value._socket
  end
  if value._server then
    return value._server
  end

  if value._raw then
    local sock = unwrap_socket(value._raw, visited)
    if sock then
      return sock
    end
  end
  if value._raw_conn then
    local sock = unwrap_socket(value._raw_conn, visited)
    if sock then
      return sock
    end
  end

  return nil
end

local function build_ready_map(host_obj, timeout)
  if not (ok_socket and type(socket.select) == "function") then
    return nil
  end

  local read_set = {}
  local by_socket = {}

  for _, listener in ipairs(host_obj._listeners) do
    local sock = unwrap_socket(listener)
    if sock then
      read_set[#read_set + 1] = sock
      by_socket[sock] = { kind = "listener", value = listener }
    end
  end

  for _, entry in ipairs(host_obj._connections) do
    local sock = unwrap_socket(entry.conn)
    if sock then
      read_set[#read_set + 1] = sock
      by_socket[sock] = { kind = "connection", value = entry }
    end
  end

  if #read_set == 0 then
    return {}
  end

  local wait_timeout = timeout
  if wait_timeout == nil then
    wait_timeout = host_obj._accept_timeout or 0
  end

  local readable, _, sel_err = socket.select(read_set, nil, wait_timeout)
  if not readable then
    return nil, error_mod.new("io", "socket select failed", { cause = sel_err })
  end

  local ready = {}
  for _, sock in ipairs(readable) do
    local item = by_socket[sock]
    if item then
      ready[item.value] = true
    end
  end
  return ready
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
      addr = target.addrs[1]
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

  local keypair = cfg.identity
  if not keypair then
    local generated, gen_err = ed25519.generate_keypair()
    if not generated then
      return nil, gen_err
    end
    keypair = generated
  end

  local local_peer, local_peer_err = peerid.from_ed25519_public_key(keypair.public_key)
  if not local_peer then
    return nil, local_peer_err
  end

  local self_obj = setmetatable({
    identity = keypair,
    _peer_id = local_peer,
    listen_addrs = list_copy(cfg.listen_addrs or cfg.listenAddrs or {}),
    transports = list_copy(cfg.transports or { "tcp" }),
    security_transports = list_copy(cfg.security_transports or cfg.securityTransports or {}),
    muxers = list_copy(cfg.muxers or {}),
    _handlers = {},
    _handler_tasks = {},
    _listeners = {},
    _connections = {},
    _connections_by_peer = {},
    _running = false,
    _connect_timeout = cfg.connect_timeout or cfg.connectTimeout or 2,
    _io_timeout = cfg.io_timeout or cfg.ioTimeout or 2,
    _accept_timeout = cfg.accept_timeout or cfg.acceptTimeout or 0,
    _service_options = {
      identify = cfg.identify or {},
      perf = cfg.perf or {},
    },
  }, self)

  if #self_obj.security_transports == 0 then
    self_obj.security_transports = { "/noise" }
  end
  if #self_obj.muxers == 0 then
    self_obj.muxers = { "/yamux/1.0.0" }
  end

  local services = cfg.services or cfg.service
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
      return nil, error_mod.new("protocol", "handler task panicked", {
        protocol = task.protocol,
        cause = result,
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

function Host:add_service(name)
  if type(name) ~= "string" or name == "" then
    return nil, error_mod.new("input", "service name must be non-empty")
  end

  if name == "identify" then
    local ok, err = self:handle(identify.ID, function(stream, ctx)
      return self:_handle_identify(stream, ctx)
    end)
    if not ok then
      return nil, err
    end

    local identify_opts = self._service_options.identify or {}
    if identify_opts.include_push ~= false then
      ok, err = self:handle(identify.PUSH_ID, function(stream, ctx)
        return self:_handle_identify(stream, ctx)
      end)
      if not ok then
        return nil, err
      end
    end

    return true
  end

  if name == "ping" then
    return self:handle(ping.ID, function(stream)
      return ping.handle(stream)
    end)
  end

  if name == "perf" then
    local perf_opts = self._service_options.perf or {}
    return self:handle(perf.ID, function(stream)
      return perf.handle(stream, {
        write_block_size = perf_opts.write_block_size or perf_opts.writeBlockSize,
        yield_every_bytes = perf_opts.yield_every_bytes or perf_opts.yieldEveryBytes,
      })
    end)
  end

  return nil, error_mod.new("input", "unsupported service", { service = name })
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
  return list_copy(self.listen_addrs)
end

function Host:peer_id()
  return self._peer_id
end

function Host:get_multiaddrs_raw()
  return list_copy(self.listen_addrs)
end

function Host:get_multiaddrs()
  local out = {}
  local pid = self._peer_id and self._peer_id.id or nil
  for _, addr in ipairs(self.listen_addrs) do
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
  local entry = {
    conn = conn,
    state = state or {},
  }
  self._connections[#self._connections + 1] = entry

  local peer_id = entry.state.remote_peer_id
  if peer_id then
    self._connections_by_peer[peer_id] = entry
  end

  return entry
end

function Host:_find_connection(peer_id)
  if not peer_id then
    return nil
  end
  return self._connections_by_peer[peer_id]
end

function Host:dial(peer_or_addr, opts)
  local resolved = resolve_target(peer_or_addr)
  if not resolved then
    return nil, nil, error_mod.new("input", "dial target must be peer id, multiaddr, or target table")
  end

  local existing = self:_find_connection(resolved.peer_id)
  if existing then
    return existing.conn, existing.state
  end

  local addr = resolved.addr
  if not addr then
    return nil, nil, error_mod.new("input", "dial target must include an address when no connection exists")
  end

  local endpoint, endpoint_err = multiaddr.to_tcp_endpoint(addr)
  if not endpoint then
    return nil, nil, endpoint_err
  end

  local raw_conn, dial_err = tcp.dial({
    host = endpoint.host,
    port = endpoint.port,
  }, {
    timeout = (opts and opts.timeout) or self._connect_timeout,
    io_timeout = (opts and opts.io_timeout) or self._io_timeout,
  })
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
    raw_conn:close()
    return nil, nil, up_err
  end

  local entry = self:_register_connection(conn, state)
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

function Host:poll_once(timeout)
  local ready_map, ready_err = build_ready_map(self, timeout)
  if not ready_map and ready_err then
    return nil, ready_err
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
      local conn, state, up_err = upgrader.upgrade_inbound(raw_conn, {
        local_keypair = self.identity,
        security_protocols = self.security_transports,
        muxer_protocols = self.muxers,
      })
      if not conn then
        raw_conn:close()
        if is_nonfatal_stream_error(up_err) then
          goto continue_listeners
        end
        return nil, up_err
      end
      self:_register_connection(conn, state)
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
      local _, process_err = conn:process_one()
      if process_err then
        if is_nonfatal_stream_error(process_err) then
          if process_err.kind ~= "timeout" then
            conn:close()
            table.remove(self._connections, i)
            if entry.state and entry.state.remote_peer_id then
              self._connections_by_peer[entry.state.remote_peer_id] = nil
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

function Host:start(opts)
  local options = opts or {}
  if #self._listeners == 0 then
    local ok, bind_err = bind_listeners(self, options.listen_addrs)
    if not ok then
      return nil, bind_err
    end
    if not self._listeners[1] then
      return nil, error_mod.new("state", "no listeners bound")
    end
  elseif options.listen_addrs ~= nil then
    local ok, bind_err = bind_listeners(self, options.listen_addrs)
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

  if type(options.on_started) == "function" then
    options.on_started(self)
  end

  local blocking = options.blocking ~= false
  if blocking then
    local iterations = 0
    local max_iterations = options.max_iterations
    local poll_interval = options.poll_interval or 0.01
    while self._running do
      local ok, err = self:poll_once(options.accept_timeout)
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
  for _, entry in ipairs(self._connections) do
    entry.conn:close()
  end
  self._connections = {}
  self._connections_by_peer = {}
  self._handler_tasks = {}

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
