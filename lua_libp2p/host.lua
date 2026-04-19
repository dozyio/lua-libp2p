local ed25519 = require("lua_libp2p.crypto.ed25519")
local error_mod = require("lua_libp2p.error")
local multiaddr = require("lua_libp2p.multiaddr")
local upgrader = require("lua_libp2p.network.upgrader")
local tcp = require("lua_libp2p.transport.tcp")

local ok_socket, socket = pcall(require, "socket")

local M = {}

local function list_copy(values)
  local out = {}
  for i, v in ipairs(values or {}) do
    out[i] = v
  end
  return out
end

local Host = {}
Host.__index = Host

local function sleep_seconds(seconds)
  if ok_socket and type(socket.sleep) == "function" then
    socket.sleep(seconds)
  end
end

local function extract_peer_id_from_multiaddr(addr)
  local parsed = multiaddr.parse(addr)
  if not parsed then
    return nil
  end
  for _, c in ipairs(parsed.components) do
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

  local self_obj = setmetatable({
    identity = keypair,
    listen_addrs = list_copy(cfg.listen_addrs or cfg.listenAddrs or {}),
    transports = list_copy(cfg.transports or { "tcp" }),
    security_transports = list_copy(cfg.security_transports or cfg.securityTransports or {}),
    muxers = list_copy(cfg.muxers or {}),
    _handlers = {},
    _listeners = {},
    _connections = {},
    _connections_by_peer = {},
    _running = false,
    _connect_timeout = cfg.connect_timeout or cfg.connectTimeout or 2,
    _io_timeout = cfg.io_timeout or cfg.ioTimeout or 2,
    _accept_timeout = cfg.accept_timeout or cfg.acceptTimeout or 0,
  }, self)

  if #self_obj.security_transports == 0 then
    self_obj.security_transports = { "/plaintext/2.0.0" }
  end
  if #self_obj.muxers == 0 then
    self_obj.muxers = { "/yamux/1.0.0" }
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

function Host:listen(addrs)
  local targets = addrs and list_copy(addrs) or self.listen_addrs
  if #targets == 0 then
    targets = { "/ip4/127.0.0.1/tcp/0" }
  end

  for _, listener in ipairs(self._listeners) do
    listener:close()
  end
  self._listeners = {}
  self.listen_addrs = {}

  for _, addr in ipairs(targets) do
    local listener, listen_err = tcp.listen({
      multiaddr = addr,
      accept_timeout = self._accept_timeout,
      io_timeout = self._io_timeout,
    })
    if not listener then
      return nil, listen_err
    end
    self._listeners[#self._listeners + 1] = listener

    local resolved, resolved_err = listener:multiaddr()
    if not resolved then
      return nil, resolved_err
    end
    self.listen_addrs[#self.listen_addrs + 1] = resolved
  end

  return self.listen_addrs
end

function Host:get_listen_addrs()
  return list_copy(self.listen_addrs)
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
  for _, listener in ipairs(self._listeners) do
    local raw_conn, accept_err = listener:accept(timeout or self._accept_timeout)
    if raw_conn then
      local conn, state, up_err = upgrader.upgrade_inbound(raw_conn, {
        local_keypair = self.identity,
        security_protocols = self.security_transports,
        muxer_protocols = self.muxers,
      })
      if not conn then
        raw_conn:close()
        return nil, up_err
      end
      self:_register_connection(conn, state)
      break
    elseif accept_err and error_mod.is_error(accept_err) and accept_err.kind ~= "timeout" then
      return nil, accept_err
    end
  end

  local router, router_err = self:_build_router()
  if not router then
    return nil, router_err
  end

  for _, entry in ipairs(self._connections) do
    local conn = entry.conn
    local _, process_err = conn:process_one()
    if process_err and error_mod.is_error(process_err) and process_err.kind ~= "timeout" and process_err.kind ~= "closed" then
      return nil, process_err
    end

    local stream, protocol_id, handler, stream_err = conn:accept_stream(router)
    if stream_err and error_mod.is_error(stream_err) and stream_err.kind ~= "timeout" then
      return nil, stream_err
    end
    if stream and handler then
      local ok, handler_err = handler(stream, {
        host = self,
        connection = conn,
        state = entry.state,
        protocol = protocol_id,
      })
      if ok == nil and handler_err then
        return nil, handler_err
      end
      return {
        protocol = protocol_id,
        connection = conn,
      }
    end
  end

  return true
end

function Host:start(opts)
  if self._running then
    return true
  end

  local options = opts or {}
  if #self._listeners == 0 then
    local _, listen_err = self:listen(options.listen_addrs)
    if not self._listeners[1] then
      return nil, listen_err
    end
  end

  self._running = true

  if options.blocking then
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
      if poll_interval > 0 then
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
