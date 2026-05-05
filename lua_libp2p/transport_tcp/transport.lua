--- Poll-based TCP transport implementation.
-- @module lua_libp2p.transport_tcp.transport
local socket = require("socket")
local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log").subsystem("tcp")
local multiaddr = require("lua_libp2p.multiaddr")

local M = {}

local function parse_ipv4(host)
  if type(host) ~= "string" then
    return nil
  end

  local a, b, c, d = host:match("^(%d+)%.(%d+)%.(%d+)%.(%d+)$")
  if not a then
    return nil
  end

  local parts = { tonumber(a), tonumber(b), tonumber(c), tonumber(d) }
  for i = 1, 4 do
    if parts[i] < 0 or parts[i] > 255 then
      return nil
    end
  end

  return string.format("%d.%d.%d.%d", parts[1], parts[2], parts[3], parts[4])
end

local function parse_ipv6(host)
  if type(host) ~= "string" then
    return nil
  end
  host = host:gsub("^%[", ""):gsub("%]$", "")
  host = host:gsub("%%.+$", "")
  if host:find(":", 1, true) == nil then
    return nil
  end
  if host:match("[^0-9a-fA-F:%.]") then
    return nil
  end
  return host
end

--- Parse `/ip*/.../tcp/...` multiaddr into endpoint table.
function M.parse_multiaddr(addr_text)
  local parsed, parse_err = multiaddr.parse(addr_text)
  if not parsed then
    return nil, parse_err
  end
  if #parsed.components ~= 2 then
    return nil, error_mod.new("input", "tcp transport expects /<host>/tcp/<port> multiaddr")
  end
  return multiaddr.to_tcp_endpoint(parsed)
end

--- Format host+port into TCP multiaddr.
function M.format_multiaddr(host, port)
  local normalized_port = tonumber(port)
  if not normalized_port or normalized_port < 0 or normalized_port > 65535 then
    return nil, error_mod.new("input", "invalid tcp port", { port = port })
  end

  local host_protocol = "dns"
  if parse_ipv4(host) then
    host_protocol = "ip4"
  elseif parse_ipv6(host) then
    host_protocol = "ip6"
  end
  return multiaddr.format({
    components = {
      { protocol = host_protocol, value = tostring(host) },
      { protocol = "tcp", value = tostring(normalized_port) },
    },
  })
end

local Connection = {}
Connection.__index = Connection

local function wrap_socket_error(kind, message, cause, context)
  local merged = context or {}
  merged.cause = cause
  return error_mod.new(kind, message, merged)
end

function Connection:new(sock, opts)
  local options = opts or {}
  return setmetatable({
    _socket = sock,
    _closed = false,
    _direction = options.direction,
    _peer_host = options.peer_host,
    _peer_port = options.peer_port,
  }, self)
end

function Connection:set_timeout(seconds)
  if self._closed then
    return nil, error_mod.new("closed", "connection already closed")
  end
  local ok, err = self._socket:settimeout(seconds)
  if not ok then
    return nil, wrap_socket_error("io", "failed setting timeout", err)
  end
  return true
end

function Connection:socket()
  return self._socket
end

function Connection:write(payload)
  if self._closed then
    return nil, error_mod.new("closed", "write on closed connection")
  end
  if type(payload) ~= "string" then
    return nil, error_mod.new("input", "payload must be a string")
  end

  local sent, err, partial = self._socket:send(payload)
  if sent then
    return true
  end

  if err == "timeout" then
    return nil, wrap_socket_error("timeout", "write timed out", err, { partial = partial or 0 })
  end
  if err == "closed" then
    self._closed = true
    log.debug("tcp connection disconnected", {
      direction = self._direction,
      peer_host = self._peer_host,
      peer_port = self._peer_port,
      operation = "write",
      cause = tostring(err),
    })
    return nil, wrap_socket_error("closed", "connection closed during write", err, { partial = partial or 0 })
  end
  return nil, wrap_socket_error("io", "write failed", err, { partial = partial or 0 })
end

function Connection:read(length)
  if self._closed then
    return nil, error_mod.new("closed", "read on closed connection")
  end
  if type(length) ~= "number" or length <= 0 then
    return nil, error_mod.new("input", "read length must be a positive number")
  end

  local data, err, partial = self._socket:receive(length)
  if data then
    return data
  end

  if err == "timeout" then
    return nil, wrap_socket_error("timeout", "read timed out", err, { partial = partial or "" })
  end
  if err == "closed" then
    self._closed = true
    log.debug("tcp connection disconnected", {
      direction = self._direction,
      peer_host = self._peer_host,
      peer_port = self._peer_port,
      operation = "read",
      cause = tostring(err),
    })
    return nil, wrap_socket_error("closed", "connection closed during read", err, { partial = partial or "" })
  end
  return nil, wrap_socket_error("io", "read failed", err, { partial = partial or "" })
end

function Connection:local_multiaddr()
  local host, port = self._socket:getsockname()
  if not host or not port then
    return nil, error_mod.new("io", "failed getting local socket address")
  end
  return M.format_multiaddr(host, port)
end

function Connection:remote_multiaddr()
  local host, port = self._socket:getpeername()
  if not host or not port then
    return nil, error_mod.new("io", "failed getting peer socket address")
  end
  return M.format_multiaddr(host, port)
end

function Connection:close()
  if self._closed then
    return true
  end
  local ok, err = self._socket:close()
  self._closed = true
  if ok == nil and err then
    log.debug("tcp connection close failed", {
      direction = self._direction,
      peer_host = self._peer_host,
      peer_port = self._peer_port,
      cause = tostring(err),
    })
    return nil, wrap_socket_error("io", "close failed", err)
  end
  log.debug("tcp connection closed", {
    direction = self._direction,
    peer_host = self._peer_host,
    peer_port = self._peer_port,
  })
  return true
end

local Listener = {}
Listener.__index = Listener

--- Construct listener wrapper.
-- `opts.io_timeout` applies to accepted sockets.
-- `opts.accept_timeout` is default timeout for `accept()`.
function Listener:new(server, opts)
  return setmetatable({
    _server = server,
    _closed = false,
    _listen_host = opts.listen_host,
    _listen_port = opts.listen_port,
    _io_timeout = opts.io_timeout,
    _default_accept_timeout = opts.accept_timeout,
  }, self)
end

function Listener:accept(timeout)
  if self._closed then
    return nil, error_mod.new("closed", "accept on closed listener")
  end

  local accept_timeout = timeout
  if accept_timeout == nil then
    accept_timeout = self._default_accept_timeout
  end

  local ok, set_err = self._server:settimeout(accept_timeout)
  if not ok then
    return nil, wrap_socket_error("io", "failed setting accept timeout", set_err)
  end

  local client, err = self._server:accept()
  if not client then
    if err == "timeout" then
      return nil, wrap_socket_error("timeout", "accept timed out", err)
    end
    log.debug("tcp accept failed", {
      listen_host = self._listen_host,
      listen_port = self._listen_port,
      cause = tostring(err),
    })
    return nil, wrap_socket_error("io", "accept failed", err)
  end

  client:settimeout(self._io_timeout)
  local peer_host, peer_port = client:getpeername()
  log.debug("tcp connection accepted", {
    listen_host = self._listen_host,
    listen_port = self._listen_port,
    peer_host = peer_host,
    peer_port = peer_port,
  })
  return Connection:new(client, {
    direction = "inbound",
    peer_host = peer_host,
    peer_port = peer_port,
  })
end

function Listener:socket()
  return self._server
end

function Listener:multiaddr()
  if self._closed then
    return nil, error_mod.new("closed", "listener already closed")
  end
  local host, port = self._server:getsockname()
  if not host or not port then
    return nil, error_mod.new("io", "failed getting listener socket address")
  end
  return M.format_multiaddr(host, port)
end

function Listener:close()
  if self._closed then
    return true
  end
  local ok, err = self._server:close()
  self._closed = true
  if ok == nil and err then
    log.debug("tcp listener close failed", {
      listen_host = self._listen_host,
      listen_port = self._listen_port,
      cause = tostring(err),
    })
    return nil, wrap_socket_error("io", "listener close failed", err)
  end
  log.debug("tcp listener closed", {
    listen_host = self._listen_host,
    listen_port = self._listen_port,
  })
  return true
end

--- Start a TCP listener.
-- `opts` accepts either `multiaddr` or (`host`,`port`), plus
-- `accept_timeout` and `io_timeout`.
-- @tparam[opt] table opts
-- @treturn table|nil listener
-- @treturn[opt] table err
function M.listen(opts)
  local options = opts or {}

  local host = options.host
  local port = options.port
  if options.multiaddr then
    local parsed, parse_err = M.parse_multiaddr(options.multiaddr)
    if not parsed then
      return nil, parse_err
    end
    host = parsed.host
    port = parsed.port
  end

  if host == nil then
    host = "127.0.0.1"
  end
  if port == nil then
    port = 0
  end

  local normalized_host = parse_ipv4(host) or parse_ipv6(host)
  if not normalized_host then
    return nil, error_mod.new("input", "invalid tcp listen host", { host = host })
  end
  if type(port) ~= "number" or port < 0 or port > 65535 then
    return nil, error_mod.new("input", "invalid listen port", { port = port })
  end

  log.debug("tcp listen begin", {
    host = normalized_host,
    port = port,
  })
  local server, err = socket.bind(normalized_host, port)
  if not server then
    log.debug("tcp listen failed", {
      host = normalized_host,
      port = port,
      cause = tostring(err),
    })
    return nil, wrap_socket_error("io", "failed binding tcp listener", err)
  end

  local listen_host, listen_port = server:getsockname()
  log.debug("tcp listen active", {
    host = listen_host or normalized_host,
    port = listen_port or port,
  })

  return Listener:new(server, {
    listen_host = listen_host or normalized_host,
    listen_port = listen_port or port,
    io_timeout = options.io_timeout,
    accept_timeout = options.accept_timeout,
  })
end

--- Dial a TCP endpoint.
-- `opts` accepts `timeout`, `io_timeout`, and `ctx`.
-- @param address Host string, multiaddr string, or `{ host, port }`.
-- @tparam[opt] table opts
-- @treturn table|nil conn
-- @treturn[opt] table err
function M.dial(address, opts)
  local options = opts or {}
  local host
  local port

  if type(address) == "string" then
    if address:sub(1, 1) == "/" then
      local parsed, parse_err = M.parse_multiaddr(address)
      if not parsed then
        return nil, parse_err
      end
      host = parsed.host
      port = parsed.port
    else
      host = address
      port = options.port
    end
  elseif type(address) == "table" then
    host = address.host
    port = address.port
  else
    return nil, error_mod.new("input", "address must be multiaddr string or table")
  end

  local normalized_host = parse_ipv4(host)
  local dial_host = normalized_host or host
  if type(dial_host) ~= "string" or dial_host == "" then
    return nil, error_mod.new("input", "invalid dial host", { host = host })
  end
  if type(port) ~= "number" or port <= 0 or port > 65535 then
    return nil, error_mod.new("input", "invalid dial port", { port = port })
  end

  log.debug("tcp dial begin", {
    host = dial_host,
    port = port,
  })
  local conn, tcp_err = socket.tcp()
  if not conn then
    log.debug("tcp dial failed", {
      host = dial_host,
      port = port,
      cause = tostring(tcp_err),
    })
    return nil, wrap_socket_error("io", "failed creating tcp socket", tcp_err)
  end

  local connect_timeout = options.connect_timeout
  if connect_timeout == nil then
    connect_timeout = options.timeout
  end
  if connect_timeout ~= nil then
    conn:settimeout(connect_timeout)
  end

  local ok, err = conn:connect(dial_host, port)
  if not ok then
    conn:close()
    log.debug("tcp dial failed", {
      host = dial_host,
      port = port,
      cause = tostring(err),
    })
    if err == "timeout" then
      return nil, wrap_socket_error("timeout", "tcp connect timed out", err)
    end
    return nil, wrap_socket_error("io", "tcp connect failed", err)
  end

  local io_timeout = options.io_timeout
  if io_timeout == nil then
    io_timeout = options.timeout
  end
  conn:settimeout(io_timeout)

  local peer_host, peer_port = conn:getpeername()
  log.debug("tcp dial succeeded", {
    host = dial_host,
    port = port,
    peer_host = peer_host,
    peer_port = peer_port,
  })
  return Connection:new(conn, {
    direction = "outbound",
    peer_host = peer_host or dial_host,
    peer_port = peer_port or port,
  })
end

return M
