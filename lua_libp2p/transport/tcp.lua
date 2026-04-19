local socket = require("socket")
local error_mod = require("lua_libp2p.error")
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

function M.format_multiaddr(host, port)
  local normalized_port = tonumber(port)
  if not normalized_port or normalized_port < 0 or normalized_port > 65535 then
    return nil, error_mod.new("input", "invalid tcp port", { port = port })
  end

  local host_protocol = parse_ipv4(host) and "ip4" or "dns"
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
  return setmetatable({
    _socket = sock,
    _closed = false,
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
    return nil, wrap_socket_error("io", "close failed", err)
  end
  return true
end

local Listener = {}
Listener.__index = Listener

function Listener:new(server, opts)
  return setmetatable({
    _server = server,
    _closed = false,
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
    return nil, wrap_socket_error("io", "accept failed", err)
  end

  client:settimeout(self._io_timeout)
  return Connection:new(client)
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
    return nil, wrap_socket_error("io", "listener close failed", err)
  end
  return true
end

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

  local normalized_host = parse_ipv4(host)
  if not normalized_host then
    return nil, error_mod.new("input", "invalid ip4 listen host", { host = host })
  end
  if type(port) ~= "number" or port < 0 or port > 65535 then
    return nil, error_mod.new("input", "invalid listen port", { port = port })
  end

  local server, err = socket.bind(normalized_host, port)
  if not server then
    return nil, wrap_socket_error("io", "failed binding tcp listener", err)
  end

  return Listener:new(server, {
    io_timeout = options.io_timeout,
    accept_timeout = options.accept_timeout,
  })
end

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

  local conn, tcp_err = socket.tcp()
  if not conn then
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

  return Connection:new(conn)
end

return M
