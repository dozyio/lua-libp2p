local tcp = require("lua_libp2p.transport.tcp")
local error_mod = require("lua_libp2p.error")
local multiaddr = require("lua_libp2p.multiaddr")

local ok_luv, uv = pcall(require, "luv")

local function env_truthy(value)
  if value == nil then
    return false
  end
  local normalized = string.lower(tostring(value))
  return normalized == "1" or normalized == "true" or normalized == "yes" or normalized == "on"
end

local native_opt_in = env_truthy(os.getenv("LUA_LIBP2P_TCP_LUV_NATIVE"))

local M = {}

M.BACKEND = (ok_luv and native_opt_in) and "luv-native" or "luv-proxy"
M.IS_NATIVE = ok_luv
M.USE_NATIVE = ok_luv and native_opt_in

local wrap

local Wrapped = {}
Wrapped.__index = function(self, key)
  local own = Wrapped[key]
  if own ~= nil then
    return own
  end

  local inner = self._inner
  local value = inner and inner[key]
  if type(value) == "function" then
    return function(_, ...)
      return value(inner, ...)
    end
  end
  return value
end

local function socket_fd(sock)
  if not sock or type(sock.getfd) ~= "function" then
    return nil
  end
  local ok, fd = pcall(sock.getfd, sock)
  if not ok or type(fd) ~= "number" or fd < 0 then
    return nil
  end
  return fd
end

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

local function parse_multiaddr(addr_text)
  local parsed, parse_err = multiaddr.parse(addr_text)
  if not parsed then
    return nil, parse_err
  end
  return multiaddr.to_tcp_endpoint(parsed)
end

local function format_multiaddr(host, port)
  local host_protocol = parse_ipv4(host) and "ip4" or "dns"
  return multiaddr.format({
    components = {
      { protocol = host_protocol, value = tostring(host) },
      { protocol = "tcp", value = tostring(port) },
    },
  })
end

local function build_timeout_timer(seconds, on_timeout)
  if type(seconds) ~= "number" or seconds <= 0 then
    return nil
  end

  local timer = uv.new_timer()
  timer:start(math.floor(seconds * 1000), 0, on_timeout)
  return timer
end

local function uv_handle_fd(handle)
  if not handle or type(handle.fileno) ~= "function" then
    return nil
  end
  local ok, fd = pcall(handle.fileno, handle)
  if not ok or type(fd) ~= "number" or fd < 0 then
    return nil
  end
  return fd
end

local function classify_uv_error(err, io_message)
  if type(err) == "table" and err.kind then
    return err
  end

  local text = tostring(err or "")
  local upper = string.upper(text)
  if upper == "CLOSED" or upper == "ECONNRESET" or upper == "EPIPE" or upper == "ENOTCONN" or upper == "EOF" then
    return error_mod.new("closed", "connection closed", { cause = err })
  end
  if upper == "ETIMEDOUT" or string.find(upper, "TIMEDOUT", 1, true) ~= nil then
    return error_mod.new("timeout", "operation timed out", { cause = err })
  end
  return error_mod.new("io", io_message or "native luv io failed", { cause = err })
end

local NativeConnection = {}
NativeConnection.__index = NativeConnection

function NativeConnection:new(raw, opts)
  local options = opts or {}
  return setmetatable({
    _raw = raw,
    _closed = false,
    _pending = "",
    _peer_host = options.peer_host,
    _peer_port = options.peer_port,
    _local_host = options.local_host,
    _local_port = options.local_port,
    _io_timeout = options.io_timeout,
  }, self)
end

function NativeConnection:set_timeout(seconds)
  if self._closed then
    return nil, error_mod.new("closed", "connection already closed")
  end
  if seconds ~= nil and type(seconds) ~= "number" then
    return nil, error_mod.new("input", "timeout must be a number or nil")
  end
  self._io_timeout = seconds
  return true
end

function NativeConnection:socket()
  return self._raw
end

function NativeConnection:watch_luv_readable(on_readable)
  if type(on_readable) ~= "function" then
    return nil, error_mod.new("input", "on_readable callback is required")
  end
  local stop_flag = false
  local fd = uv_handle_fd(self._raw)
  if fd == nil then
    return nil, error_mod.new("io", "failed resolving native connection fd")
  end

  local poll = assert(uv.new_poll(fd))
  local ok, err = poll:start("r", function(cb_err)
    if cb_err or stop_flag then
      return
    end
    on_readable()
  end)
  if not ok then
    poll:close()
    return nil, error_mod.new("io", "failed starting native luv poll", { cause = err })
  end
  return function()
    if stop_flag then
      return true
    end
    stop_flag = true
    pcall(function()
      poll:stop()
      poll:close()
    end)
    return true
  end
end

function NativeConnection:write(payload)
  if self._closed then
    return nil, error_mod.new("closed", "write on closed connection")
  end
  if type(payload) ~= "string" then
    return nil, error_mod.new("input", "payload must be a string")
  end

  local done = false
  local write_err = nil
  local timer = build_timeout_timer(self._io_timeout, function()
    if done then
      return
    end
    done = true
    write_err = error_mod.new("timeout", "write timed out")
  end)

  self._raw:write(payload, function(err)
    if done then
      return
    end
    write_err = err
    done = true
  end)

  while not done do
    uv.run("once")
  end

  if timer then
    timer:stop()
    timer:close()
  end

  if write_err then
    if type(write_err) == "table" and write_err.kind then
      return nil, write_err
    end
    local mapped = classify_uv_error(write_err, "native luv write failed")
    if mapped.kind == "closed" then
      self._closed = true
    end
    return nil, mapped
  end
  return true
end

function NativeConnection:read(length)
  if self._closed then
    return nil, error_mod.new("closed", "read on closed connection")
  end
  if type(length) ~= "number" or length <= 0 then
    return nil, error_mod.new("input", "read length must be a positive number")
  end

  if #self._pending >= length then
    local out = self._pending:sub(1, length)
    self._pending = self._pending:sub(length + 1)
    return out
  end

  local done = false
  local read_err = nil
  local read_chunk = nil
  local timer = build_timeout_timer(self._io_timeout, function()
    if done then
      return
    end
    done = true
    read_err = error_mod.new("timeout", "read timed out")
    pcall(function()
      self._raw:read_stop()
    end)
  end)

  self._raw:read_start(function(err, chunk)
    if done then
      return
    end
    if err then
      read_err = err
      done = true
      self._raw:read_stop()
      return
    end
    if chunk == nil then
      read_err = "closed"
      done = true
      self._raw:read_stop()
      return
    end
    self._pending = self._pending .. chunk
    if #self._pending >= length then
      read_chunk = self._pending:sub(1, length)
      self._pending = self._pending:sub(length + 1)
      done = true
      self._raw:read_stop()
    end
  end)

  while not done do
    uv.run("once")
  end

  if timer then
    timer:stop()
    timer:close()
  end

  if read_err then
    if type(read_err) == "table" and read_err.kind then
      return nil, read_err
    end
    if read_err == "closed" then
      self._closed = true
      return nil, error_mod.new("closed", "connection closed during read")
    end
    local mapped = classify_uv_error(read_err, "native luv read failed")
    if mapped.kind == "closed" then
      self._closed = true
    end
    return nil, mapped
  end

  return read_chunk
end

function NativeConnection:local_multiaddr()
  return format_multiaddr(self._local_host, self._local_port)
end

function NativeConnection:remote_multiaddr()
  return format_multiaddr(self._peer_host, self._peer_port)
end

function NativeConnection:close()
  if self._closed then
    return true
  end
  self._closed = true
  pcall(function()
    self._raw:shutdown(function() end)
  end)
  pcall(function()
    self._raw:close()
  end)
  return true
end

local NativeListener = {}
NativeListener.__index = NativeListener

function NativeListener:new(server, opts)
  local options = opts or {}
  return setmetatable({
    _server = server,
    _closed = false,
    _pending = {},
    _listen_host = options.listen_host,
    _listen_port = options.listen_port,
    _io_timeout = options.io_timeout,
    _default_accept_timeout = options.accept_timeout,
    _watch_callbacks = {},
    _next_watch_id = 1,
  }, self)
end

function NativeListener:socket()
  return self._server
end

function NativeListener:watch_luv_readable(on_readable)
  if type(on_readable) ~= "function" then
    return nil, error_mod.new("input", "on_readable callback is required")
  end

  local watch_id = self._next_watch_id
  self._next_watch_id = watch_id + 1
  self._watch_callbacks[watch_id] = on_readable

  return function()
    self._watch_callbacks[watch_id] = nil
    return true
  end
end

function NativeListener:accept(timeout)
  if self._closed then
    return nil, error_mod.new("closed", "accept on closed listener")
  end
  if #self._pending > 0 then
    local conn = table.remove(self._pending, 1)
    return conn
  end

  local accept_timeout = timeout
  if accept_timeout == nil then
    accept_timeout = self._default_accept_timeout
  end

  if accept_timeout == 0 then
    if #self._pending > 0 then
      local conn = table.remove(self._pending, 1)
      return conn
    end
    return nil, error_mod.new("timeout", "accept timed out")
  end

  local done = false
  local out_conn = nil
  local acc_err = nil
  local timer = build_timeout_timer(accept_timeout, function()
      if not done then
        done = true
        acc_err = error_mod.new("timeout", "accept timed out")
      end
    end)

  while not done do
    uv.run("once")
    if #self._pending > 0 then
      out_conn = table.remove(self._pending, 1)
      done = true
    end
  end

  if timer then
    timer:stop()
    timer:close()
  end

  if out_conn then
    return out_conn
  end
  return nil, acc_err
end

function NativeListener:multiaddr()
  return format_multiaddr(self._listen_host, self._listen_port)
end

function NativeListener:close()
  if self._closed then
    return true
  end
  self._closed = true
  pcall(function()
    self._server:close()
  end)
  self._pending = {}
  return true
end

function Wrapped:watch_luv_readable(on_readable)
  if type(self._inner.watch_luv_readable) == "function" then
    local unwatch, inner_err = self._inner:watch_luv_readable(on_readable)
    if type(unwatch) ~= "function" then
      return nil, inner_err
    end
    self._unwatchers[#self._unwatchers + 1] = unwatch
    return unwatch
  end

  if not ok_luv then
    return nil, error_mod.new("unsupported", "luv not available for tcp_luv watcher")
  end
  if type(on_readable) ~= "function" then
    return nil, error_mod.new("input", "on_readable callback is required")
  end

  local sock = nil
  if type(self._inner.socket) == "function" then
    sock = self._inner:socket()
  end
  local fd = socket_fd(sock)
  if fd == nil then
    return nil, error_mod.new("io", "tcp_luv wrapper could not resolve socket fd")
  end

  local handle, poll_err = uv.new_poll(fd)
  if not handle then
    return nil, error_mod.new("io", "failed creating luv poll handle", { cause = poll_err })
  end

  local ok, start_err = handle:start("r", function(err)
    if err then
      return
    end
    on_readable()
  end)
  if not ok then
    pcall(function()
      handle:close()
    end)
    return nil, error_mod.new("io", "failed starting luv poll handle", { cause = start_err })
  end

  local active = true
  local function unwatch()
    if not active then
      return true
    end
    active = false
    pcall(function()
      handle:stop()
      handle:close()
    end)
    return true
  end

  self._unwatchers[#self._unwatchers + 1] = unwatch
  return unwatch
end

function Wrapped:close()
  for _, unwatch in ipairs(self._unwatchers) do
    pcall(unwatch)
  end
  self._unwatchers = {}
  return self._inner:close()
end

function Wrapped:accept(timeout)
  if type(self._inner.accept) ~= "function" then
    return nil, error_mod.new("input", "accept is only supported on listeners")
  end
  local conn, err = self._inner:accept(timeout)
  if not conn then
    return nil, err
  end
  return wrap(conn)
end

function Wrapped:socket()
  if type(self._inner.socket) == "function" then
    return self._inner:socket()
  end
  return nil
end

wrap = function(inner)
  if type(inner) ~= "table" then
    return inner
  end
  if inner._is_tcp_luv_wrapped then
    return inner
  end
  return setmetatable({
    _inner = inner,
    _unwatchers = {},
    _is_tcp_luv_wrapped = true,
  }, Wrapped)
end

function M.listen(target, opts)
  if ok_luv and M.USE_NATIVE then
    local options = {}
    if type(target) == "table" then
      for k, v in pairs(target) do
        options[k] = v
      end
    elseif type(target) == "string" then
      options.multiaddr = target
    end
    if type(opts) == "table" then
      for k, v in pairs(opts) do
        options[k] = v
      end
    end

    local host = options.host
    local port = options.port
    if options.multiaddr then
      local parsed, parse_err = parse_multiaddr(options.multiaddr)
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

    local server = uv.new_tcp()
    local ok, bind_err = server:bind(normalized_host, port)
    if not ok then
      return nil, error_mod.new("io", "failed binding native luv listener", { cause = bind_err })
    end

    local sockname = server:getsockname()
    local listener = NativeListener:new(server, {
      listen_host = sockname and sockname.ip or normalized_host,
      listen_port = sockname and sockname.port or port,
      io_timeout = options.io_timeout,
      accept_timeout = options.accept_timeout,
    })

    server:listen(128, function(err)
      if err then
        return
      end
      local client = uv.new_tcp()
      local accepted, acc_err = server:accept(client)
      if not accepted then
        client:close()
        return
      end
      local local_name = client:getsockname() or {}
      local peer_name = client:getpeername() or {}
      listener._pending[#listener._pending + 1] = NativeConnection:new(client, {
        local_host = local_name.ip or normalized_host,
        local_port = local_name.port or port,
        peer_host = peer_name.ip or "127.0.0.1",
        peer_port = peer_name.port or 0,
        io_timeout = listener._io_timeout,
      })
      for _, cb in pairs(listener._watch_callbacks) do
        cb()
      end
    end)

    return wrap(listener)
  end

  local inner, err = tcp.listen(target, opts)
  if not inner then
    return nil, err
  end
  return wrap(inner)
end

function M.dial(target, opts)
  if ok_luv and M.USE_NATIVE then
    local options = opts or {}
    local host
    local port

    if type(target) == "string" then
      if target:sub(1, 1) == "/" then
        local parsed, parse_err = parse_multiaddr(target)
        if not parsed then
          return nil, parse_err
        end
        host = parsed.host
        port = parsed.port
      else
        host = target
        port = options.port
      end
    elseif type(target) == "table" then
      host = target.host
      port = target.port
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

    local conn = uv.new_tcp()
    local done = false
    local dial_err = nil
    conn:connect(dial_host, port, function(err)
      dial_err = err
      done = true
    end)

    local timeout = options.connect_timeout
    if timeout == nil then
      timeout = options.timeout
    end
    local timer = nil
    if type(timeout) == "number" and timeout > 0 then
      timer = uv.new_timer()
      timer:start(math.floor(timeout * 1000), 0, function()
        if not done then
          done = true
          dial_err = error_mod.new("timeout", "tcp connect timed out")
        end
      end)
    end

    while not done do
      uv.run("once")
    end

    if timer then
      timer:stop()
      timer:close()
    end

    if dial_err then
      conn:close()
      if type(dial_err) == "table" and dial_err.kind then
        return nil, dial_err
      end
      return nil, classify_uv_error(dial_err, "native luv tcp connect failed")
    end

    local local_name = conn:getsockname() or {}
    local peer_name = conn:getpeername() or {}
    return wrap(NativeConnection:new(conn, {
      local_host = local_name.ip or "127.0.0.1",
      local_port = local_name.port or 0,
      peer_host = peer_name.ip or dial_host,
      peer_port = peer_name.port or port,
      io_timeout = options.io_timeout,
    }))
  end

  local inner, err = tcp.dial(target, opts)
  if not inner then
    return nil, err
  end
  return wrap(inner)
end

return M
