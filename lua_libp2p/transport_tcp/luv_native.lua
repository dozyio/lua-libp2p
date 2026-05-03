--- Native luv TCP transport implementation.
-- @module lua_libp2p.transport_tcp.luv_native
local error_mod = require("lua_libp2p.error")
local multiaddr = require("lua_libp2p.multiaddr")

local ok_luv, uv = pcall(require, "luv")
local ok_socket, socket = pcall(require, "socket")

local M = {
  AVAILABLE = ok_luv,
}

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

local function resolve_dial_host(host)
  local ip = parse_ipv4(host)
  if ip then
    return ip
  end
  local ip6 = parse_ipv6(host)
  if ip6 then
    return ip6
  end
  if not ok_socket or not socket or not socket.dns or type(socket.dns.toip) ~= "function" then
    return nil, error_mod.new("unsupported", "native luv tcp dial requires DNS resolver for hostnames", { host = host })
  end
  local resolved, resolve_err = socket.dns.toip(host)
  if type(resolved) ~= "string" or resolved == "" then
    return nil, error_mod.new("io", "dns lookup failed", { host = host, cause = resolve_err })
  end
  return resolved
end

local function parse_multiaddr(addr_text)
  local parsed, parse_err = multiaddr.parse(addr_text)
  if not parsed then
    return nil, parse_err
  end
  return multiaddr.to_tcp_endpoint(parsed)
end

local function format_multiaddr(host, port)
  local host_protocol = "dns"
  if parse_ipv4(host) then
    host_protocol = "ip4"
  elseif parse_ipv6(host) then
    host_protocol = "ip6"
  end
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

local function close_timer(timer)
  if timer then
    pcall(function()
      timer:stop()
      timer:close()
    end)
  end
end

local function classify_uv_error(err, io_message)
  if type(err) == "table" and err.kind then
    return err
  end
  local upper = string.upper(tostring(err or ""))
  if upper == "CLOSED"
    or upper == "ECONNRESET"
    or upper == "EPIPE"
    or upper == "ENOTCONN"
    or upper == "EOF"
    or string.find(upper, "ECONNRESET", 1, true) ~= nil
    or string.find(upper, "EPIPE", 1, true) ~= nil
    or string.find(upper, "ENOTCONN", 1, true) ~= nil
    or string.find(upper, "CLOSED", 1, true) ~= nil
    or string.find(upper, "EOF", 1, true) ~= nil
  then
    return error_mod.new("closed", "connection closed", { cause = err })
  end
  if upper == "ETIMEDOUT" or string.find(upper, "TIMEDOUT", 1, true) ~= nil then
    return error_mod.new("timeout", "operation timed out", { cause = err })
  end
  return error_mod.new("io", io_message or "native luv io failed", { cause = err })
end

local function run_once_or_loop_running()
  if type(coroutine.isyieldable) == "function" and coroutine.isyieldable() then
    return nil, "loop_running"
  end
  local called, ok, err = pcall(function()
    return uv.run("once")
  end)
  if not called then
    local msg = tostring(ok or "")
    if string.find(msg, "loop already running", 1, true) ~= nil or string.find(msg, "cannot resume non%-suspended coroutine") ~= nil then
      return nil, "loop_running"
    end
    return nil, ok
  end
  if ok == nil and string.find(tostring(err or ""), "loop already running", 1, true) ~= nil then
    return nil, "loop_running"
  end
  return ok, err
end

local function enable_tcp_nodelay(tcp, enabled)
  if enabled ~= false and tcp and type(tcp.nodelay) == "function" then
    pcall(tcp.nodelay, tcp, true)
  end
end

local function enable_tcp_keepalive(tcp, enabled, initial_delay)
  if enabled ~= false and tcp and type(tcp.keepalive) == "function" then
    pcall(tcp.keepalive, tcp, true, initial_delay or 0)
  end
end

local function configure_tcp_socket(tcp, opts)
  local options = opts or {}
  enable_tcp_nodelay(tcp, options.nodelay)
  enable_tcp_keepalive(tcp, options.keepalive, options.keepalive_initial_delay)
end

local function yield_if_possible(reason, ctx)
  if ctx then
    if reason and reason.type == "read" and type(ctx.wait_read) == "function" then
      ctx:wait_read(reason.connection)
      return true
    end
    if reason and reason.type == "dial" and type(ctx.wait_dial) == "function" then
      ctx:wait_dial(reason.connection)
      return true
    end
    if reason and reason.type == "write" and type(ctx.wait_write) == "function" then
      ctx:wait_write(reason.connection)
      return true
    end
  end
  -- Under runtime=luv the host already owns uv.run. Yield lets the host resume
  -- the coroutine on a later tick instead of re-entering the running loop.
  if type(coroutine.isyieldable) == "function" and coroutine.isyieldable() then
    coroutine.yield(reason)
    return true
  end
  return false
end

local NativeConnection = {}
NativeConnection.__index = NativeConnection

--- Wrap raw luv tcp socket as native connection.
-- `opts.local_host`, `opts.local_port`, `opts.peer_host`, `opts.peer_port` set address metadata.
-- `opts.io_timeout` sets per-read/write timeout.
function NativeConnection:new(raw, opts)
  local options = opts or {}
  local conn = setmetatable({
    _raw = raw,
    _closed = false,
    _pending = "",
    _read_started = false,
    _read_error = nil,
    _write_error = nil,
    _tx_active = false,
    _tx_consumed = "",
    _watch_callbacks = {},
    _write_watch_callbacks = {},
    _active_timers = {},
    _next_watch_id = 1,
    _peer_host = options.peer_host,
    _peer_port = options.peer_port,
    _local_host = options.local_host,
    _local_port = options.local_port,
    _io_timeout = options.io_timeout,
    _ctx = options.ctx,
  }, self)
  conn:_ensure_read_pump()
  return conn
end

function NativeConnection:_ensure_read_pump()
  if self._closed or self._read_started then
    return
  end
  if type(self._raw.read_start) ~= "function" then
    return
  end
  self._read_started = true
  local ok, start_err = self._raw:read_start(function(err, chunk)
    if self._closed then
      return
    end
    if err then
      self._read_error = err
      for _, cb in pairs(self._watch_callbacks) do
        cb()
      end
      self._read_started = false
      pcall(function()
        self._raw:read_stop()
      end)
      return
    end
    if chunk == nil then
      self._read_error = "closed"
      for _, cb in pairs(self._watch_callbacks) do
        cb()
      end
      self._read_started = false
      pcall(function()
        self._raw:read_stop()
      end)
      return
    end
    self._pending = self._pending .. chunk
    for _, cb in pairs(self._watch_callbacks) do
      cb()
    end
  end)
  if not ok then
    if start_err == nil then
      return
    end
    local start_text = string.upper(tostring(start_err or ""))
    if string.find(start_text, "EALREADY", 1, true) ~= nil
      or string.find(start_text, "ALREADY", 1, true) ~= nil
    then
      self._read_started = true
      return
    end
    self._read_started = false
    self._read_error = classify_uv_error(start_err, "failed to start native read pump")
  end
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

function NativeConnection:set_context(ctx)
  self._ctx = ctx
  return true
end

function NativeConnection:_track_timer(timer)
  if timer then
    self._active_timers[timer] = true
  end
  return timer
end

function NativeConnection:_untrack_timer(timer)
  if timer then
    self._active_timers[timer] = nil
  end
  return timer
end

function NativeConnection:_close_active_timers()
  for timer in pairs(self._active_timers) do
    close_timer(timer)
    self._active_timers[timer] = nil
  end
end

function NativeConnection:begin_read_tx()
  self._tx_active = true
  self._tx_consumed = ""
  return true
end

function NativeConnection:commit_read_tx()
  self._tx_active = false
  self._tx_consumed = ""
  return true
end

function NativeConnection:rollback_read_tx()
  if self._tx_active and #self._tx_consumed > 0 then
    self._pending = self._tx_consumed .. self._pending
  end
  self._tx_active = false
  self._tx_consumed = ""
  return true
end

function NativeConnection:socket()
  return self._raw
end

function NativeConnection:watch_luv_readable(on_readable)
  if type(on_readable) ~= "function" then
    return nil, error_mod.new("input", "on_readable callback is required")
  end
  local watch_id = self._next_watch_id
  self._next_watch_id = watch_id + 1
  self._watch_callbacks[watch_id] = on_readable
  if #self._pending > 0 or self._read_error ~= nil then
    on_readable()
  end
  return function()
    self._watch_callbacks[watch_id] = nil
    return true
  end
end

function NativeConnection:watch_luv_write(on_write)
  if type(on_write) ~= "function" then
    return nil, error_mod.new("input", "on_write callback is required")
  end
  local watch_id = self._next_watch_id
  self._next_watch_id = watch_id + 1
  self._write_watch_callbacks[watch_id] = on_write
  return function()
    self._write_watch_callbacks[watch_id] = nil
    return true
  end
end

function NativeConnection:_notify_write_watchers()
  for _, cb in pairs(self._write_watch_callbacks) do
    cb()
  end
end

function NativeConnection:write(payload)
  if self._closed then
    return nil, error_mod.new("closed", "write on closed connection")
  end
  if self._write_error ~= nil then
    local mapped_cached = classify_uv_error(self._write_error, "native luv write failed")
    self._write_error = nil
    if mapped_cached.kind == "closed" then
      self._closed = true
    end
    return nil, mapped_cached
  end
  if type(payload) ~= "string" then
    return nil, error_mod.new("input", "payload must be a string")
  end

  if type(self._raw.try_write) == "function" then
    local immediate_wrote, immediate_err = self._raw:try_write(payload)
    if type(immediate_wrote) == "number" and immediate_wrote == #payload then
      return true
    end
    if type(immediate_wrote) == "number" and immediate_wrote > 0 and immediate_wrote < #payload then
      payload = payload:sub(immediate_wrote + 1)
    end
    if immediate_err and tostring(immediate_err) ~= "EAGAIN" then
      local mapped_immediate = classify_uv_error(immediate_err, "native luv try_write failed")
      if mapped_immediate.kind == "closed" then
        self._closed = true
        return nil, mapped_immediate
      end
    end
  end

  local done = false
  local write_err = nil
  local timer = self:_track_timer(build_timeout_timer(self._io_timeout, function()
    if done then
      return
    end
    done = true
    write_err = error_mod.new("timeout", "write timed out")
    self:_notify_write_watchers()
  end))

  self._raw:write(payload, function(err)
    if done then
      return
    end
    write_err = err
    done = true
    self:_notify_write_watchers()
  end)

  while not done do
    if yield_if_possible({ type = "write", connection = self }, self._ctx) then
      goto continue_write_wait
    end
    local _, step_err = run_once_or_loop_running()
    if step_err == "loop_running" then
      if yield_if_possible({ type = "write", connection = self }, self._ctx) then
        goto continue_write_wait
      end
      -- Non-yieldable callers preserve the historical queued-write behavior.
      close_timer(timer)
      self:_untrack_timer(timer)
      return true
    end

    ::continue_write_wait::
  end

  close_timer(timer)
  self:_untrack_timer(timer)

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
    if self._tx_active then
      self._tx_consumed = self._tx_consumed .. out
    end
    return out
  end

  self:_ensure_read_pump()

  if self._read_error then
    local read_err = self._read_error
    self._read_error = nil
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

  local done = false
  local read_err = nil
  local timer = self:_track_timer(build_timeout_timer(self._io_timeout, function()
    if done then
      return
    end
    done = true
    read_err = error_mod.new("timeout", "read timed out")
    for _, cb in pairs(self._watch_callbacks) do
      cb()
    end
  end))

  while not done do
    if #self._pending >= length then
      done = true
      break
    end
    if self._read_error ~= nil then
      read_err = self._read_error
      self._read_error = nil
      done = true
      break
    end
    if yield_if_possible({ type = "read", connection = self }, self._ctx) then
      self:_ensure_read_pump()
      goto continue_read_wait
    end
    local _, step_err = run_once_or_loop_running()
    if step_err == "loop_running" then
      if yield_if_possible({ type = "read", connection = self }, self._ctx) then
        self:_ensure_read_pump()
        goto continue_read_wait
      end
      done = true
      read_err = error_mod.new("timeout", "read would block while loop is already running")
    end

    ::continue_read_wait::
  end

  close_timer(timer)
  self:_untrack_timer(timer)

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

  local read_chunk = self._pending:sub(1, length)
  self._pending = self._pending:sub(length + 1)
  if self._tx_active then
    self._tx_consumed = self._tx_consumed .. read_chunk
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
  self:_close_active_timers()
  pcall(function()
    self._raw:read_stop()
  end)
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

--- Wrap raw luv server as native listener.
-- `opts.listen_host`, `opts.listen_port` set listener multiaddr metadata.
-- `opts.io_timeout` and `opts.accept_timeout` set default timeouts.
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
    _tcp_options = options.tcp_options or {},
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
    return table.remove(self._pending, 1)
  end

  local accept_timeout = timeout
  if accept_timeout == nil then
    accept_timeout = self._default_accept_timeout
  end
  if accept_timeout == 0 then
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
    local _, step_err = run_once_or_loop_running()
    if step_err == "loop_running" then
      done = true
      acc_err = error_mod.new("timeout", "accept would block while loop is already running")
    end
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

--- Start a native luv TCP listener.
-- `target` may be multiaddr string or table `{ host, port, multiaddr }`.
-- `opts` overrides target table and supports `accept_timeout` and `io_timeout`.
-- @param[opt] target
-- @tparam[opt] table opts
-- @treturn table|nil listener
-- @treturn[opt] table err
function M.listen(target, opts)
  if not ok_luv then
    return nil, error_mod.new("unsupported", "luv is unavailable")
  end

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
  host = host or "127.0.0.1"
  port = port or 0

  local normalized_host = parse_ipv4(host) or parse_ipv6(host)
  if not normalized_host then
    return nil, error_mod.new("input", "invalid tcp listen host", { host = host })
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
    tcp_options = {
      nodelay = options.nodelay,
      keepalive = options.keepalive,
      keepalive_initial_delay = options.keepalive_initial_delay,
    },
  })

  server:listen(128, function(err)
    if err then
      return
    end
    local client = uv.new_tcp()
    local accepted = server:accept(client)
    if not accepted then
      client:close()
      return
    end
    configure_tcp_socket(client, listener._tcp_options)
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

  return listener
end

--- Dial a native luv TCP connection.
-- `opts.port` is used when target is a host string.
-- `opts.timeout`, `opts.io_timeout`, and `opts.ctx` control timing and cancellation.
-- @param target Dial target.
-- @tparam[opt] table opts
-- @treturn table|nil conn
-- @treturn[opt] table err
function M.dial(target, opts)
  if not ok_luv then
    return nil, error_mod.new("unsupported", "luv is unavailable")
  end

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

  local dial_host, resolve_err = resolve_dial_host(host)
  if not dial_host then
    return nil, resolve_err
  end
  if type(dial_host) ~= "string" or dial_host == "" then
    return nil, error_mod.new("input", "invalid dial host", { host = host })
  end
  if type(port) ~= "number" or port <= 0 or port > 65535 then
    return nil, error_mod.new("input", "invalid dial port", { port = port })
  end

  local conn = uv.new_tcp()
  local done = false
  local dial_err = nil
  local connect_watchers = {}
  local dial_wait = {
    watch_luv_connect = function(_, on_connect)
      connect_watchers[#connect_watchers + 1] = on_connect
      local active = true
      return function()
        active = false
        for i = #connect_watchers, 1, -1 do
          if connect_watchers[i] == on_connect then
            table.remove(connect_watchers, i)
            break
          end
        end
        return active
      end
    end,
  }
  local connect_ok, connect_err = pcall(function()
    conn:connect(dial_host, port, function(err)
      dial_err = err
      done = true
      for _, watcher in ipairs(connect_watchers) do
        watcher()
      end
    end)
  end)
  if not connect_ok then
    conn:close()
    return nil, classify_uv_error(connect_err, "native luv tcp connect failed")
  end

  local timeout = options.connect_timeout or options.timeout
  local timer = build_timeout_timer(timeout, function()
    if not done then
      done = true
      dial_err = error_mod.new("timeout", "tcp connect timed out")
      for _, watcher in ipairs(connect_watchers) do
        watcher()
      end
    end
  end)

  while not done do
    if yield_if_possible({ type = "dial", connection = dial_wait }, options.ctx) then
      goto continue_dial_wait
    end
    local _, step_err = run_once_or_loop_running()
    if step_err == "loop_running" then
      if yield_if_possible({ type = "dial", connection = dial_wait }, options.ctx) then
        goto continue_dial_wait
      end
      done = true
      dial_err = error_mod.new("timeout", "dial would block while loop is already running")
    end

    ::continue_dial_wait::
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

  configure_tcp_socket(conn, options)
  local local_name = conn:getsockname() or {}
  local peer_name = conn:getpeername() or {}
  return NativeConnection:new(conn, {
    local_host = local_name.ip or "127.0.0.1",
    local_port = local_name.port or 0,
    peer_host = peer_name.ip or dial_host,
    peer_port = peer_name.port or port,
    io_timeout = options.io_timeout,
    ctx = options.ctx,
  })
end

return M
