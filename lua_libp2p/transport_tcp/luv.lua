local tcp = require("lua_libp2p.transport_tcp.transport")
local error_mod = require("lua_libp2p.error")
local tcp_luv_native = require("lua_libp2p.transport_tcp.luv_native")

local ok_luv, uv = pcall(require, "luv")

local function env_truthy(value)
  if value == nil then
    return false
  end
  local normalized = string.lower(tostring(value))
  return normalized == "1" or normalized == "true" or normalized == "yes" or normalized == "on"
end

local force_proxy = env_truthy(os.getenv("LUA_LIBP2P_TCP_LUV_PROXY"))
local use_native = ok_luv and not force_proxy

local M = {}

-- Proxy mode is a temporary compatibility fallback. Native luv TCP is the
-- supported runtime=luv transport path when luv is installed.
M.BACKEND = use_native and "luv-native" or "luv-proxy"
M.IS_NATIVE = ok_luv
M.USE_NATIVE = use_native

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
    local listener, listen_err = tcp_luv_native.listen(target, opts)
    if not listener then
      return nil, listen_err
    end
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
    local conn, dial_err = tcp_luv_native.dial(target, opts)
    if not conn then
      return nil, dial_err
    end
    return wrap(conn)
  end

  local inner, err = tcp.dial(target, opts)
  if not inner then
    return nil, err
  end
  return wrap(inner)
end

return M
