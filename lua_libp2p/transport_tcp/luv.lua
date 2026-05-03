--- Luv-backed TCP transport wrapper.
-- Uses native libuv TCP handles through luv.
-- @module lua_libp2p.transport_tcp.luv
local tcp_luv_native = require("lua_libp2p.transport_tcp.luv_native")
local error_mod = require("lua_libp2p.error")

local M = {}

M.BACKEND = "luv-native"
M.IS_NATIVE = true
M.USE_NATIVE = true

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

function Wrapped:watch_luv_readable(on_readable)
  if type(self._inner.watch_luv_readable) == "function" then
    local unwatch, inner_err = self._inner:watch_luv_readable(on_readable)
    if type(unwatch) ~= "function" then
      return nil, inner_err
    end
    self._unwatchers[#self._unwatchers + 1] = unwatch
    return unwatch
  end
  return nil, error_mod.new("unsupported", "native luv readable watcher is unavailable")
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

--- Start a listener using native luv TCP.
-- `opts.multiaddr` or `opts.host`/`opts.port` selects bind endpoint.
-- `opts.accept_timeout` and `opts.io_timeout` tune socket timeouts.
-- `opts.nodelay` and `opts.keepalive` default to enabled; `keepalive_initial_delay`
-- defaults to 0 when keepalive is enabled.
-- @param target Optional listen target (string or table).
-- @tparam[opt] table opts
-- @treturn table|nil listener
-- @treturn[opt] table err
function M.listen(target, opts)
  local listener, listen_err = tcp_luv_native.listen(target, opts)
  if not listener then
    return nil, listen_err
  end
  return wrap(listener)
end

--- Dial using native luv TCP.
-- `opts.port` is used when `target` is host string.
-- `opts.timeout`, `opts.io_timeout`, and `opts.ctx` control dial/IO timing.
-- `opts.nodelay` and `opts.keepalive` default to enabled; `keepalive_initial_delay`
-- defaults to 0 when keepalive is enabled.
-- @param target Dial target (multiaddr, host string, or table).
-- @tparam[opt] table opts
-- @treturn table|nil conn
-- @treturn[opt] table err
function M.dial(target, opts)
  local conn, dial_err = tcp_luv_native.dial(target, opts)
  if not conn then
    return nil, dial_err
  end
  return wrap(conn)
end

return M
