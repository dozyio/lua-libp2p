--- Host listener binding internals.
-- @module lua_libp2p.host.listeners
local M = {}
local log = require("lua_libp2p.log").subsystem("host")

local function list_copy(values)
  local out = {}
  for i, v in ipairs(values or {}) do
    out[i] = v
  end
  return out
end

local function close_all(listeners)
  for _, listener in ipairs(listeners or {}) do
    if listener and type(listener.close) == "function" then
      listener:close()
    end
  end
end

function M.init(host)
  host._listeners = {}
end

function M.install(Host)
  function Host:_bind_listeners(addrs)
    local targets = addrs and list_copy(addrs) or self.listen_addrs
    if #targets == 0 then
      targets = { "/ip4/127.0.0.1/tcp/0" }
    end
    log.debug("host listener bind started", {
      targets = #targets,
    })

    local next_listeners = {}
    local next_addrs = {}

    for _, addr in ipairs(targets) do
      if addr == "/p2p-circuit" then
        log.debug("host listener bind skipped", {
          addr = addr,
          reason = "circuit_listener_managed_by_autorelay",
        })
        goto continue_target
      end
      log.debug("host listener bind attempt", {
        addr = addr,
      })
      local listener, listen_err = self._tcp_transport.listen({
        multiaddr = addr,
        accept_timeout = self._accept_timeout,
        io_timeout = self._io_timeout,
        nodelay = self._tcp_options and self._tcp_options.nodelay,
        keepalive = self._tcp_options and self._tcp_options.keepalive,
        keepalive_initial_delay = self._tcp_options and self._tcp_options.keepalive_initial_delay,
      })
      if not listener then
        close_all(next_listeners)
        log.debug("host listener bind failed", {
          addr = addr,
          cause = tostring(listen_err),
        })
        return nil, listen_err
      end
      next_listeners[#next_listeners + 1] = listener

      local resolved, resolved_err = listener:multiaddr()
      if not resolved then
        close_all(next_listeners)
        log.debug("host listener address resolution failed", {
          addr = addr,
          cause = tostring(resolved_err),
        })
        return nil, resolved_err
      end
      next_addrs[#next_addrs + 1] = resolved
      log.debug("host listener bound", {
        addr = addr,
        resolved_addr = resolved,
      })
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

    if self._running then
      local self_update_ok, self_update_err = self:_emit_self_peer_update_if_changed()
      if not self_update_ok then
        return nil, self_update_err
      end
    end

    if self._running and self._runtime_impl and self._runtime_impl.sync_watchers then
      local ok, sync_err = self._runtime_impl.sync_watchers(self)
      if not ok then
        return nil, sync_err
      end
    end

    log.debug("host listener bind completed", {
      listeners = #self._listeners,
      listen_addrs = #self.listen_addrs,
    })
    return true
  end

  function Host:_close_listeners()
    log.debug("host listeners closing", {
      listeners = #self._listeners,
    })
    close_all(self._listeners)
    self._listeners = {}
  end
end

return M
