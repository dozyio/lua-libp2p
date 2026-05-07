--- Host listener binding internals.
-- @module lua_libp2p.host.listeners
local M = {}
local log = require("lua_libp2p.log").subsystem("host")
local multiaddr = require("lua_libp2p.multiaddr")
local table_utils = require("lua_libp2p.util.tables")

local list_copy = table_utils.copy_list

local function close_all(listeners)
  for _, listener in ipairs(listeners or {}) do
    if listener and type(listener.close) == "function" then
      listener:close()
    end
  end
end

local function is_addr_in_use_error(err)
  local text = string.upper(tostring(err or ""))
  if text:find("EADDRINUSE", 1, true) ~= nil then
    return true
  end
  if type(err) == "table" and type(err.context) == "table" and err.context.cause ~= nil then
    local cause_text = string.upper(tostring(err.context.cause))
    if cause_text:find("EADDRINUSE", 1, true) ~= nil then
      return true
    end
  end
  return false
end

local function has_ipv6_wildcard_listener(bound_addrs, port)
  for _, addr in ipairs(bound_addrs or {}) do
    local parsed = multiaddr.parse(addr)
    if parsed then
      local endpoint = multiaddr.to_tcp_endpoint(parsed)
      local host_proto = parsed.components and parsed.components[1] and parsed.components[1].protocol or nil
      if endpoint and host_proto == "ip6" and endpoint.host == "::" and endpoint.port == port then
        return true
      end
    end
  end
  return false
end

local function verify_bound_targets(targets, bound_addrs)
  local bound = {}
  for _, addr in ipairs(bound_addrs or {}) do
    local parsed = multiaddr.parse(addr)
    if parsed then
      local endpoint = multiaddr.to_tcp_endpoint(parsed)
      if endpoint and endpoint.host and endpoint.port then
        local family = endpoint.host:find(":", 1, true) and "ip6" or "ip4"
        bound[#bound + 1] = {
          family = family,
          port = endpoint.port,
          addr = addr,
        }
      end
    end
  end

  for _, target in ipairs(targets or {}) do
    local parsed = multiaddr.parse(target)
    if parsed then
      local host_proto = parsed.components and parsed.components[1] and parsed.components[1].protocol or nil
      if host_proto ~= "ip4" and host_proto ~= "ip6" then
        goto continue_target
      end
      local endpoint = multiaddr.to_tcp_endpoint(parsed)
      if endpoint and endpoint.host and endpoint.port then
        local family = host_proto
        local matched = false
        for _, b in ipairs(bound) do
          if b.family == family and (endpoint.port == 0 or b.port == endpoint.port) then
            matched = true
            break
          end
        end
        if not matched and family == "ip4" and endpoint.port ~= 0 and has_ipv6_wildcard_listener(bound_addrs, endpoint.port) then
          matched = true
        end
        if not matched then
          return nil, "listen bind verification failed: missing " .. family .. " listener for " .. tostring(target)
        end
      end
    end
    ::continue_target::
  end
  return true
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

    local ip6_targets = {}
    local other_targets = {}
    for _, addr in ipairs(targets) do
      local parsed = multiaddr.parse(addr)
      local host_proto = parsed and parsed.components and parsed.components[1] and parsed.components[1].protocol or nil
      if host_proto == "ip6" then
        ip6_targets[#ip6_targets + 1] = addr
      else
        other_targets[#other_targets + 1] = addr
      end
    end
    targets = {}
    for _, addr in ipairs(ip6_targets) do
      targets[#targets + 1] = addr
    end
    for _, addr in ipairs(other_targets) do
      targets[#targets + 1] = addr
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
        local parsed = multiaddr.parse(addr)
        local endpoint = parsed and multiaddr.to_tcp_endpoint(parsed) or nil
        local host_proto = parsed and parsed.components and parsed.components[1] and parsed.components[1].protocol or nil
        if host_proto == "ip4" and endpoint and endpoint.port ~= 0 and is_addr_in_use_error(listen_err)
          and has_ipv6_wildcard_listener(next_addrs, endpoint.port)
        then
          goto continue_target
        end
        close_all(next_listeners)
        log.error("host listener bind failed", {
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

    local verify_ok, verify_err = verify_bound_targets(targets, next_addrs)
    if not verify_ok then
      close_all(next_listeners)
      log.error("host listener bind verification failed", {
        cause = tostring(verify_err),
      })
      return nil, verify_err
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
