--- Host protocol handler registry and limited-connection policy.
-- @module lua_libp2p.host.protocols
local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log").subsystem("host")
local multistream = require("lua_libp2p.multistream_select.protocol")

local M = {}

local function normalize_protocol_list(protocols)
  if type(protocols) == "string" then
    return { protocols }
  end
  return protocols
end

local function list_contains(values, needle)
  for _, value in ipairs(values or {}) do
    if value == needle then
      return true
    end
  end
  return false
end

function M.init(host)
  host._handlers = {}
  host._handler_options = {}
end

function M.install(Host)
  --- Register a stream handler for a protocol ID.
  -- @tparam string protocol_id Protocol multistream ID.
  -- @tparam function handler Stream handler `(stream, ctx)`.
  -- @tparam[opt] table opts Handler options.
  -- `opts.run_on_limited_connection=true` allows this protocol on limited relay links.
  -- @treturn true|nil ok True on success, otherwise nil.
  -- @treturn[opt] table err Structured error on failure.
  function Host:handle(protocol_id, handler, opts)
    if type(protocol_id) ~= "string" or protocol_id == "" then
      return nil, error_mod.new("input", "protocol id must be non-empty")
    end
    if type(handler) ~= "function" then
      return nil, error_mod.new("input", "handler must be a function")
    end
    local options = opts or {}
    self._handlers[protocol_id] = handler
    self._handler_options[protocol_id] = options
    log.debug("host protocol handler registered", {
      protocol = protocol_id,
      run_on_limited_connection = options.run_on_limited_connection == true,
    })
    if self._running then
      return self:_emit_self_peer_update_if_changed()
    end
    return true
  end

  --- Remove a previously registered stream handler.
  -- @tparam string protocol_id Protocol multistream ID.
  -- @treturn boolean removed
  -- @treturn[opt] table err
  function Host:unhandle(protocol_id)
    if type(protocol_id) ~= "string" or protocol_id == "" then
      return nil, error_mod.new("input", "protocol id must be non-empty")
    end
    if self._handlers[protocol_id] == nil then
      return false
    end
    self._handlers[protocol_id] = nil
    self._handler_options[protocol_id] = nil
    log.debug("host protocol handler removed", {
      protocol = protocol_id,
    })
    if self._running then
      local ok, err = self:_emit_self_peer_update_if_changed()
      if not ok then
        return nil, err
      end
    end
    return true
  end

  function Host:_connection_is_limited(state)
    if type(state) ~= "table" then
      return false
    end
    if state.limited == true then
      return true
    end
    return type(state.relay) == "table" and state.relay.limit_kind == "limited"
  end

  --- Check limited-connection policy for one protocol.
  -- `opts.allow_limited_connection=true` overrides default restrictions.
  function Host:_protocol_allowed_on_limited_connection(protocol_id, opts)
    if type(protocol_id) ~= "string" or protocol_id == "" then
      return false
    end
    local options = opts or {}
    if options.allow_limited_connection == true or options.run_on_limited_connection == true then
      return true
    end
    local handler_options = self._handler_options[protocol_id]
    if type(handler_options) == "table" and handler_options.run_on_limited_connection == true then
      return true
    end
    return false
  end

  --- Filter protocol list by limited-connection policy.
  -- `opts.allow_limited_connection=true` allows all protocols.
  function Host:_protocols_allowed_on_limited_connection(protocols, opts)
    for _, protocol_id in ipairs(normalize_protocol_list(protocols) or {}) do
      if not self:_protocol_allowed_on_limited_connection(protocol_id, opts) then
        return nil,
          error_mod.new("permission", "protocol is not allowed over limited connection", {
            protocol = protocol_id,
          })
      end
    end
    return true
  end

  function Host:_build_router()
    local router = multistream.new_router()
    for protocol_id, handler in pairs(self._handlers) do
      local ok, err = router:register(protocol_id, handler, self._handler_options[protocol_id])
      if not ok then
        return nil, err
      end
    end
    return router
  end

  function Host:on_protocol(protocol_id, handler)
    if type(protocol_id) ~= "string" or protocol_id == "" then
      return nil, error_mod.new("input", "protocol id must be non-empty")
    end
    if type(handler) ~= "function" then
      return nil, error_mod.new("input", "protocol handler must be a function")
    end

    local function wrapped(payload, event)
      if
        list_contains(payload and payload.protocols, protocol_id)
        or list_contains(payload and payload.added_protocols, protocol_id)
      then
        return handler(payload.peer_id, payload, event)
      end
      return true
    end

    local ok, err = self:on("peer_protocols_updated", wrapped)
    if not ok then
      return nil, err
    end

    if self.peerstore and type(self.peerstore.all) == "function" then
      for _, peer in ipairs(self.peerstore:all()) do
        if list_contains(peer.protocols, protocol_id) then
          local call_ok, call_err = pcall(handler, peer.peer_id, {
            peer_id = peer.peer_id,
            protocols = peer.protocols,
            added_protocols = { protocol_id },
            source = "peerstore",
          }, nil)
          if not call_ok then
            return nil, error_mod.new("protocol", "protocol handler panicked", { cause = call_err })
          end
        end
      end
    end

    return wrapped
  end
end

return M
