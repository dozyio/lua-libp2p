--- Host event bus internals.
local error_mod = require("lua_libp2p.error")

local M = {}

M.DEFAULT_EVENT_QUEUE_MAX = 256

function M.init(host, cfg)
  cfg = cfg or {}
  host._event_handlers = {}
  host._event_subscribers = {}
  host._next_subscriber_id = 1
  host._event_queue_max = cfg.event_queue_max or M.DEFAULT_EVENT_QUEUE_MAX
end

function M.emit(host, name, payload)
  local event = {
    name = name,
    payload = payload,
    ts = os.time(),
  }

  for _, sub in pairs(host._event_subscribers or {}) do
    if sub.event_name == nil or sub.event_name == name then
      local queue = sub.queue
      queue[#queue + 1] = event
      local max_size = sub.max_queue or host._event_queue_max or M.DEFAULT_EVENT_QUEUE_MAX
      while #queue > max_size do
        table.remove(queue, 1)
      end
    end
  end

  local handlers = host._event_handlers and host._event_handlers[name]
  if type(handlers) == "table" then
    for _, handler in ipairs(handlers) do
      local ok, err = pcall(handler, payload, event)
      if not ok then
        return nil,
          error_mod.new("protocol", "host event handler panicked", {
            event = name,
            cause = err,
          })
      end
    end
  end

  return true
end

function M.install(Host)
  --- Subscribe to host events.
  --- event_name string Event key to subscribe to.
  --- handler function Callback `(payload, event)`.
  -- Common events/payload keys:
  -- - `connection_opened`: `{ peer_id, connection_id, direction, remote_addr, limited }`
  -- - `connection_closed`: `{ peer_id, connection_id, cause }`
  -- - `peer_identified`: `{ peer_id, protocols, observed_addr }`
  -- - `stream:negotiated`: `{ peer_id, connection_id, protocol, limited }`
  -- - `task:started|task:completed|task:failed|task:cancelled`: `{ task_id, name, service }`
  --- true|nil ok
  --- table|nil err
  function Host:on(event_name, handler)
    if type(event_name) ~= "string" or event_name == "" then
      return nil, error_mod.new("input", "event name must be non-empty")
    end
    if type(handler) ~= "function" then
      return nil, error_mod.new("input", "event handler must be a function")
    end

    local handlers = self._event_handlers[event_name]
    if not handlers then
      handlers = {}
      self._event_handlers[event_name] = handlers
    end
    handlers[#handlers + 1] = handler
    return true
  end

  --- Emit a host event.
  --- event_name string Event key.
  --- payload? table Event payload table.
  --- true|nil ok
  --- table|nil err
  function Host:emit(event_name, payload)
    if type(event_name) ~= "string" or event_name == "" then
      return nil, error_mod.new("input", "event name must be non-empty")
    end
    return M.emit(self, event_name, payload)
  end

  --- Remove a previously registered event handler.
  --- event_name string Event key.
  --- handler function Previously registered callback.
  --- boolean removed
  function Host:off(event_name, handler)
    local handlers = self._event_handlers[event_name]
    if type(handlers) ~= "table" then
      return false
    end
    for i = 1, #handlers do
      if handlers[i] == handler then
        table.remove(handlers, i)
        return true
      end
    end
    return false
  end

  --- Create an event queue subscription.
  -- `event_name_or_opts` may be a string event name, or table `{ event_name|event, max_queue }`.
  -- `opts.max_queue` (`number`) overrides per-subscriber queue bound.
  -- @param[opt] event_name_or_opts Event name or options table.
  --- opts? table Options when first arg is event name.
  --- table|nil subscriber
  --- table|nil err
  function Host:subscribe(event_name_or_opts, opts)
    local event_name = nil
    local options = opts or {}

    if type(event_name_or_opts) == "table" then
      options = event_name_or_opts
      event_name = options.event_name or options.event
    elseif event_name_or_opts ~= nil then
      event_name = event_name_or_opts
    end

    if event_name ~= nil and (type(event_name) ~= "string" or event_name == "") then
      return nil, error_mod.new("input", "event name must be non-empty")
    end

    local sub = {
      id = self._next_subscriber_id,
      event_name = event_name,
      queue = {},
      max_queue = options.max_queue or self._event_queue_max or M.DEFAULT_EVENT_QUEUE_MAX,
    }
    self._next_subscriber_id = self._next_subscriber_id + 1
    self._event_subscribers[sub.id] = sub
    return sub
  end

  --- Remove a queue subscription.
  -- @param subscriber Subscriber table or numeric id.
  --- boolean removed
  function Host:unsubscribe(subscriber)
    local id = subscriber
    if type(subscriber) == "table" then
      id = subscriber.id
    end
    if type(id) ~= "number" then
      return false
    end
    if self._event_subscribers[id] == nil then
      return false
    end
    self._event_subscribers[id] = nil
    return true
  end

  --- Pop next queued event from a subscription.
  -- Returns `nil` when queue is empty.
  --- subscriber table Subscriber handle returned by @{subscribe}.
  --- table|nil event
  --- table|nil err
  function Host:next_event(subscriber)
    if type(subscriber) ~= "table" or type(subscriber.id) ~= "number" then
      return nil, error_mod.new("input", "subscriber handle is required")
    end
    local current = self._event_subscribers[subscriber.id]
    if current == nil then
      return nil, error_mod.new("state", "subscriber is not registered")
    end
    if #current.queue == 0 then
      return nil
    end
    return table.remove(current.queue, 1)
  end
end

return M
