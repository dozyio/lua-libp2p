local address_manager = require("lua_libp2p.address_manager")
local autorelay_mod = require("lua_libp2p.transport_circuit_relay_v2.autorelay")
local relay_proto = require("lua_libp2p.transport_circuit_relay_v2.protocol")

local relay_a = "12D3KooWCryG7Mon9orvQxcS1rYZjotPgpwoJNHHKcLLfE4Hf5mV"
local relay_b = "12D3KooWQWZLu9qXWPTDnF9rTRrAiVGZrXCbHAvkqYrsG8cW4UHg"

local function fake_host(reserve_log)
  local handlers = {}
  local host = {
    address_manager = address_manager.new(),
    _event_handlers = handlers,
    connection_manager = {
      protected = {},
      tags = {},
      protect = function(self, peer_id, tag)
        self.protected[peer_id] = tag
        return true
      end,
      unprotect = function(self, peer_id, tag)
        if self.protected[peer_id] == tag then
          self.protected[peer_id] = nil
        end
        return true
      end,
      tag_peer = function(self, peer_id, tag, value)
        self.tags[peer_id] = self.tags[peer_id] or {}
        self.tags[peer_id][tag] = value
        return true
      end,
    },
  }
  function host:on(event_name, handler)
    handlers[event_name] = handlers[event_name] or {}
    handlers[event_name][#handlers[event_name] + 1] = handler
    return true
  end
  function host:emit(event_name, payload)
    for _, handler in ipairs(handlers[event_name] or {}) do
      local ok, err = pcall(handler, payload, { name = event_name, ts = os.time() })
      if not ok then
        return nil, err
      end
    end
    return true
  end
  function host:off(event_name, handler)
    local list = handlers[event_name] or {}
    for i = 1, #list do
      if list[i] == handler then
        table.remove(list, i)
        return true
      end
    end
    return false
  end
  function host:on_protocol(protocol_id, handler)
    return self:on("peer_protocols_updated", function(payload, event)
      for _, p in ipairs(payload.protocols or {}) do
        if p == protocol_id then
          return handler(payload.peer_id, payload, event)
        end
      end
      return true
    end)
  end
  function host:peer_id()
    return { id = "12D3KooWDst1ALeN4CpCWz1CPBZEz47wUSJz7WxZuo1JpKfTy5UK" }
  end
  function host:new_stream(target)
    reserve_log[#reserve_log + 1] = target
    return nil, nil, nil, "not used"
  end
  function host:_find_connection(peer_id)
    return self._connections_by_peer and self._connections_by_peer[peer_id] or nil
  end
  function host:handle(protocol_id, handler)
    self._handlers = self._handlers or {}
    self._handlers[protocol_id] = handler
    return true
  end
  function host:_handle_relay_stop()
    return true
  end
  return host
end

local function run()
  local reserve_log = {}
  local host = fake_host(reserve_log)
  local events = {}
  for _, event_name in ipairs({
    "relay:reservation:active",
    "relay:reservation:removed",
    "relay:reservation:failed",
    "relay:not-enough-relays",
    "relay:found-enough-relays",
  }) do
    host:on(event_name, function(payload)
      events[#events + 1] = { name = event_name, payload = payload }
      return true
    end)
  end
  host.peerstore = require("lua_libp2p.peerstore").new()
  host._connections_by_peer = {}
  local ar, ar_err = autorelay_mod.new(host, {
    relays = { relay_a },
    max_reservations = 2,
    refresh_timeout = 15,
    refresh_timeout_min = 1,
    min_reservation_ttl = 0,
    keepalive_interval = false,
  })
  if not ar then
    return nil, ar_err
  end
  local default_keepalive_host = fake_host({})
  default_keepalive_host.peerstore = require("lua_libp2p.peerstore").new()
  default_keepalive_host._connections_by_peer = {}
  local default_keepalive = assert(autorelay_mod.new(default_keepalive_host, {
    relays = { relay_a },
    min_reservation_ttl = 0,
  }))
  if default_keepalive.keepalive_interval ~= autorelay_mod.DEFAULT_KEEPALIVE_INTERVAL then
    return nil, "autorelay should enable relay keepalive by default"
  end
  function default_keepalive.client:reserve(target)
    local relay_peer_id = type(target) == "table" and target.peer_id or target
    return {
      relay_peer_id = relay_peer_id,
      reservation = { expire = os.time() + 30 },
      relay_addrs = {},
      connection = {},
      connection_id = 100,
    }
  end
  local default_report, default_report_err = default_keepalive:start()
  if not default_report then
    return nil, default_report_err
  end
  local default_reservation = default_keepalive._reserved[relay_a]
  if not default_reservation or not default_reservation.next_keepalive_at then
    return nil, "autorelay should schedule first keepalive after reservation activation"
  end
  local keepalive_due_at = default_reservation.next_keepalive_at
  local default_ticked, default_tick_err = default_keepalive:tick(os.time())
  if not default_ticked then
    return nil, default_tick_err
  end
  if default_keepalive._reserved[relay_a] == nil or default_reservation.next_keepalive_at ~= keepalive_due_at then
    return nil, "autorelay should not run keepalive immediately after reservation activation"
  end
  default_keepalive:stop()

  local keepalive_failure_host = fake_host({})
  keepalive_failure_host.peerstore = require("lua_libp2p.peerstore").new()
  keepalive_failure_host._connections_by_peer = {}
  local keepalive_failure = assert(autorelay_mod.new(keepalive_failure_host, {
    relays = { relay_a },
    keepalive_interval = 1,
    min_reservation_ttl = 0,
  }))
  function keepalive_failure.client:reserve(target)
    local relay_peer_id = type(target) == "table" and target.peer_id or target
    return {
      relay_peer_id = relay_peer_id,
      reservation = { expire = os.time() + 30 },
      relay_addrs = {},
      connection = {},
      connection_id = 101,
    }
  end
  function keepalive_failure_host:new_stream()
    return nil, nil, nil, "keepalive failed"
  end
  function keepalive_failure_host:spawn_task(name, fn, opts)
    local task = {
      id = 1,
      name = name,
      service = opts and opts.service,
      status = "ready",
    }
    local ok, result, err = pcall(fn, {})
    if not ok then
      task.status = "failed"
      task.error = result
    elseif result == nil and err then
      task.status = "failed"
      task.error = err
    else
      task.status = "completed"
      task.result = result
    end
    return task
  end
  assert(keepalive_failure:start())
  local keepalive_reservation = keepalive_failure._reserved[relay_a]
  keepalive_reservation.next_keepalive_at = os.time() - 1
  assert(keepalive_failure:tick(os.time()))
  if keepalive_failure._reserved[relay_a] == nil then
    return nil, "autorelay should not remove reservation on keepalive ping failure alone"
  end
  keepalive_failure:stop()
  function ar.client:reserve(target)
    reserve_log[#reserve_log + 1] = target
    local relay_peer_id = type(target) == "table" and target.peer_id or target
    local conn = { relay_peer_id = relay_peer_id }
    local connection_id = #reserve_log
    return {
      relay_peer_id = relay_peer_id,
      reservation = { expire = os.time() + 30 },
      relay_addrs = {},
      connection = conn,
      connection_id = connection_id,
    }
  end

  local report, report_err = ar:start()
  if not report then
    return nil, report_err
  end
  if report.attempted ~= 1 or report.reserved ~= 1 then
    return nil, "autorelay should reserve configured relays on start"
  end
  if type(host._handlers) ~= "table" or type(host._handlers[relay_proto.STOP_ID]) ~= "function" then
    return nil, "autorelay should install relay stop handler"
  end
  if reserve_log[1] ~= relay_a then
    return nil, "autorelay should reserve configured relay target"
  end
  if events[1].name ~= "relay:reservation:active" or events[1].payload.relay_peer_id ~= relay_a then
    return nil, "autorelay should emit reservation active event"
  end

  local handlers = host._event_handlers.peer_protocols_updated
  if type(handlers) ~= "table" or type(handlers[1]) ~= "function" then
    return nil, "autorelay should subscribe to relay protocol topology"
  end
  handlers[1]({
    peer_id = relay_b,
    protocols = { relay_proto.HOP_ID },
    addrs = { "/ip4/203.0.113.2/tcp/4001/p2p/" .. relay_b },
  })
  if type(reserve_log[2]) ~= "table" or reserve_log[2].peer_id ~= relay_b then
    return nil, "autorelay should reserve discovered relay peers"
  end
  local saw_relay_b_active = false
  for _, event in ipairs(events) do
    if event.name == "relay:reservation:active" and event.payload.relay_peer_id == relay_b then
      saw_relay_b_active = true
      break
    end
  end
  if not saw_relay_b_active then
    return nil, "autorelay should emit discovered reservation active event"
  end

  handlers[1]({ peer_id = "third", protocols = { relay_proto.HOP_ID } })
  if #reserve_log ~= 2 then
    return nil, "autorelay should respect max_reservations"
  end

  local ticked, tick_err = ar:tick(os.time() + 20)
  if not ticked then
    return nil, tick_err
  end
  if #reserve_log < 3 then
    return nil, "autorelay should refresh reservations before expiry"
  end
  local relay_tags = host.peerstore:get_tags(relay_a)
  if not relay_tags[autorelay_mod.KEEP_ALIVE_TAG] then
    return nil, "autorelay should tag reserved relays to keep connections alive"
  end
  if host.connection_manager.protected[relay_a] ~= autorelay_mod.KEEP_ALIVE_TAG then
    return nil, "autorelay should protect relay backing connections in connection manager"
  end
  if not host.connection_manager.tags[relay_a]
    or host.connection_manager.tags[relay_a][autorelay_mod.KEEP_ALIVE_TAG] ~= 100
  then
    return nil, "autorelay should tag relay peer in connection manager"
  end

  local connection_handlers = host._event_handlers.connection_closed
  if type(connection_handlers) ~= "table" or type(connection_handlers[1]) ~= "function" then
    return nil, "autorelay should subscribe to connection close events"
  end
  connection_handlers[1]({ connection_id = 999 })
  if ar._reserved[relay_a] == nil then
    return nil, "autorelay should ignore unrelated connection close events"
  end
  connection_handlers[1]({ connection_id = ar._reserved[relay_b].connection_id })
  if ar._reserved[relay_b] ~= nil then
    return nil, "autorelay should remove reservation when backing connection closes"
  end
  local saw_connection_closed_removed = false
  for _, event in ipairs(events) do
    if event.name == "relay:reservation:removed" and event.payload.reason == "connection_closed" then
      saw_connection_closed_removed = true
      break
    end
  end
  if not saw_connection_closed_removed then
    return nil, "autorelay should emit reservation removed event"
  end

  local failed_attempts = 0
  function ar.client:reserve()
    failed_attempts = failed_attempts + 1
    return nil, "refresh failed"
  end
  ar._reserved[relay_a].reservation.expire = os.time() + 10
  ar._reserved[relay_a].next_refresh_at = nil
  local removed_tick, removed_tick_err = ar:tick(os.time() + 5)
  if not removed_tick then
    return nil, removed_tick_err
  end
  if ar._reserved[relay_a] ~= nil or ar.reservation_count >= 2 then
    return nil, "autorelay should remove failed refreshed reservations"
  end
  if host.peerstore:get_tags(relay_a)[autorelay_mod.KEEP_ALIVE_TAG] ~= nil then
    return nil, "autorelay should remove relay keepalive tag on reservation removal"
  end
  if host.connection_manager.protected[relay_a] ~= nil then
    return nil, "autorelay should unprotect relay peer on reservation removal"
  end
  if host.connection_manager.tags[relay_a][autorelay_mod.KEEP_ALIVE_TAG] ~= 0 then
    return nil, "autorelay should clear relay connection-manager tag on reservation removal"
  end
  local saw_failed = false
  local saw_refresh_removed = false
  local saw_not_enough = false
  for _, event in ipairs(events) do
    if event.name == "relay:reservation:failed" and event.payload.relay_peer_id == relay_a then
      saw_failed = true
    elseif event.name == "relay:reservation:removed" and event.payload.reason == "refresh_failed" then
      saw_refresh_removed = true
    elseif event.name == "relay:not-enough-relays" then
      saw_not_enough = true
    end
  end
  if not saw_failed then
    return nil, "autorelay should emit reservation failed event"
  end
  if not saw_refresh_removed then
    return nil, "autorelay should emit refresh removal event"
  end
  if not saw_not_enough then
    return nil, "autorelay should emit relay:not-enough-relays after reservation loss"
  end

  handlers[1]({ peer_id = relay_a, protocols = { relay_proto.HOP_ID } })
  if failed_attempts ~= 1 then
    return nil, "autorelay should back off failed relay candidates"
  end

  handlers[1]({ peer_id = "replacement", protocols = { relay_proto.HOP_ID } })
  local replacement_tick, replacement_tick_err = ar:tick(os.time() + 70)
  if not replacement_tick then
    return nil, replacement_tick_err
  end
  if failed_attempts < 2 then
    return nil, "autorelay should attempt replacement relay after removal"
  end

  local stopped = ar:stop()
  if not stopped or ar.started then
    return nil, "autorelay stop should unsubscribe and clear started state"
  end

  return true
end

return {
  name = "autorelay static and topology reservations",
  run = run,
}
