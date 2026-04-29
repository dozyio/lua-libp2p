local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log")
local multiaddr = require("lua_libp2p.multiaddr")
local ping = require("lua_libp2p.protocol.ping")
local relay_client = require("lua_libp2p.relay.client")
local relay_proto = require("lua_libp2p.protocol.circuit_relay_v2")

local M = {}
M.KEEP_ALIVE_TAG = "relay-keep-alive"
M.DEFAULT_KEEPALIVE_INTERVAL = 30

local AutoRelay = {}
AutoRelay.__index = AutoRelay

local function copy_list(values)
  local out = {}
  for i, value in ipairs(values or {}) do
    out[i] = value
  end
  return out
end

local function target_peer_id(target)
  if type(target) == "string" then
    if target:sub(1, 1) == "/" then
      local parsed = multiaddr.parse(target)
      if parsed then
        for i = #parsed.components, 1, -1 do
          if parsed.components[i].protocol == "p2p" then
            return parsed.components[i].value
          end
        end
      end
      return target
    end
    return target
  end
  if type(target) == "table" then
    return target.peer_id or target.addr or tostring(target)
  end
  return tostring(target)
end

local function local_peer_id(host)
  if host and type(host.peer_id) == "function" then
    local peer = host:peer_id()
    return peer and peer.id
  end
  return nil
end

local function reservation_expire_at(reservation)
  local expire = reservation and reservation.reservation and tonumber(reservation.reservation.expire)
  if expire and expire > 0 then
    return expire
  end
  return nil
end

local function reservation_ttl(reservation, now)
  local expire_at = reservation_expire_at(reservation)
  if not expire_at then
    return nil
  end
  return expire_at - (now or os.time())
end

local function should_refresh(reservation, now, margin)
  if reservation.next_refresh_at then
    return now >= reservation.next_refresh_at
  end
  local expire_at = reservation_expire_at(reservation)
  if not expire_at then
    return false
  end
  return now >= (expire_at - margin)
end

local function next_refresh_at(reservation, now, refresh_timeout, refresh_timeout_min)
  local expire_at = reservation_expire_at(reservation)
  if not expire_at then
    return nil
  end
  local desired = expire_at - refresh_timeout
  local minimum = now + refresh_timeout_min
  if desired < minimum then
    desired = minimum
  end
  if desired >= expire_at then
    desired = math.max(now, expire_at - 1)
  end
  return desired
end

local function emit_event(host, name, payload)
  if not (host and type(host.emit) == "function") then
    return true
  end
  local ok, err = host:emit(name, payload)
  if not ok then
    log.warn("autorelay event handler failed", {
      event = name,
      cause = tostring(err),
    })
  end
  return true
end

function AutoRelay:_active_or_queued_count()
  local queued = 0
  for _ in pairs(self._queued) do
    queued = queued + 1
  end
  return (self.reservation_count or 0) + queued
end

function AutoRelay:_enqueue_target(target, opts)
  local options = opts or {}
  local key = target_peer_id(target)
  if not key or key == local_peer_id(self.host) then
    return false
  end
  local now = options.now or os.time()
  if self._reserved[key] or self._queued[key] or self._invalid[key] then
    return false
  end
  local backoff_until = self._backoff_until[key]
  if backoff_until and backoff_until > now then
    return false
  end
  if not options.force and self.max_reservations and self:_active_or_queued_count() >= self.max_reservations then
    return false
  end
  if #self._queue >= self.max_queue_length then
    return false
  end
  self._queued[key] = true
  self._targets[key] = target
  self._queue[#self._queue + 1] = { key = key, target = target, type = options.type or "discovered" }
  return true
end

function AutoRelay:_reserve_target(target, opts)
  local key = target_peer_id(target)
  if self._reserved[key] and not (opts and opts.force_refresh) then
    return self._reserved[key]
  end
  if self._failed[key] and not (opts and opts.retry_failed) then
    return nil, self._failed[key]
  end
  if not (opts and opts.force_refresh) and self.max_reservations and self.reservation_count >= self.max_reservations then
    return nil, error_mod.new("state", "autorelay reservation limit reached", {
      max_reservations = self.max_reservations,
    })
  end

  local reservation, err = self.client:reserve(target, opts or self.reserve_opts)
  if not reservation then
    self._failed[key] = err
    self._backoff_until[key] = os.time() + self.backoff_seconds
    if error_mod.is_error(err) and (err.kind == "unsupported" or err.kind == "protocol") then
      self._invalid[key] = true
    end
    emit_event(self.host, "relay:reservation:failed", {
      relay_peer_id = key,
      target = target,
      error = err,
      error_message = tostring(err),
    })
    return nil, err
  end
  local now = os.time()
  local ttl = reservation_ttl(reservation, now)
  if ttl ~= nil and ttl <= self.min_reservation_ttl then
    local ttl_err = error_mod.new("protocol", "relay reservation expires too soon", {
      ttl = ttl,
      min_ttl = self.min_reservation_ttl,
    })
    self._failed[key] = ttl_err
    self._backoff_until[key] = now + self.backoff_seconds
    emit_event(self.host, "relay:reservation:failed", {
      relay_peer_id = reservation.relay_peer_id or key,
      target = target,
      reservation = reservation.reservation,
      relay_addrs = copy_list(reservation.relay_addrs),
      connection_id = reservation.connection_id,
      error = ttl_err,
      error_message = tostring(ttl_err),
    })
    return nil, ttl_err
  end
  self._reserved[key] = reservation
  self._targets[key] = target
  self._failed[key] = nil
  self._backoff_until[key] = nil
  reservation.next_refresh_at = next_refresh_at(reservation, now, self.refresh_timeout, self.refresh_timeout_min)
  if self.keepalive_interval ~= nil and self.keepalive_interval ~= false then
    reservation.next_keepalive_at = now + self.keepalive_interval
  end
  if self.host and self.host.peerstore then
    local expire_at = reservation_expire_at(reservation)
    self.host.peerstore:tag(reservation.relay_peer_id or key, M.KEEP_ALIVE_TAG, {
      value = 1,
      ttl = expire_at and math.max(expire_at - now, 1) or math.huge,
    })
  end
  if self.host and self.host.connection_manager then
    local relay_peer_id = reservation.relay_peer_id or key
    if type(self.host.connection_manager.protect) == "function" then
      self.host.connection_manager:protect(relay_peer_id, M.KEEP_ALIVE_TAG)
    end
    if type(self.host.connection_manager.tag_peer) == "function" then
      self.host.connection_manager:tag_peer(relay_peer_id, M.KEEP_ALIVE_TAG, 100)
    end
  end
  if not (opts and opts.force_refresh) then
    self.reservation_count = self.reservation_count + 1
  end
  log.info("autorelay reservation active", {
    relay_peer_id = reservation.relay_peer_id or key,
    relay_addrs = #(reservation.relay_addrs or {}),
    expires_at = reservation.reservation and reservation.reservation.expire or nil,
  })
  emit_event(self.host, "relay:reservation:active", {
    relay_peer_id = reservation.relay_peer_id or key,
    reservation = reservation.reservation,
    relay_addrs = copy_list(reservation.relay_addrs),
    connection_id = reservation.connection_id,
  })
  return reservation
end

function AutoRelay:_remove_reservation(key, opts)
  local reservation = self._reserved[key]
  if not reservation then
    return false
  end
  local options = opts or {}
  self._reserved[key] = nil
  self._failed[key] = nil
  self._targets[key] = nil
  self.reservation_count = math.max((self.reservation_count or 1) - 1, 0)
  if self.client and type(self.client.remove_reservation) == "function" then
    self.client:remove_reservation(reservation.relay_peer_id or key)
  end
  if self.host and self.host.address_manager then
    self.host.address_manager:remove_relay_addrs(reservation.relay_addrs or {})
  end
  if self.host and self.host.peerstore and type(self.host.peerstore.untag) == "function" then
    self.host.peerstore:untag(reservation.relay_peer_id or key, M.KEEP_ALIVE_TAG)
  end
  if self.host and self.host.connection_manager then
    local relay_peer_id = reservation.relay_peer_id or key
    if type(self.host.connection_manager.unprotect) == "function" then
      self.host.connection_manager:unprotect(relay_peer_id, M.KEEP_ALIVE_TAG)
    end
    if type(self.host.connection_manager.tag_peer) == "function" then
      self.host.connection_manager:tag_peer(relay_peer_id, M.KEEP_ALIVE_TAG, 0)
    end
  end
  log.info("autorelay reservation removed", {
    relay_peer_id = reservation.relay_peer_id or key,
    relay_addrs = #(reservation.relay_addrs or {}),
    reason = options.reason,
  })
  emit_event(self.host, "relay:reservation:removed", {
    relay_peer_id = reservation.relay_peer_id or key,
    reservation = reservation.reservation,
    relay_addrs = copy_list(reservation.relay_addrs),
    connection_id = reservation.connection_id,
    reason = options.reason,
  })
  self:_maybe_request_replacement()
  return true
end

function AutoRelay:_keepalive(key, reservation, now)
  if self.keepalive_interval == nil or self.keepalive_interval == false then
    return true
  end
  local relay_peer_id = reservation and reservation.relay_peer_id
  if type(relay_peer_id) ~= "string" or relay_peer_id == "" then
    return true
  end
  if reservation.next_keepalive_at and now < reservation.next_keepalive_at then
    return true
  end
  reservation.next_keepalive_at = now + self.keepalive_interval
  if reservation.keepalive_task
    and reservation.keepalive_task.status ~= "completed"
    and reservation.keepalive_task.status ~= "failed"
    and reservation.keepalive_task.status ~= "cancelled"
  then
    return true
  end
  if self.host and type(self.host.spawn_task) == "function" then
    local task, task_err = self.host:spawn_task("autorelay.keepalive", function(ctx)
      log.info("autorelay keepalive ping running", {
        relay_peer_id = relay_peer_id,
        connection_id = reservation.connection_id,
      })
      local stream, _, _, stream_err = self.host:new_stream(relay_peer_id, { ping.ID }, {
        timeout = self.keepalive_timeout,
        io_timeout = self.keepalive_timeout,
        ctx = ctx,
      })
      if not stream then
        return nil, stream_err
      end
      local ok, ping_err = ping.ping_once(stream)
      if type(stream.close) == "function" then
        pcall(function()
          stream:close()
        end)
      end
      if not ok then
        return nil, ping_err
      end
      log.info("autorelay keepalive ping completed", {
        relay_peer_id = relay_peer_id,
        connection_id = reservation.connection_id,
        rtt = ok.rtt_seconds,
      })
      return true
    end, {
      service = "autorelay",
      peer_id = relay_peer_id,
    })
    if not task then
      return nil, task_err
    end
    reservation.keepalive_task = task
    return true
  end
  return true
end

function AutoRelay:_maybe_request_replacement()
  if not self.started or self.discover == false then
    return false
  end
  if self.max_reservations and self:_active_or_queued_count() >= self.max_reservations then
    return false
  end
  self.need_more_relays = true
  return true
end

local function has_protocol(protocols, protocol_id)
  for _, value in ipairs(protocols or {}) do
    if value == protocol_id then
      return true
    end
  end
  return false
end

function AutoRelay:scan_peerstore()
  if not (self.host and self.host.peerstore and type(self.host.peerstore.all) == "function") then
    return 0
  end
  local added = 0
  for _, peer in ipairs(self.host.peerstore:all()) do
    if has_protocol(peer.protocols, relay_proto.HOP_ID) then
      if self:_enqueue_target({ peer_id = peer.peer_id, addrs = peer.addrs }, { type = "discovered" }) then
        added = added + 1
      end
    end
  end
  return added
end

function AutoRelay:_process_queue(now, limit)
  local processed = 0
  local max = limit or self.reservation_concurrency
  while processed < max and #self._queue > 0 do
    local item = table.remove(self._queue, 1)
    self._queued[item.key] = nil
    local backoff_until = self._backoff_until[item.key]
    if not self._reserved[item.key]
        and not self._invalid[item.key]
        and not (backoff_until and backoff_until > now) then
      self:_reserve_target(item.target)
      processed = processed + 1
    end
  end
  if self.max_reservations and self.reservation_count >= self.max_reservations then
    self.need_more_relays = false
  end
  return processed
end

function AutoRelay:start()
  if self.started then
    return true
  end
  self.started = true

  if self.host and type(self.host.handle) == "function" and type(self.host._handle_relay_stop) == "function" then
    local ok, err = self.host:handle(relay_proto.STOP_ID, function(stream, ctx)
      return self.host:_handle_relay_stop(stream, ctx)
    end)
    if not ok then
      return nil, err
    end
  end

  if self.discover ~= false and self.host and type(self.host.on_protocol) == "function" then
    local token, err = self.host:on_protocol(relay_proto.HOP_ID, function(peer_id, payload)
      if not peer_id then
        return true
      end
      local target = { peer_id = peer_id }
      if payload and type(payload.addrs) == "table" then
        target.addrs = payload.addrs
      end
      self:_enqueue_target(target, { type = "discovered" })
      self:_process_queue(os.time(), self.reservation_concurrency)
      return true
    end)
    if not token then
      return nil, err
    end
    self._protocol_subscription = token
  end

  if self.host and type(self.host.on) == "function" then
    local token, err = self.host:on("connection_closed", function(payload)
      local connection_id = payload and payload.connection_id
      if not connection_id then
        return true
      end
      for key, reservation in pairs(self._reserved) do
        if reservation.connection_id == connection_id then
          self:_remove_reservation(key, { reason = "connection_closed" })
          break
        end
      end
      return true
    end)
    if not token then
      return nil, err
    end
    self._connection_closed_subscription = token
  end

  local report = {
    attempted = 0,
    reserved = 0,
    failed = 0,
    reservations = {},
    errors = {},
  }
  for _, target in ipairs(self.relays) do
    report.attempted = report.attempted + 1
    self:_enqueue_target(target, { type = "configured", force = true })
    self:_process_queue(os.time(), self.reservation_concurrency)
    local reservation = self._reserved[target_peer_id(target)]
    local err = self._failed[target_peer_id(target)]
    if reservation then
      report.reserved = report.reserved + 1
      report.reservations[#report.reservations + 1] = reservation
    else
      report.failed = report.failed + 1
      report.errors[#report.errors + 1] = err
      if self.fail_fast then
        return nil, err
      end
    end
  end
  self.start_report = report
  return report
end

function AutoRelay:stop()
  if self._protocol_subscription and self.host and type(self.host.off) == "function" then
    self.host:off("peer_protocols_updated", self._protocol_subscription)
  end
  if self._connection_closed_subscription and self.host and type(self.host.off) == "function" then
    self.host:off("connection_closed", self._connection_closed_subscription)
  end
  self._protocol_subscription = nil
  self._connection_closed_subscription = nil
  self.started = false
  return true
end

function AutoRelay:get_reservations()
  return self.client:get_reservations()
end

function AutoRelay:status()
  local failed = 0
  local failure_summary = {}
  local recent_failures = {}
  for _ in pairs(self._failed) do
    failed = failed + 1
  end
  for key, err in pairs(self._failed) do
    local message = tostring(err)
    failure_summary[message] = (failure_summary[message] or 0) + 1
    if #recent_failures < 5 then
      recent_failures[#recent_failures + 1] = {
        peer_id = key,
        error = message,
      }
    end
  end
  local invalid = 0
  for _ in pairs(self._invalid) do
    invalid = invalid + 1
  end
  return {
    reservations = self.reservation_count or 0,
    queued = #self._queue,
    failed = failed,
    invalid = invalid,
    failure_summary = failure_summary,
    recent_failures = recent_failures,
    need_more_relays = self.need_more_relays == true,
  }
end

function AutoRelay:tick(now)
  if not self.started then
    return true
  end
  local current = now or os.time()
  if self.need_more_relays and #self._queue == 0 then
    self:scan_peerstore()
  end
  self:_process_queue(current, self.reservation_concurrency)
  for key, reservation in pairs(self._reserved) do
    local keepalive_ok = self:_keepalive(key, reservation, current)
    if not keepalive_ok then
      self._failed[key] = error_mod.new("io", "relay keepalive failed")
      log.warn("autorelay keepalive failed", {
        relay_peer_id = reservation.relay_peer_id or key,
        connection_id = reservation.connection_id,
      })
    end
    local keepalive_task = reservation.keepalive_task
    if keepalive_task and keepalive_task.status == "failed" then
      self._failed[key] = keepalive_task.error
      log.warn("autorelay keepalive failed", {
        relay_peer_id = reservation.relay_peer_id or key,
        connection_id = reservation.connection_id,
        cause = tostring(keepalive_task.error),
      })
      reservation.keepalive_task = nil
    elseif keepalive_task and (keepalive_task.status == "completed" or keepalive_task.status == "cancelled") then
      reservation.keepalive_task = nil
    end
    if should_refresh(reservation, current, self.refresh_margin) then
      local target = self._targets[key]
      if target then
        local refreshed, err = self:_reserve_target(target, {
          force_refresh = true,
          retry_failed = true,
        })
        if not refreshed then
          self._failed[key] = err
          self:_remove_reservation(key, { reason = "refresh_failed" })
        end
      end
    end
    ::continue_reserved::
  end
  return true
end

function M.new(host, opts)
  if type(host) ~= "table" then
    return nil, error_mod.new("input", "autorelay requires host")
  end
  local options = opts or {}
  local client, client_err = relay_client.new(host, {
    relays = options.relays,
  })
  if not client then
    return nil, client_err
  end
  return setmetatable({
    host = host,
    relays = copy_list(options.relays),
    client = client,
    max_reservations = options.max_reservations or 2,
    max_queue_length = options.max_queue_length or 32,
    reservation_concurrency = options.reservation_concurrency or 1,
    backoff_seconds = options.backoff_seconds or 60,
    keepalive_interval = options.keepalive_interval == nil and M.DEFAULT_KEEPALIVE_INTERVAL or options.keepalive_interval,
    keepalive_timeout = options.keepalive_timeout or 5,
    fail_fast = options.fail_fast == true,
    discover = options.discover,
    reserve_opts = options.reserve_opts or {},
    refresh_margin = options.refresh_margin or 60,
    refresh_timeout = options.refresh_timeout or 300,
    refresh_timeout_min = options.refresh_timeout_min or 30,
    min_reservation_ttl = options.min_reservation_ttl or 10,
    reservation_count = 0,
    need_more_relays = false,
    _reserved = {},
    _failed = {},
    _invalid = {},
    _backoff_until = {},
    _queue = {},
    _queued = {},
    _targets = {},
    started = false,
  }, AutoRelay)
end

M.AutoRelay = AutoRelay

return M
