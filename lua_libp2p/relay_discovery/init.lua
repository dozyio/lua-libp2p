--- Relay candidate discovery service.
-- Coordinates AutoRelay candidate replenishment using host peer discovery and
-- KAD random walks without embedding that policy in the host runtime.
-- @module lua_libp2p.relay_discovery
local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log").subsystem("relay_discovery")

local M = {
  provides = { "relay_discovery" },
  requires = { "autorelay" },
}

local RelayDiscovery = {}
RelayDiscovery.__index = RelayDiscovery

local function task_active(task)
  return task
    and task.status ~= "completed"
    and task.status ~= "failed"
    and task.status ~= "cancelled"
end

function RelayDiscovery:_schedule()
  if not self.pending then
    log.debug("relay discovery schedule skipped", { reason = "not_pending" })
    return true
  end
  if task_active(self.task) then
    log.debug("relay discovery schedule skipped", {
      reason = "task_active",
      task_status = self.task.status,
    })
    return true
  end
  if not (self.host and self.host._running) then
    log.debug("relay discovery schedule skipped", { reason = "host_not_running" })
    return true
  end

  local current = os.time()
  local delay = math.max(0, (self.next_allowed_at or 0) - current)
  log.debug("relay discovery scheduled", {
    reason = self.reason,
    delay_seconds = delay,
  })
  local task, task_err = self.host:spawn_task("relay_discovery.replenish", function(ctx)
    if delay > 0 then
      local slept, sleep_err = ctx:sleep(delay)
      if slept == nil and sleep_err then
        return nil, sleep_err
      end
    end
    if not self.host._running then
      return true
    end
    return self:run_once()
  end, { service = "relay_discovery" })
  if not task then
    return nil, task_err
  end
  self.task = task
  return true
end

function RelayDiscovery:request(reason)
  self.pending = true
  self.reason = reason
  log.debug("relay discovery requested", {
    reason = reason,
  })
  return self:_schedule()
end

function RelayDiscovery:on_host_started()
  if self.host and self.host.autorelay and self.host.autorelay.need_more_relays == true then
    self:request("initial_need_more_relays")
  end
  return self:_schedule()
end

function RelayDiscovery:run_once(now)
  if not self.pending then
    log.debug("relay discovery run skipped", { reason = "not_pending" })
    return true
  end
  if self.host.autorelay and self.host.autorelay.need_more_relays == false then
    self.pending = false
    self.task = nil
    log.debug("relay discovery run skipped", { reason = "relays_sufficient" })
    return true
  end
  local current = now or os.time()
  if current < (self.next_allowed_at or 0) then
    log.debug("relay discovery run delayed", {
      next_allowed_at = self.next_allowed_at,
      now = current,
    })
    return true
  end

  local ktable_peers = 0
  if self.host.kad_dht
    and self.host.kad_dht.routing_table
    and type(self.host.kad_dht.routing_table.all_peers) == "function"
  then
    ktable_peers = #(self.host.kad_dht.routing_table:all_peers() or {})
  end

  if ktable_peers == 0 and self.host._bootstrap_discovery and self.host._bootstrap_discovery.dial_on_start ~= false then
    log.debug("relay discovery triggering bootstrap discovery", {
      reason = self.reason,
      cooldown_seconds = self.cooldown_seconds,
    })
    self.host._bootstrap_discovery_done = false
    self.next_allowed_at = current + self.cooldown_seconds
    local boot_ok, boot_err = self.host:_schedule_bootstrap_discovery(0)
    if not boot_ok then
      return nil, boot_err
    end
    self.task = nil
    return self:_schedule()
  end

  if self.host.kad_dht and type(self.host.kad_dht.random_walk) == "function" then
    if task_active(self.walk_task) then
      self.next_allowed_at = current + 1
      self.task = nil
      log.debug("relay discovery random walk skipped", {
        reason = "walk_active",
        walk_task_status = self.walk_task.status,
      })
      return self:_schedule()
    end
    log.debug("relay discovery random walk started", {
      reason = self.reason,
      routing_table_peers = ktable_peers,
    })
    local walk_op, walk_err = self.host.kad_dht:random_walk({
      alpha = self.host.kad_dht.alpha,
      disjoint_paths = self.host.kad_dht.disjoint_paths,
    })
    if not walk_op then
      log.debug("relay discovery random walk failed", {
        cause = tostring(walk_err),
      })
      return nil, walk_err
    end
    self.walk_task = walk_op:task()
    self.pending = false
    self.next_allowed_at = current + self.cooldown_seconds
    return true
  end

  self.pending = false
  self.next_allowed_at = current + self.cooldown_seconds
  log.debug("relay discovery run completed", {
    reason = "no_discovery_path",
    cooldown_seconds = self.cooldown_seconds,
  })
  return true
end

function RelayDiscovery:start()
  if self.started then
    return true
  end
  if not self.host.autorelay then
    return nil, error_mod.new("state", "relay_discovery requires autorelay service")
  end
  local token, on_err = self.host:on("relay:not-enough-relays", function(payload)
    self:request(payload and payload.reason or "relay_not_enough")
    return true
  end)
  if not token then
    return nil, on_err
  end
  self._not_enough_subscription = token
  self.started = true
  log.debug("relay discovery service started", {
    cooldown_seconds = self.cooldown_seconds,
  })
  if self.host.autorelay.need_more_relays == true then
    self:request("initial_need_more_relays")
  end
  return true
end

function RelayDiscovery:stop()
  if self._not_enough_subscription and self.host and type(self.host.off) == "function" then
    self.host:off("relay:not-enough-relays", self._not_enough_subscription)
  end
  self._not_enough_subscription = nil
  self.started = false
  log.debug("relay discovery service stopped")
  return true
end

function RelayDiscovery:status()
  return {
    pending = self.pending == true,
    reason = self.reason,
    next_allowed_at = self.next_allowed_at,
    task_status = self.task and self.task.status or nil,
    walk_task_status = self.walk_task and self.walk_task.status or nil,
  }
end

function M.new(host, opts)
  local options = opts or {}
  return setmetatable({
    host = host,
    pending = false,
    reason = nil,
    cooldown_seconds = options.cooldown_seconds or 30,
    next_allowed_at = 0,
    task = nil,
    walk_task = nil,
    started = false,
  }, RelayDiscovery)
end

M.RelayDiscovery = RelayDiscovery

return M
