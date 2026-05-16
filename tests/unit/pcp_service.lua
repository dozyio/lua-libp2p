local address_manager = require("lua_libp2p.address_manager")
local pcp_service = require("lua_libp2p.port_mapping.pcp.service")

local function run()
  local handlers = {}
  local host = {
    address_manager = address_manager.new({
      listen_addrs = {
        "/ip4/192.168.1.44/tcp/4001",
      },
    }),
    events = {},
    on = function(_, event_name, handler)
      handlers[event_name] = handlers[event_name] or {}
      handlers[event_name][#handlers[event_name] + 1] = handler
      return true
    end,
    off = function()
      return true
    end,
    emit = function(self, event_name, payload)
      self.events[#self.events + 1] = { name = event_name, payload = payload }
      for _, handler in ipairs(handlers[event_name] or {}) do
        local ok, err = handler(payload)
        if not ok then
          return nil, err
        end
      end
      return true
    end,
    _emit_self_peer_update_if_changed = function(self)
      return self:emit("self:peer_updated", {})
    end,
    _tasks = {},
    spawn_task = function(self, name, fn)
      self._tasks[#self._tasks + 1] = { name = name, fn = fn }
      return { id = #self._tasks, name = name }
    end,
    cancel_task = function()
      return true
    end,
  }
  local client = {
    calls = 0,
    map_port = function(self, protocol, internal_port, external_port, lifetime)
      self.calls = self.calls + 1
      return {
        protocol = protocol,
        external_ip = "198.51.100.20",
        external_port = external_port,
        lifetime = lifetime,
        internal_port = internal_port,
      }
    end,
  }
  local svc = assert(pcp_service.new(host, {
    gateway = "192.168.1.1",
    client = client,
  }))
  local ok, err = svc:start()
  if not ok then
    return nil, err
  end
  if client.calls ~= 0 then
    return nil, "pcp start should not block on initial mapping"
  end
  if #host._tasks ~= 1 or host._tasks[1].name ~= "pcp.initial_map" then
    return nil, "pcp start should schedule async initial mapping task"
  end
  local task_ok, task_err = host._tasks[1].fn({
    sleep = function()
      return true
    end,
  })
  if not task_ok then
    return nil, task_err
  end
  if client.calls ~= 1 then
    return nil, "pcp mapping should not re-enter from its own self-peer update"
  end
  local mappings = host.address_manager:get_public_address_mappings()
  if #mappings ~= 1 or mappings[1] ~= "/ip4/198.51.100.20/tcp/4001" then
    return nil, "pcp mapping should be recorded"
  end

  local auto_host = {
    address_manager = address_manager.new({
      listen_addrs = {
        "/ip4/192.168.1.45/tcp/4001",
      },
    }),
    events = {},
    on = host.on,
    off = host.off,
    emit = host.emit,
    _emit_self_peer_update_if_changed = host._emit_self_peer_update_if_changed,
    _tasks = {},
    spawn_task = host.spawn_task,
    cancel_task = host.cancel_task,
  }
  local auto_svc = assert(pcp_service.new(auto_host, {
    client = client,
    os_routing = {
      snapshot = function()
        return {
          default_route_v4 = { gateway = "192.168.1.254", ifname = "en0" },
          default_route_v6 = nil,
          router_candidates_v6 = {},
        }
      end,
    },
  }))
  local auto_ok, auto_err = auto_svc:start()
  if not auto_ok then
    return nil, auto_err
  end
  local auto_task_ok, auto_task_err = auto_host._tasks[1].fn({
    sleep = function()
      return true
    end,
  })
  if not auto_task_ok then
    return nil, auto_task_err
  end
  if auto_svc._selected_gateway ~= "192.168.1.254" then
    return nil, "pcp auto gateway should select default ipv4 route gateway"
  end
  return true
end

return {
  name = "pcp mapping service",
  run = run,
}
