local address_manager = require("lua_libp2p.address_manager")
local pcp_service = require("lua_libp2p.pcp.service")

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
      return self:emit("self_peer_update", {})
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
  if client.calls ~= 1 then
    return nil, "pcp mapping should not re-enter from its own self-peer update"
  end
  local mappings = host.address_manager:get_public_address_mappings()
  if #mappings ~= 1 or mappings[1] ~= "/ip4/198.51.100.20/tcp/4001" then
    return nil, "pcp mapping should be recorded"
  end
  return true
end

return {
  name = "pcp mapping service",
  run = run,
}
