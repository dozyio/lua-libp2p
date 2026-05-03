local ed25519 = require("lua_libp2p.crypto.ed25519")
local host_mod = require("lua_libp2p.host")
local dcutr_service = require("lua_libp2p.protocol_dcutr.service")
local identify_service = require("lua_libp2p.protocol_identify.service")

local function run()
  local keypair = assert(ed25519.generate_keypair())
  local host, host_err = host_mod.new({
    runtime = "poll",
    identity = keypair,
    services = {
      identify = { module = identify_service },
      dcutr = {
        module = dcutr_service,
        config = {
          auto_on_relay_connection = true,
          allow_private_obs_addrs = true,
        },
      },
    },
    blocking = false,
  })
  if not host then
    return nil, host_err
  end

  local calls = 0
  local original_get_multiaddrs = host.get_multiaddrs
  host.get_multiaddrs = function()
    return { "/ip4/192.168.1.124/tcp/4001" }
  end
  local original_supports_protocol = host.peerstore.supports_protocol
  host.peerstore.supports_protocol = function(_, peer_id, proto)
    if proto == "/libp2p/dcutr" then
      return peer_id == "peer-inbound"
    end
    return original_supports_protocol(host.peerstore, peer_id, proto)
  end

  local original = host.dcutr.start_hole_punch
  host.dcutr.start_hole_punch = function(_, peer_id, opts)
    calls = calls + 1
    local task, task_err = host:spawn_task("test.dcutr.mock", function()
      return { peer_id = peer_id, opts = opts }
    end, { service = "test" })
    if not task then
      return nil, task_err
    end
    return task
  end

  local ok, emit_err = host:emit("peer_connected", {
    peer_id = "peer-inbound",
    state = {
      direction = "inbound",
      relay = { limit_kind = "limited" },
    },
  })
  if not ok then
    return nil, emit_err
  end
  ok, emit_err = host:poll_once(0)
  if not ok then
    return nil, emit_err
  end

  ok, emit_err = host:emit("peer_connected", {
    peer_id = "peer-outbound",
    state = {
      direction = "outbound",
      relay = { limit_kind = "limited" },
    },
  })
  if not ok then
    return nil, emit_err
  end

  ok, emit_err = host:emit("peer_connected", {
    peer_id = "peer-direct",
    state = {
      direction = "inbound",
      relay = { limit_kind = "unlimited" },
    },
  })
  if not ok then
    return nil, emit_err
  end

  host.dcutr.start_hole_punch = original
  host.get_multiaddrs = original_get_multiaddrs
  host.peerstore.supports_protocol = original_supports_protocol

  if calls ~= 1 then
    return nil, "expected exactly one auto dcutr trigger for inbound limited relay connection"
  end

  return true
end

return {
  name = "dcutr auto policy inbound-only",
  run = run,
}
