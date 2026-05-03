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
          relay_grace_seconds = 0,
          allow_private_obs_addrs = true,
        },
      },
    },
    blocking = false,
  })
  if not host then
    return nil, host_err
  end

  local start_hole_punch_calls = 0
  local original_get_multiaddrs = host.get_multiaddrs
  host.get_multiaddrs = function()
    return { "/ip4/192.168.1.124/tcp/4001" }
  end
  local original_supports_protocol = host.peerstore.supports_protocol
  host.peerstore.supports_protocol = function(_, peer_id, proto)
    if proto == "/libp2p/dcutr" then
      return true
    end
    return original_supports_protocol(host.peerstore, peer_id, proto)
  end

  local original_start_hole_punch = host.dcutr.start_hole_punch
  host.dcutr.start_hole_punch = function(self, peer_id, opts)
    start_hole_punch_calls = start_hole_punch_calls + 1
    return host:spawn_task("test.dcutr.fallback", function()
      return { peer_id = peer_id, opts = opts }
    end, { service = "test" })
  end

  local original_get_addrs = host.peerstore.get_addrs
  host.peerstore.get_addrs = function(_, peer_id)
    if peer_id == "peer-unilateral" then
      return { "/ip4/8.8.8.8/tcp/4001" }
    end
    return {}
  end

  local dial_calls = 0
  local original_dial = host.dial
  host.dial = function(_, target)
    dial_calls = dial_calls + 1
    if type(target) == "table" and target.peer_id == "peer-unilateral" then
      return {}, { remote_peer_id = target.peer_id }
    end
    return nil, nil, "forced dial failure"
  end

  local ok, emit_err = host:emit("peer_connected", {
    peer_id = "peer-unilateral",
    state = {
      direction = "inbound",
      relay = { limit_kind = "limited" },
      connection_id = 1001,
      remote_addr = "/ip4/1.2.3.4/tcp/4001",
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
    peer_id = "peer-fallback",
    state = {
      direction = "inbound",
      relay = { limit_kind = "limited" },
      connection_id = 1002,
      remote_addr = "/ip4/1.2.3.4/tcp/4001",
    },
  })
  if not ok then
    return nil, emit_err
  end
  ok, emit_err = host:poll_once(0)
  if not ok then
    return nil, emit_err
  end

  host.dial = original_dial
  host.peerstore.get_addrs = original_get_addrs
  host.peerstore.supports_protocol = original_supports_protocol
  host.get_multiaddrs = original_get_multiaddrs
  host.dcutr.start_hole_punch = original_start_hole_punch

  if dial_calls == 0 then
    return nil, "expected unilateral direct dial attempt"
  end
  if start_hole_punch_calls ~= 1 then
    return nil, "expected fallback dcutr task for peer without unilateral candidates"
  end

  return true
end

return {
  name = "dcutr unilateral then fallback policy",
  run = run,
}
