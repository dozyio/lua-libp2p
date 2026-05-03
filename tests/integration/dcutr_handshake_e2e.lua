local ed25519 = require("lua_libp2p.crypto.ed25519")
local host_mod = require("lua_libp2p.host")
local dcutr_service = require("lua_libp2p.protocol_dcutr.service")
local identify_service = require("lua_libp2p.protocol_identify.service")

local function new_host()
  local identity = assert(ed25519.generate_keypair())
  local host, host_err = host_mod.new({
    runtime = "poll",
    identity = identity,
    blocking = false,
    listen_addrs = { "/ip4/127.0.0.1/tcp/0" },
    services = {
      identify = { module = identify_service },
      dcutr = { module = dcutr_service, config = { auto_on_relay_connection = false } },
    },
  })
  if not host then
    return nil, host_err
  end
  local ok, start_err = host:start()
  if not ok then
    return nil, start_err
  end
  local addrs = host:get_multiaddrs()
  if #addrs == 0 then
    host:stop()
    return nil, "expected listening multiaddr"
  end
  return host, addrs[1]
end

local function run()
  if os.getenv("LUA_LIBP2P_ENABLE_DCUTR_E2E") ~= "1" then
    return true
  end

  local host_a, addr_a_or_err = new_host()
  if not host_a then
    return nil, addr_a_or_err
  end
  local host_b, addr_b_or_err = new_host()
  if not host_b then
    host_a:stop()
    return nil, addr_b_or_err
  end
  local addr_b = addr_b_or_err

  local inbound_sync_seen = false
  local outbound_success_seen = false

  local orig_attempt_a = host_a.dcutr._attempt_direct_dials
  local orig_attempt_b = host_b.dcutr._attempt_direct_dials

  host_a.dcutr._attempt_direct_dials = function(self, peer_id, addrs, opts)
    outbound_success_seen = type(peer_id) == "string" and #addrs > 0
    return { connection = {}, state = { remote_peer_id = peer_id }, addr = addrs[1] }
  end

  host_b.dcutr._attempt_direct_dials = function(self, peer_id, addrs, opts)
    inbound_sync_seen = type(peer_id) == "string"
    return { connection = {}, state = { remote_peer_id = peer_id }, addr = addrs[1] or "/ip4/127.0.0.1/tcp/1" }
  end

  local task, task_err = host_a.dcutr:start_hole_punch({
    peer_id = host_b:peer_id().id,
    addrs = { addr_b },
  }, {
    max_attempts = 1,
    timeout = 4,
    dial_timeout = 1,
  })
  if not task then
    host_a.dcutr._attempt_direct_dials = orig_attempt_a
    host_b.dcutr._attempt_direct_dials = orig_attempt_b
    host_b:stop()
    host_a:stop()
    return nil, task_err
  end

  local result, run_err = host_a:run_until_task(task, { poll_interval = 0.01, timeout = 6 })

  host_a.dcutr._attempt_direct_dials = orig_attempt_a
  host_b.dcutr._attempt_direct_dials = orig_attempt_b
  host_b:stop()
  host_a:stop()

  if not result then
    return nil, run_err
  end
  if not outbound_success_seen then
    return nil, "expected outbound dcutr direct dial phase to run"
  end
  if not inbound_sync_seen then
    return nil, "expected inbound dcutr sync/direct dial phase to run"
  end

  return true
end

return {
  name = "dcutr connect/sync handshake e2e",
  run = run,
}
