local ed25519 = require("lua_libp2p.crypto.ed25519")
local keys = require("lua_libp2p.crypto.keys")
local host = require("lua_libp2p.host")
local identify = require("lua_libp2p.protocol.identify")
local upgrader = require("lua_libp2p.network.upgrader")
local perf = require("lua_libp2p.protocol.perf")
local ping = require("lua_libp2p.protocol.ping")

local function run()
  local keypair, key_err = ed25519.generate_keypair()
  if not keypair then
    return nil, key_err
  end

  local bad_host, bad_err = host.new({ runtime = "invalid-runtime" })
  if bad_host ~= nil or not bad_err then
    return nil, "expected invalid runtime to return input error"
  end

  local default_host, default_host_err = host.new({
    identity = keypair,
    blocking = false,
  })
  if not default_host then
    return nil, default_host_err
  end
  local has_luv = pcall(require, "luv")
  local expected_default_runtime = has_luv and "luv" or "poll"
  if default_host._runtime ~= expected_default_runtime then
    return nil, "host should default to luv when available and poll otherwise"
  end

  local bad_circuit_host, bad_circuit_err = host.new({
    runtime = "poll",
    identity = keypair,
    listen_addrs = { "/p2p-circuit" },
    blocking = false,
  })
  if bad_circuit_host ~= nil or not bad_circuit_err then
    return nil, "expected /p2p-circuit listen addr without autorelay to fail"
  end

  local default_discovery_host, default_discovery_host_err = host.new({
    runtime = "poll",
    identity = keypair,
    peer_discovery = { bootstrap = {} },
    blocking = false,
  })
  if not default_discovery_host then
    return nil, default_discovery_host_err
  end
  if not default_discovery_host._bootstrap_discovery
      or type(default_discovery_host._bootstrap_discovery.config.list) ~= "table"
      or #default_discovery_host._bootstrap_discovery.config.list == 0 then
    return nil, "empty bootstrap config should use default bootstrappers"
  end

  local discovery_host, discovery_host_err = host.new({
    runtime = "poll",
    identity = keypair,
    peer_discovery = {
      bootstrap = {
        list = {
          "/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWCryG7Mon9orvQxcS1rYZjotPgpwoJNHHKcLLfE4Hf5mV",
          "/ip4/127.0.0.1/tcp/4002/p2p/12D3KooWQWZLu9qXWPTDnF9rTRrAiVGZrXCbHAvkqYrsG8cW4UHg",
        },
      },
    },
    services = { "kad_dht" },
    blocking = false,
  })
  if not discovery_host then
    return nil, discovery_host_err
  end
  if not discovery_host.peer_discovery then
    return nil, "host should build peer discovery from config"
  end
  if not discovery_host.kad_dht or discovery_host.kad_dht.peer_discovery ~= discovery_host.peer_discovery then
    return nil, "kad_dht service should default to host peer_discovery"
  end

  local bootstrap_dialed = {}
  function discovery_host:dial(target)
    bootstrap_dialed[#bootstrap_dialed + 1] = target
    return {}, { remote_peer_id = target.peer_id }
  end
  discovery_host._bootstrap_discovery.timeout = 0
  local discovery_started, discovery_start_err = discovery_host:start()
  if not discovery_started then
    return nil, discovery_start_err
  end
  local discovery_polled, discovery_poll_err = discovery_host:poll_once(0)
  if not discovery_polled then
    return nil, discovery_poll_err
  end
  if #bootstrap_dialed ~= 2 then
    return nil, "host should spawn scheduler dials for all bootstrap peers after startup delay"
  end
  local bootstrap_tags = discovery_host.peerstore:get_tags(bootstrap_dialed[1].peer_id)
  if not bootstrap_tags.bootstrap or bootstrap_tags.bootstrap.value ~= 50 then
    return nil, "host should tag bootstrap peers"
  end
  local bootstrap_stats = discovery_host:task_stats()
  if bootstrap_stats.by_name["host.bootstrap_dial"] ~= 2 or bootstrap_stats.by_status.completed ~= 2 then
    return nil, "host task stats should include completed bootstrap dial tasks"
  end
  discovery_host:stop()

  local multi_dial_host = assert(host.new({
    runtime = "poll",
    identity = keypair,
    blocking = false,
  }))
  local dialed = {}
  multi_dial_host._tcp_transport = {
    dial = function(endpoint)
      dialed[#dialed + 1] = endpoint.host .. ":" .. tostring(endpoint.port)
      if #dialed == 1 then
        return nil, require("lua_libp2p.error").new("timeout", "tcp connect timed out")
      end
      return { close = function() return true end }
    end,
  }
  local original_upgrade_outbound = upgrader.upgrade_outbound
  local multi_peer_id = "12D3KooWCryG7Mon9orvQxcS1rYZjotPgpwoJNHHKcLLfE4Hf5mV"
  upgrader.upgrade_outbound = function()
    return { close = function() return true end }, { remote_peer_id = multi_peer_id }
  end
  local multi_conn, _, multi_err = multi_dial_host:dial({
    peer_id = multi_peer_id,
    addrs = {
      "/ip4/203.0.113.10/tcp/4001/p2p/" .. multi_peer_id,
      "/ip4/198.51.100.20/tcp/4001/p2p/" .. multi_peer_id,
    },
  })
  upgrader.upgrade_outbound = original_upgrade_outbound
  if not multi_conn then
    return nil, multi_err
  end
  if #dialed ~= 2 or dialed[1] ~= "203.0.113.10:4001" or dialed[2] ~= "198.51.100.20:4001" then
    return nil, "host should try the next peer address when the first dial fails"
  end

  for _, key_type in ipairs({ "rsa", "ecdsa", "secp256k1" }) do
    local identity, identity_err = keys.generate_keypair(key_type)
    if not identity then
      return nil, identity_err
    end
    local typed_host, typed_host_err = host.new({
      runtime = "poll",
      identity = identity,
      blocking = false,
    })
    if not typed_host then
      return nil, typed_host_err
    end
    if typed_host:peer_id().type ~= key_type then
      return nil, "expected host peer id type " .. key_type
    end
  end

  local h, h_err = host.new({
    runtime = "poll",
    identity = keypair,
    listen_addrs = { "/ip4/127.0.0.1/tcp/0" },
    transports = { "tcp" },
    security_transports = { "/plaintext/2.0.0" },
    muxers = { "/yamux/1.0.0" },
    services = { "identify" },
    blocking = false,
    accept_timeout = 0.01,
  })
  if not h then
    return nil, h_err
  end

  local ok, reg_err = h:handle(ping.ID, function()
    return true
  end)
  if not ok then
    return nil, reg_err
  end

  if type(h._handlers[identify.ID]) ~= "function" then
    return nil, "identify service should register /ipfs/id/1.0.0 handler"
  end
  if type(h._handlers[identify.PUSH_ID]) ~= "function" then
    return nil, "identify service should register /ipfs/id/push/1.0.0 handler"
  end

  local svc_ok, svc_err = h:add_service("perf")
  if not svc_ok then
    return nil, svc_err
  end
  if type(h._handlers[perf.ID]) ~= "function" then
    return nil, "perf service should register /perf/1.0.0 handler"
  end

  local dht_svc_ok, dht_svc_err = h:add_service("kad_dht")
  if not dht_svc_ok then
    return nil, dht_svc_err
  end
  if not h.kad_dht or h.kad_dht.mode ~= "client" then
    return nil, "kad_dht host service should default to client mode"
  end
  if h._handlers[h.kad_dht.protocol_id] ~= nil then
    return nil, "client-mode kad_dht service should not advertise/register handler"
  end

  local upnp_mod = require("lua_libp2p.upnp.nat")
  local original_upnp_new = upnp_mod.new
  local upnp_seen_opts
  upnp_mod.new = function(_, opts)
    upnp_seen_opts = opts
    return {
      start = function()
        return true
      end,
    }
  end
  local upnp_host, upnp_host_err = host.new({
    runtime = "poll",
    identity = keypair,
    services = { "upnp_nat" },
    upnp_nat = { internal_client = "192.168.1.124" },
    blocking = false,
  })
  upnp_mod.new = original_upnp_new
  if not upnp_host then
    return nil, upnp_host_err
  end
  if not upnp_seen_opts or upnp_seen_opts.internal_client ~= "192.168.1.124" then
    return nil, "host should pass upnp_nat service options"
  end

  local started, start_err = h:start()
  if not started then
    return nil, start_err
  end

  local addrs = h:get_multiaddrs_raw()
  if #addrs ~= 1 then
    return nil, "expected one listen address"
  end
  if not addrs[1]:match("^/ip4/127.0.0.1/tcp/%d+$") then
    return nil, "unexpected listen address shape"
  end

  local listed = h:get_listen_addrs()
  if #listed ~= 1 or listed[1] ~= addrs[1] then
    return nil, "get_listen_addrs mismatch"
  end

  local local_peer = h:peer_id()
  if type(local_peer) ~= "table" or type(local_peer.id) ~= "string" or local_peer.id == "" then
    return nil, "peer_id method should return local peer id record"
  end
  if not h.peerstore then
    return nil, "expected host peerstore"
  end
  if not h.address_manager then
    return nil, "expected host address manager"
  end

  local addrs_raw = h:get_multiaddrs_raw()
  if #addrs_raw ~= 1 or addrs_raw[1] ~= addrs[1] then
    return nil, "get_multiaddrs_raw mismatch"
  end

  local full_addrs = h:get_multiaddrs()
  if #full_addrs ~= 1 then
    return nil, "expected one full multiaddr"
  end
  if full_addrs[1] ~= addrs[1] .. "/p2p/" .. local_peer.id then
    return nil, "get_multiaddrs should append local peer id"
  end

  h.address_manager:add_relay_addr("/ip4/198.51.100.1/tcp/4001/p2p/relay/p2p-circuit")
  local with_relay = h:get_multiaddrs()
  if #with_relay ~= 2 then
    return nil, "expected listen and relay advertised addrs"
  end
  if with_relay[2] ~= "/ip4/198.51.100.1/tcp/4001/p2p/relay/p2p-circuit/p2p/" .. local_peer.id then
    return nil, "expected relay addr to be advertised with local peer id"
  end
  h.address_manager:remove_relay_addr("/ip4/198.51.100.1/tcp/4001/p2p/relay/p2p-circuit")

  local matched_peer = nil
  local protocol_cb, protocol_cb_err = h:on_protocol("/relay/test/1.0.0", function(peer_id)
    matched_peer = peer_id
    return true
  end)
  if not protocol_cb then
    return nil, protocol_cb_err
  end
  local protocol_handlers = h._event_handlers.peer_protocols_updated
  if type(protocol_handlers) ~= "table" or type(protocol_handlers[1]) ~= "function" then
    return nil, "expected protocol update handler"
  end
  protocol_handlers[1]({
    peer_id = "peer-relay",
    protocols = { "/relay/test/1.0.0" },
    added_protocols = { "/relay/test/1.0.0" },
  })
  if matched_peer ~= "peer-relay" then
    return nil, "on_protocol should match protocol update events"
  end

  h.listen_addrs = {
    "/ip4/203.0.113.1/tcp/4001/p2p/12D3KooWCryG7Mon9orvQxcS1rYZjotPgpwoJNHHKcLLfE4Hf5mV/p2p-circuit",
  }
  local relay_addrs = h:get_multiaddrs()
  if #relay_addrs ~= 1 then
    return nil, "expected one relay multiaddr"
  end
  if relay_addrs[1] ~= h.listen_addrs[1] .. "/p2p/" .. local_peer.id then
    return nil, "get_multiaddrs should append local peer id after p2p-circuit"
  end

  h.listen_addrs = {
    "/ip4/127.0.0.1/tcp/4001/p2p/" .. local_peer.id,
  }
  local terminal = h:get_multiaddrs()
  if #terminal ~= 1 or terminal[1] ~= h.listen_addrs[1] then
    return nil, "get_multiaddrs should preserve terminal p2p address"
  end

  h._start_blocking = true
  h._start_max_iterations = 1
  h._start_poll_interval = 0
  started, start_err = h:start()
  if not started then
    return nil, start_err
  end
  if not h:is_running() then
    return nil, "host should be running after start"
  end

  local stopped, stop_err = h:stop()
  if not stopped then
    return nil, stop_err
  end
  if h:is_running() then
    return nil, "host should not be running after stop"
  end

  local _, _, dial_err = h:dial("12D3KooWCryG7Mon9orvQxcS1rYZjotPgpwoJNHHKcLLfE4Hf5mV")
  if not dial_err then
    return nil, "expected dial error for peer id without address"
  end

  h:close()
  return true
end

return {
  name = "host config and lifecycle",
  run = run,
}
