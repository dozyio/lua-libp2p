local ed25519 = require("lua_libp2p.crypto.ed25519")
local host = require("lua_libp2p.host")
local identify = require("lua_libp2p.protocol.identify")
local perf = require("lua_libp2p.protocol.perf")
local ping = require("lua_libp2p.protocol.ping")

local function run()
  local keypair, key_err = ed25519.generate_keypair()
  if not keypair then
    return nil, key_err
  end

  local h, h_err = host.new({
    identity = keypair,
    listen_addrs = { "/ip4/127.0.0.1/tcp/0" },
    transports = { "tcp" },
    security_transports = { "/plaintext/2.0.0" },
    muxers = { "/yamux/1.0.0" },
    services = { "identify" },
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

  local started, start_err = h:start({ blocking = false, accept_timeout = 0.01 })
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

  started, start_err = h:start({ max_iterations = 1, blocking = true, poll_interval = 0 })
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
