package.path = table.concat({
  "./?.lua",
  "./?/init.lua",
  package.path,
}, ";")

local ed25519 = require("lua_libp2p.crypto.ed25519")
local host_mod = require("lua_libp2p.host")
local identify_service = require("lua_libp2p.protocol_identify.service")
local ping_service = require("lua_libp2p.protocol_ping.service")
local dcutr_service = require("lua_libp2p.protocol_dcutr.service")
local dcutr = require("lua_libp2p.protocol_dcutr.protocol")
local multiaddr = require("lua_libp2p.multiaddr")

local target_addr = arg[1]
if not target_addr or target_addr == "" then
  io.stderr:write("usage: lua tests/interop/dcutr_unilateral_go_client.lua <go-multiaddr-with-p2p>\n")
  os.exit(2)
end

local runtime = os.getenv("LUA_LIBP2P_INTEROP_RUNTIME") or "luv"
if runtime ~= "luv" then
  io.stderr:write("invalid LUA_LIBP2P_INTEROP_RUNTIME, expected 'luv'\n")
  os.exit(2)
end

local parsed_target = assert(multiaddr.parse(target_addr), "target address must be a valid multiaddr")
local peer_id = nil
for _, component in ipairs(parsed_target.components or {}) do
  if component.protocol == "p2p" and type(component.value) == "string" and component.value ~= "" then
    peer_id = component.value
  end
end
assert(peer_id, "target address must include /p2p")
local identity = assert(ed25519.generate_keypair())
local host, host_err = host_mod.new({
  runtime = runtime,
  identity = identity,
  blocking = false,
  services = {
    identify = identify_service,
    ping = { module = ping_service },
    dcutr = {
      module = dcutr_service,
      config = {
        auto_on_relay_connection = true,
        allow_private_candidate_addrs = true,
        relay_grace_seconds = 0,
      },
    },
  },
})
if not host then
  io.stderr:write(tostring(host_err) .. "\n")
  os.exit(1)
end

assert(host:start())
assert(host.peerstore:add_addrs(peer_id, { target_addr }, { ttl = false }))
assert(host.peerstore:add_protocols(peer_id, { dcutr.ID, "/ipfs/ping/1.0.0" }))

local migrated = false
local direct_connection_id = nil
local last_failure = nil
local last_precheck = nil
host:on("dcutr:migrated", function(payload)
  if payload and payload.peer_id == peer_id and payload.direction == "inbound_unilateral" then
    migrated = true
  end
  return true
end)
host:on("dcutr:attempt:failed", function(payload)
  last_failure = payload and (payload.error or payload.reason) or "failed"
  return true
end)
host:on("dcutr:unilateral:failed", function(payload)
  last_failure = payload and payload.reason or "unilateral_failed"
  return true
end)
host:on("dcutr:attempt:precheck", function(payload)
  last_precheck = payload
  return true
end)
host:on("connection_opened", function(payload)
  local state = payload and payload.state or {}
  if payload and payload.peer_id == peer_id and not host:_connection_is_limited(state) then
    direct_connection_id = payload.connection_id
  end
  return true
end)

assert(host:emit("peer_connected", {
  peer_id = peer_id,
  state = {
    connection_id = 999999,
    remote_peer_id = peer_id,
    direction = "inbound",
    relay = { limit_kind = "limited" },
  },
}))

local deadline = os.time() + 8
while os.time() < deadline and (not migrated or not direct_connection_id) do
  local ok, err = host:poll_once(0.02)
  if not ok then
    host:stop()
    io.stderr:write(tostring(err) .. "\n")
    os.exit(1)
  end
end

if not migrated then
  host:stop()
  io.stderr:write(
    "unilateral migration did not complete: "
      .. tostring(last_failure)
      .. " precheck_candidates="
      .. tostring(last_precheck and last_precheck.remote_candidate_addrs)
      .. " supports="
      .. tostring(last_precheck and last_precheck.remote_supports_dcutr)
      .. " local_obs="
      .. tostring(last_precheck and last_precheck.local_observed_addrs)
      .. "\n"
  )
  os.exit(1)
end
if not direct_connection_id then
  host:stop()
  io.stderr:write("unilateral migration did not open a direct connection\n")
  os.exit(1)
end

local ping_result, ping_err = host.ping:ping(peer_id, { timeout = 3, require_unlimited_connection = true })
if not ping_result then
  host:stop()
  io.stderr:write(tostring(ping_err) .. "\n")
  os.exit(1)
end

host:stop()
print("ok")
