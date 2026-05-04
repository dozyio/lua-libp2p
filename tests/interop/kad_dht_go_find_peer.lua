package.path = table.concat({ "./?.lua", "./?/init.lua", package.path }, ";")

local host_mod = require("lua_libp2p.host")
local identify_service = require("lua_libp2p.protocol_identify.service")
local kad_dht_service = require("lua_libp2p.kad_dht")
local peer_discovery_bootstrap = require("lua_libp2p.peer_discovery_bootstrap")
local ping_service = require("lua_libp2p.protocol_ping.service")

local bootstrap_addr = arg[1]
if not bootstrap_addr or bootstrap_addr == "" then
  io.stderr:write("usage: lua tests/interop/kad_dht_go_find_peer.lua <bootstrap-multiaddr>\n")
  os.exit(2)
end
local peer_id = bootstrap_addr:match("/p2p/([^/]+)$")
if not peer_id then
  io.stderr:write("bootstrap multiaddr must include /p2p/<peer-id>\n")
  os.exit(2)
end

local host, host_err = host_mod.new({
  runtime = "luv",
  peer_discovery = {
    bootstrap = { module = peer_discovery_bootstrap, config = { list = { bootstrap_addr }, dialable_only = true, ignore_resolve_errors = true, dial_on_start = true } },
  },
  listen_addrs = { "/ip4/127.0.0.1/tcp/0" },
  services = {
    identify = { module = identify_service },
    ping = { module = ping_service },
    kad_dht = { module = kad_dht_service, config = { mode = "client", alpha = 1, disjoint_paths = 1, address_filter = "all" } },
  },
  blocking = false,
  connect_timeout = 4,
  io_timeout = 6,
  accept_timeout = 0.05,
})
if not host then
  io.stderr:write(tostring(host_err) .. "\n")
  os.exit(1)
end
local started, start_err = host:start()
if not started then
  io.stderr:write(tostring(start_err) .. "\n")
  host:stop()
  os.exit(1)
end
local deadline = os.time() + 8
while os.time() < deadline and #(host.kad_dht.routing_table:all_peers()) == 0 do
  host:sleep(0.05)
end
local op, op_err = host.kad_dht:find_peer(peer_id, { use_cache = false, peers = { { peer_id = peer_id, addrs = { bootstrap_addr } } }, alpha = 1, disjoint_paths = 1 })
if not op then
  io.stderr:write(tostring(op_err) .. "\n")
  host:stop()
  os.exit(1)
end
local result, result_err = op:result({ timeout = 8, poll_interval = 0.02 })
host:stop()
if not result then
  io.stderr:write(tostring(result_err) .. "\n")
  os.exit(1)
end
if not result.peer or result.peer.peer_id ~= peer_id then
  io.stderr:write("expected target peer in find_peer result\n")
  os.exit(1)
end
print("ok")
