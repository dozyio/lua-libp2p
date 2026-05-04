local common = require("examples.kad_dht_client_common")

local function usage()
  io.stderr:write([[
Usage:
  lua examples/kad_dht_get_closest_peers.lua [--target <peer-id>] [--bootstrap <multiaddr>] [--count 20] [--identity-key <path>] [--persist-peerstore]
  lua examples/kad_dht_get_closest_peers.lua --persist-peerstore <path>

If --target is omitted, the lookup targets this host's own peer id.
Repeat --bootstrap to seed from additional concrete /ip4|/dns/.../tcp/.../p2p/... addrs.
The local peer id is stable across runs by default via .lua-libp2p-kad-client.key.
Use --persist-peerstore to keep discovered peer addresses/protocols in a SQLite-backed peerstore.
]])
end

local opts, arg_err = common.parse_args(arg)
if not opts then
  io.stderr:write(tostring(arg_err) .. "\n")
  usage()
  os.exit(2)
end

local client, client_err = common.new_client(opts)
if not client then
  io.stderr:write("client init failed: " .. tostring(client_err) .. "\n")
  os.exit(1)
end

local target = opts.target or client.host:peer_id().id
io.stdout:write("target: " .. tostring(target) .. "\n")

local started_at = os.time()
local op, op_err = client.dht:get_closest_peers(target, {
  alpha = opts.alpha,
  disjoint_paths = opts.disjoint_paths,
  count = opts.count,
})
local peers, report, result_err
if op then
  peers, report, result_err = op:result({ timeout = opts.timeout })
else
  result_err = op_err
end
if not peers then
  common.stop(client)
  io.stderr:write("get_closest_peers failed: " .. tostring(result_err) .. "\n")
  os.exit(1)
end

common.print_lookup("get_closest_peers", report)
io.stdout:write("  duration: " .. tostring(os.time() - started_at) .. "s\n")
io.stdout:write("closest peers:\n")
for _, peer in ipairs(peers) do
  io.stdout:write("  " .. tostring(peer.peer_id) .. " addrs=" .. tostring(#(peer.addrs or {})) .. "\n")
end

common.stop(client)
