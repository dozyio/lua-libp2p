local common = require("examples.kad_dht_client_common")

local function usage()
  io.stderr:write([[
Usage:
  lua examples/kad_dht_get_closest_peers.lua [--target <peer-id>] [--bootstrap <multiaddr>] [--count 20]

If --target is omitted, the lookup targets this host's own peer id.
Repeat --bootstrap to seed from additional concrete /ip4|/dns/.../tcp/.../p2p/... addrs.
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
local task_result, task_err = common.run_task(client.host, "example.get_closest_peers", function(ctx)
  local task_peers, task_lookup = client.dht:get_closest_peers(target, {
    alpha = opts.alpha,
    disjoint_paths = opts.disjoint_paths,
    count = opts.count,
    scheduler_task = true,
    ctx = ctx,
  })
  if not task_peers then
    return nil, task_lookup
  end
  return { peers = task_peers, lookup = task_lookup }
end)
local peers = task_result and task_result.peers
local lookup = task_result and task_result.lookup or task_err
if not peers then
  common.stop(client)
  io.stderr:write("get_closest_peers failed: " .. tostring(lookup) .. "\n")
  os.exit(1)
end

common.print_lookup("get_closest_peers", lookup)
io.stdout:write("  duration: " .. tostring(os.time() - started_at) .. "s\n")
io.stdout:write("closest peers:\n")
for _, peer in ipairs(peers) do
  io.stdout:write("  " .. tostring(peer.peer_id) .. " addrs=" .. tostring(#(peer.addrs or {})) .. "\n")
end

common.stop(client)
