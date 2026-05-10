local common = require("examples.kad_dht_client_common")

local function usage()
  io.stderr:write([[Usage:
  lua examples/kad_dht_query_multiaddrs.lua <bootstrap-multiaddr> [bootstrap-multiaddr ...] [--target <peer-id>] [--count 20]

Example:
  lua examples/kad_dht_query_multiaddrs.lua \
    /ip4/127.0.0.1/tcp/4001/p2p/12D3KooW... \
    /ip6/::1/tcp/4001/p2p/12D3KooW...
]])
end

local function parse_args(args)
  local forwarded = {}
  local bootstrappers = {}
  local i = 1
  while i <= #args do
    local token = args[i]
    if type(token) == "string" and token:sub(1, 1) == "/" then
      bootstrappers[#bootstrappers + 1] = token
      i = i + 1
    else
      forwarded[#forwarded + 1] = token
      if
        token == "--bootstrap"
        or token == "--target"
        or token == "--count"
        or token == "--alpha"
        or token == "--disjoint-paths"
        or token == "--connect-timeout"
        or token == "--io-timeout"
        or token == "--identity-key"
        or token == "--key"
        or token == "--key-hex"
        or token == "--limit"
        or token == "--persist-peerstore"
      then
        local value = args[i + 1]
        if value and value:sub(1, 2) ~= "--" then
          forwarded[#forwarded + 1] = value
          i = i + 2
        else
          i = i + 1
        end
      else
        i = i + 1
      end
    end
  end

  local opts, err = common.parse_args(forwarded)
  if not opts then
    return nil, err
  end
  for _, ma in ipairs(bootstrappers) do
    opts.bootstrappers[#opts.bootstrappers + 1] = ma
  end
  if #opts.bootstrappers == 0 then
    return nil, "at least one bootstrap multiaddr is required"
  end
  return opts
end

local opts, arg_err = parse_args(arg)
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
  io.stderr:write("query failed: " .. tostring(result_err) .. "\n")
  os.exit(1)
end

common.print_lookup("query", report)
io.stdout:write("closest peers:\n")
for _, peer in ipairs(peers) do
  io.stdout:write("  " .. tostring(peer.peer_id) .. " addrs=" .. tostring(#(peer.addrs or {})) .. "\n")
end

common.stop(client)
