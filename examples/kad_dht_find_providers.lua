local common = require("examples.kad_dht_client_common")

local function usage()
  io.stderr:write([[
Usage:
  lua examples/kad_dht_find_providers.lua --key <bytes-as-text> [--bootstrap <multiaddr>] [--limit 5]
  lua examples/kad_dht_find_providers.lua --key-hex <hex> [--bootstrap <multiaddr>] [--limit 5]

The key is sent as the KAD GET_PROVIDERS key bytes. For real IPFS content this
is normally the multihash bytes, not a CID string.
]])
end

local opts, arg_err = common.parse_args(arg)
if not opts then
  io.stderr:write(tostring(arg_err) .. "\n")
  usage()
  os.exit(2)
end

local key, key_err = common.key_from_opts(opts)
if not key then
  io.stderr:write(tostring(key_err) .. "\n")
  usage()
  os.exit(2)
end

local client, client_err = common.new_client(opts)
if not client then
  io.stderr:write("client init failed: " .. tostring(client_err) .. "\n")
  os.exit(1)
end

local started_at = os.time()
local result, providers_err = client.dht:find_providers(key, {
  alpha = opts.alpha,
  disjoint_paths = opts.disjoint_paths,
  limit = opts.limit,
})
if not result then
  common.stop(client)
  io.stderr:write("find_providers failed: " .. tostring(providers_err) .. "\n")
  os.exit(1)
end

common.print_lookup("find_providers", result.lookup)
io.stdout:write("  duration: " .. tostring(os.time() - started_at) .. "s\n")
io.stdout:write("providers: " .. tostring(#(result.providers or {})) .. "\n")
for _, provider in ipairs(result.providers or {}) do
  io.stdout:write("  " .. tostring(provider.peer_id) .. " addrs=" .. tostring(#(provider.addrs or {})) .. "\n")
end

common.stop(client)
