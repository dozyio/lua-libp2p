local common = require("examples.kad_dht_client_common")

local function usage()
  io.stderr:write([[
Usage:
  lua examples/kad_dht_find_value.lua --key <bytes-as-text> [--bootstrap <multiaddr>]
  lua examples/kad_dht_find_value.lua --key-hex <hex> [--bootstrap <multiaddr>]

This exercises iterative GET_VALUE. Arbitrary keys usually return no record;
use this to verify lookup behavior and record retrieval when you know a DHT key.
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
local result, value_err = common.run_task(client.host, "example.find_value", function(ctx)
  return client.dht:find_value(key, {
    alpha = opts.alpha,
    disjoint_paths = opts.disjoint_paths,
    scheduler_task = true,
    ctx = ctx,
  })
end)
if not result then
  common.stop(client)
  io.stderr:write("find_value failed: " .. tostring(value_err) .. "\n")
  os.exit(1)
end

common.print_lookup("find_value", result.lookup)
io.stdout:write("  duration: " .. tostring(os.time() - started_at) .. "s\n")
if result.record then
  io.stdout:write("record found:\n")
  io.stdout:write("  key_bytes: " .. tostring(#(result.record.key or "")) .. "\n")
  io.stdout:write("  value_bytes: " .. tostring(#(result.record.value or "")) .. "\n")
else
  io.stdout:write("record found: no\n")
end

common.stop(client)
