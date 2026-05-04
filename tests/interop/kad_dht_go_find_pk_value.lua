package.path = table.concat({
  "./?.lua",
  "./?/init.lua",
  package.path,
}, ";")

local host_mod = require("lua_libp2p.host")
local identify_service = require("lua_libp2p.protocol_identify.service")
local kad_dht_service = require("lua_libp2p.kad_dht")
local peer_discovery_bootstrap = require("lua_libp2p.peer_discovery_bootstrap")
local ping_service = require("lua_libp2p.protocol_ping.service")

local bootstrap_addr = arg[1]
local key_hex = arg[2]
if not bootstrap_addr or bootstrap_addr == "" or not key_hex or key_hex == "" then
  io.stderr:write("usage: lua tests/interop/kad_dht_go_find_pk_value.lua <bootstrap-multiaddr> <key-hex>\n")
  os.exit(2)
end

local function hex_to_bytes(hex)
  if (#hex % 2) ~= 0 or hex:match("[^0-9a-fA-F]") then
    return nil
  end
  local out = {}
  for i = 1, #hex, 2 do
    out[#out + 1] = string.char(tonumber(hex:sub(i, i + 1), 16))
  end
  return table.concat(out)
end

local key = hex_to_bytes(key_hex)
if not key then
  io.stderr:write("invalid key hex\n")
  os.exit(2)
end

local host, host_err = host_mod.new({
  runtime = "luv",
  peer_discovery = {
    bootstrap = {
      module = peer_discovery_bootstrap,
      config = {
        list = { bootstrap_addr },
        dialable_only = true,
        ignore_resolve_errors = true,
        dial_on_start = true,
      },
    },
  },
  listen_addrs = { "/ip4/127.0.0.1/tcp/0" },
  services = {
    identify = { module = identify_service },
    ping = { module = ping_service },
    kad_dht = {
      module = kad_dht_service,
      config = {
        mode = "client",
        alpha = 1,
        disjoint_paths = 1,
        address_filter = "all",
      },
    },
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

local op, op_err = host.kad_dht:find_value(key, {
  alpha = 1,
  disjoint_paths = 1,
})
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
if not result.record or type(result.record.value) ~= "string" or result.record.value == "" then
  io.stderr:write("expected /pk value record\n")
  os.exit(1)
end

print("ok")
