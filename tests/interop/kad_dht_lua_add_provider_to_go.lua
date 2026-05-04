package.path = table.concat({ "./?.lua", "./?/init.lua", package.path }, ";")

local host_mod = require("lua_libp2p.host")
local identify_service = require("lua_libp2p.protocol_identify.service")
local kad_dht_service = require("lua_libp2p.kad_dht")
local multihash = require("lua_libp2p.multiformats.multihash")
local ping_service = require("lua_libp2p.protocol_ping.service")

local bootstrap_addr = arg[1]
if not bootstrap_addr or bootstrap_addr == "" then
  io.stderr:write("usage: lua tests/interop/kad_dht_lua_add_provider_to_go.lua <bootstrap-multiaddr>\n")
  os.exit(2)
end
local peer_id = bootstrap_addr:match("/p2p/([^/]+)$")
if not peer_id then
  io.stderr:write("bootstrap multiaddr must include /p2p/<peer-id>\n")
  os.exit(2)
end

local host, host_err = host_mod.new({
  runtime = "luv",
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
local key = assert(multihash.sha2_256("lua-add-provider-to-go"))
local target = { peer_id = peer_id, addrs = { bootstrap_addr } }
local provide_op, provide_err = host.kad_dht:provide(key, { peers = { target }, count = 1 })
if not provide_op then
  io.stderr:write(tostring(provide_err) .. "\n")
  host:stop()
  os.exit(1)
end
local provide_result, provide_result_err = provide_op:result({ timeout = 8, poll_interval = 0.02 })
if not provide_result or provide_result.succeeded ~= 1 then
  io.stderr:write(tostring(provide_result_err or "expected add provider to succeed") .. "\n")
  if provide_result then
    io.stderr:write("attempted=" .. tostring(provide_result.attempted) .. " succeeded=" .. tostring(provide_result.succeeded) .. " failed=" .. tostring(provide_result.failed) .. "\n")
    for _, err in ipairs(provide_result.errors or {}) do
      io.stderr:write(tostring(err) .. "\n")
    end
  end
  host:stop()
  os.exit(1)
end
local find_op, find_err = host.kad_dht:find_providers(key, { peers = { target }, limit = 1, alpha = 1, disjoint_paths = 1 })
if not find_op then
  io.stderr:write(tostring(find_err) .. "\n")
  host:stop()
  os.exit(1)
end
local found, found_err = find_op:result({ timeout = 8, poll_interval = 0.02 })
host:stop()
if not found then
  io.stderr:write(tostring(found_err) .. "\n")
  os.exit(1)
end
if #(found.providers or {}) < 1 or found.providers[1].peer_id ~= host:peer_id().id then
  io.stderr:write("expected Go server to return Lua provider after ADD_PROVIDER\n")
  io.stderr:write("providers=" .. tostring(#(found.providers or {})) .. "\n")
  for _, provider in ipairs(found.providers or {}) do
    io.stderr:write("provider=" .. tostring(provider.peer_id) .. " addrs=" .. tostring(#(provider.addrs or {})) .. "\n")
  end
  os.exit(1)
end
print("ok")
