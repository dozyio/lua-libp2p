package.path = table.concat({
  "./?.lua",
  "./?/init.lua",
  package.path,
}, ";")

local host_mod = require("lua_libp2p.host")
local identify_service = require("lua_libp2p.protocol_identify.service")
local kad_dht_service = require("lua_libp2p.kad_dht")
local multihash = require("lua_libp2p.multiformats.multihash")
local ping_service = require("lua_libp2p.protocol_ping.service")

local function hex_encode(value)
  return (value:gsub(".", function(c)
    return string.format("%02x", c:byte())
  end))
end

local key = assert(multihash.sha2_256("lua-libp2p-dht-provider-interop-reverse"))

local function on_started(h)
  local addrs = h:get_multiaddrs()
  if #addrs == 0 then
    io.stderr:write("host has no listen addresses\n")
    os.exit(1)
  end
  local added, add_err = h.kad_dht:add_provider(key, {
    peer_id = h:peer_id().id,
    addrs = addrs,
  }, { ttl = false })
  if not added then
    io.stderr:write(tostring(add_err) .. "\n")
    os.exit(1)
  end
  io.stdout:write(addrs[1] .. "\n")
  io.stdout:write(hex_encode(key) .. "\n")
  io.stdout:flush()
end

local host, host_err = host_mod.new({
  runtime = "luv",
  listen_addrs = { "/ip4/127.0.0.1/tcp/0" },
  services = {
    identify = { module = identify_service },
    ping = { module = ping_service },
    kad_dht = {
      module = kad_dht_service,
      config = {
        mode = "server",
        address_filter = "all",
        provider_ttl_seconds = false,
      },
    },
  },
  blocking = true,
  poll_interval = 0.01,
  accept_timeout = 0.05,
  io_timeout = 6,
  on_started = on_started,
})
if not host then
  io.stderr:write(tostring(host_err) .. "\n")
  os.exit(1)
end

local started, start_err = host:start()
if not started then
  io.stderr:write(tostring(start_err) .. "\n")
  os.exit(1)
end
