package.path = table.concat({
  "./?.lua",
  "./?/init.lua",
  package.path,
}, ";")

local host_mod = require("lua_libp2p.host")
local identify_service = require("lua_libp2p.protocol_identify.service")
local ping_service = require("lua_libp2p.protocol_ping.service")
local kad_dht_service = require("lua_libp2p.kad_dht")
local autorelay_service = require("lua_libp2p.transport_circuit_relay_v2.autorelay")
local peer_discovery_bootstrap = require("lua_libp2p.peer_discovery_bootstrap")
local pcp_service = require("lua_libp2p.pcp.service")
local log = require("lua_libp2p.log")

local function usage()
  io.stderr:write([[
Usage:
  lua examples/pcp_autorelay_parity_demo.lua --gateway <gateway-ip> --internal-client <ip> [--listen-ipv6] [--bootstrap <multiaddr>] [--runtime luv] [--duration 60] [--pcp-delay 5] [--debug]

This keeps the autorelay_dht_demo host wiring and adds PCP mapping.
]])
end

local function parse_args(args)
  local opts = {
    runtime = "luv",
    duration = 60,
    ttl = 7200,
    timeout = 0.25,
    retries = 6,
    listen_ipv6 = false,
    relays = {},
    max_reservations = 2,
    pcp_delay = 0,
  }
  local i = 1
  while i <= #args do
    local name = args[i]
    local value = args[i + 1]
    if name == "--gateway" then
      opts.gateway = value
      i = i + 2
    elseif name == "--internal-client" then
      opts.internal_client = value
      i = i + 2
    elseif name == "--runtime" then
      opts.runtime = value or opts.runtime
      i = i + 2
    elseif name == "--duration" then
      opts.duration = tonumber(value) or opts.duration
      i = i + 2
    elseif name == "--listen-ipv6" then
      opts.listen_ipv6 = true
      i = i + 1
    elseif name == "--bootstrap" then
      opts.bootstrap_addrs = opts.bootstrap_addrs or {}
      opts.bootstrap_addrs[#opts.bootstrap_addrs + 1] = value
      i = i + 2
    elseif name == "--debug" then
      opts.debug = true
      i = i + 1
    elseif name == "--pcp-delay" then
      opts.pcp_delay = tonumber(value) or 0
      i = i + 2
    else
      return nil, "unknown argument: " .. tostring(name)
    end
  end
  if not opts.gateway or opts.gateway == "" then
    return nil, "--gateway is required"
  end
  if not opts.internal_client or opts.internal_client == "" then
    return nil, "--internal-client is required"
  end
  return opts
end

local opts, err = parse_args(arg)
if not opts then
  io.stderr:write(tostring(err) .. "\n")
  usage()
  os.exit(2)
end

if opts.debug then
  log.set_level("debug")
end

local peer_discovery = {
  bootstrap = {
    module = peer_discovery_bootstrap,
    config = {
      dial_on_start = true,
    },
  },
}
if opts.bootstrap_addrs and #opts.bootstrap_addrs > 0 then
  peer_discovery.bootstrap.config.list = opts.bootstrap_addrs
end

local listen_addrs = {
  "/ip4/127.0.0.1/tcp/0",
  "/p2p-circuit",
}
if opts.listen_ipv6 then
  listen_addrs[1] = "/ip4/0.0.0.0/tcp/4001"
  listen_addrs[2] = "/ip6/" .. tostring(opts.internal_client) .. "/tcp/4001"
end

local h, host_err = host_mod.new({
  runtime = opts.runtime,
  peer_discovery = peer_discovery,
  listen_addrs = listen_addrs,
  services = {
    identify = { module = identify_service },
    ping = { module = ping_service },
    kad_dht = { module = kad_dht_service },
    autorelay = { module = autorelay_service },
    pcp = {
      module = pcp_service,
      config = {
        gateway = opts.gateway,
        internal_client = opts.internal_client,
        ttl = opts.ttl,
        timeout = opts.timeout,
        retries = opts.retries,
        map_on_self_peer_update = opts.pcp_delay <= 0,
        initial_map_delay_seconds = opts.pcp_delay,
      },
    },
  },
  kad_dht = {
    mode = "client",
  },
  autorelay = {
    relays = opts.relays,
    max_reservations = opts.max_reservations,
  },
  blocking = false,
  connect_timeout = 6,
  io_timeout = 10,
  accept_timeout = 0.05,
})
if not h then
  io.stderr:write("host init failed: " .. tostring(host_err) .. "\n")
  os.exit(1)
end

h:on("peer_identified", function(payload)
  return true
end)

h:on("peer_identify_failed", function(payload)
  print("peer identify failed: " .. tostring(payload.peer_id) .. " cause=" .. tostring(payload.error_message or payload.error))
  return true
end)

h:on("pcp:mapping:active", function(mapping)
  print("pcp mapping active: " .. tostring(mapping.external_addr))
  return true
end)

assert(h:start())
print("peer id: " .. tostring(h:peer_id().id))

h:sleep(opts.duration, { poll_interval = 0.05 })
h:stop()
