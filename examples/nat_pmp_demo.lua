package.path = table.concat({
  "./?.lua",
  "./?/init.lua",
  package.path,
}, ";")

local host_mod = require("lua_libp2p.host")
local identify_service = require("lua_libp2p.protocol_identify.service")
local ping_service = require("lua_libp2p.protocol_ping.service")
local autonat_service = require("lua_libp2p.autonat.client")
local nat_pmp_service = require("lua_libp2p.nat_pmp.service")

local function usage()
  io.stderr:write([[
Usage:
  lua examples/nat_pmp_demo.lua --gateway <gateway-ip> [--internal-client <lan-ip>] [--port 4001]
  lua examples/nat_pmp_demo.lua --gateway 192.168.1.254 --internal-client 192.168.1.124 --autonat-server <peer-id-or-multiaddr>

Creates NAT-PMP TCP mappings for eligible private listen addresses and prints
mapping events and address-manager state. AutoNAT is optional and can be used
to verify public reachability.
]])
end

local function parse_args(args)
  local opts = {
    port = 4001,
    duration = 60,
    runtime = "auto",
    ttl = 7200,
    timeout = 0.25,
    retries = 6,
    auto_confirm = false,
    drain_seconds = 3,
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
    elseif name == "--listen-addr" then
      opts.listen_addr = value
      i = i + 2
    elseif name == "--port" then
      opts.port = tonumber(value) or opts.port
      i = i + 2
    elseif name == "--duration" then
      opts.duration = tonumber(value) or opts.duration
      i = i + 2
    elseif name == "--ttl" then
      opts.ttl = tonumber(value) or opts.ttl
      i = i + 2
    elseif name == "--timeout" then
      opts.timeout = tonumber(value) or opts.timeout
      i = i + 2
    elseif name == "--retries" then
      opts.retries = tonumber(value) or opts.retries
      i = i + 2
    elseif name == "--runtime" then
      opts.runtime = value
      i = i + 2
    elseif name == "--auto-confirm" then
      opts.auto_confirm = true
      i = i + 1
    elseif name == "--autonat-server" then
      opts.autonat_server = value
      i = i + 2
    elseif name == "--drain-seconds" then
      opts.drain_seconds = tonumber(value) or opts.drain_seconds
      i = i + 2
    elseif name == "--help" or name == "-h" then
      usage()
      os.exit(0)
    else
      return nil, "unknown argument: " .. tostring(name)
    end
  end
  if not opts.gateway or opts.gateway == "" then
    return nil, "--gateway is required"
  end
  if not opts.listen_addr then
    opts.listen_addr = "/ip4/0.0.0.0/tcp/" .. tostring(opts.port)
  end
  return opts
end

local function wait_for_duration(host, seconds)
  local task, task_err = host:spawn_task("example.sleep", function(ctx)
    local ok, err = ctx:sleep(seconds)
    if ok == nil and err then
      return nil, err
    end
    return true
  end, { service = "example" })
  if not task then
    return nil, task_err
  end
  return host:run_until_task(task, { poll_interval = 0.05 })
end

local opts, opts_err = parse_args(arg)
if not opts then
  io.stderr:write(tostring(opts_err) .. "\n")
  usage()
  os.exit(2)
end

local h, host_err = host_mod.new({
  runtime = opts.runtime,
  listen_addrs = { opts.listen_addr },
  services = {
    identify = { module = identify_service },
    ping = { module = ping_service },
    autonat = { module = autonat_service },
    nat_pmp = {
      module = nat_pmp_service,
      config = {
        gateway = opts.gateway,
        internal_client = opts.internal_client,
        ttl = opts.ttl,
        timeout = opts.timeout,
        retries = opts.retries,
        auto_confirm_address = opts.auto_confirm,
      },
    },
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

h:on("nat_pmp:mapping:active", function(mapping)
  print("nat-pmp mapping active: " .. tostring(mapping.external_addr))
  print("  internal: " .. tostring(mapping.internal_client) .. ":" .. tostring(mapping.internal_port))
  print("  external port: " .. tostring(mapping.external_port))
  return true
end)

h:on("nat_pmp:mapping:failed", function(payload)
  print("nat-pmp mapping failed: " .. tostring(payload.error_message or payload.error))
  return true
end)

h:on("autonat:address:reachable", function(payload)
  print("autonat reachable: " .. tostring(payload.addr))
  return true
end)

h:on("autonat:address:unreachable", function(payload)
  print("autonat unreachable: " .. tostring(payload.addr))
  return true
end)

local started, start_err = h:start()
if not started then
  io.stderr:write("host start failed: " .. tostring(start_err) .. "\n")
  os.exit(1)
end

print("peer id: " .. tostring(h:peer_id().id))
print("listen addrs:")
for _, addr in ipairs(h:get_multiaddrs()) do
  print("  " .. tostring(addr))
end

if opts.autonat_server and h.autonat then
  local check_task, check_task_err = h.autonat:start_check(opts.autonat_server, {
    timeout = 8,
  })
  if check_task then
    local ok, run_err = h:run_until_task(check_task, { poll_interval = 0.05 })
    if not ok then
      print("autonat check failed: " .. tostring(run_err))
    end
  elseif check_task_err then
    print("autonat check spawn failed: " .. tostring(check_task_err))
  end
end

local wait_ok, wait_err = wait_for_duration(h, opts.duration)
if not wait_ok then
  print("wait failed: " .. tostring(wait_err))
end

if opts.drain_seconds and opts.drain_seconds > 0 then
  wait_for_duration(h, opts.drain_seconds)
end

if h.address_manager and type(h.address_manager.get_public_addresses) == "function" then
  print("address manager public addresses:")
  for _, item in ipairs(h.address_manager:get_public_addresses()) do
    print("  " .. tostring(item.addr) .. " status=" .. tostring(item.status) .. " source=" .. tostring(item.source))
  end
end

h:stop()
