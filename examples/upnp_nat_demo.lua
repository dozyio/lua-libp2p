package.path = table.concat({
  "./?.lua",
  "./?/init.lua",
  package.path,
}, ";")

local host_mod = require("lua_libp2p.host")
local identify_service = require("lua_libp2p.protocol_identify.service")
local ping_service = require("lua_libp2p.protocol_ping.service")
local autonat_service = require("lua_libp2p.autonat.client")
local kad_dht_service = require("lua_libp2p.kad_dht")
local upnp_nat_service = require("lua_libp2p.upnp.nat")
local peer_discovery_bootstrap = require("lua_libp2p.peer_discovery_bootstrap")

local function usage()
  io.stderr:write([[
Usage:
  lua examples/upnp_nat_demo.lua [--port 4001] [--auto-confirm] [--duration 60]
  lua examples/upnp_nat_demo.lua --listen-addr /ip4/<lan-ip>/tcp/4001 [--auto-confirm]
  lua examples/upnp_nat_demo.lua --autonat-server <peer-id-or-multiaddr>
  lua examples/upnp_nat_demo.lua --discover-autonat [--bootstrap <multiaddr>]

Creates a UPnP IGD port mapping for a private TCP listen address and prints the
address-manager mapping state. When listening on 0.0.0.0, interface addresses
are expanded automatically; --internal-client can override the selected LAN IP.
Pass --autonat-server to verify mapped addresses with a known AutoNAT v2 server,
or --discover-autonat to bootstrap/crawl the DHT for peers advertising AutoNAT v2.
Use --debug-raw to log raw SSDP packets and UPnP HTTP/SOAP request/response payloads.
Use --wanppp-only to force WANPPPConnection:1 probing only.
]])
end

local function parse_args(args)
  local opts = {
    port = 4001,
    duration = 60,
    auto_confirm = false,
    gateway_timeout = 10,
    mx = 2,
    replace_existing = false,
    debug_soap = false,
    debug_raw = false,
    wanppp_only = false,
    description = nil,
    list_mappings = false,
    list_mappings_max = 64,
    runtime = "auto",
    drain_seconds = 3,
    bootstrappers = {},
    discover_autonat = false,
    max_autonat_servers = 3,
    target_autonat_responses = 1,
  }
  local i = 1
  while i <= #args do
    local name = args[i]
    local value = args[i + 1]
    if name == "--internal-client" then
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
    elseif name == "--gateway-timeout" then
      opts.gateway_timeout = tonumber(value) or opts.gateway_timeout
      i = i + 2
    elseif name == "--mx" then
      opts.mx = tonumber(value) or opts.mx
      i = i + 2
    elseif name == "--replace-existing" then
      opts.replace_existing = true
      i = i + 1
    elseif name == "--debug-soap" then
      opts.debug_soap = true
      i = i + 1
    elseif name == "--debug-raw" then
      opts.debug_raw = true
      i = i + 1
    elseif name == "--wanppp-only" then
      opts.wanppp_only = true
      i = i + 1
    elseif name == "--description" then
      opts.description = value
      i = i + 2
    elseif name == "--list-mappings" then
      opts.list_mappings = true
      i = i + 1
    elseif name == "--list-mappings-max" then
      opts.list_mappings_max = tonumber(value) or opts.list_mappings_max
      i = i + 2
    elseif name == "--runtime" then
      opts.runtime = value
      i = i + 2
    elseif name == "--drain-seconds" then
      opts.drain_seconds = tonumber(value) or opts.drain_seconds
      i = i + 2
    elseif name == "--auto-confirm" then
      opts.auto_confirm = true
      i = i + 1
    elseif name == "--autonat-server" then
      opts.autonat_server = value
      i = i + 2
    elseif name == "--discover-autonat" then
      opts.discover_autonat = true
      i = i + 1
    elseif name == "--bootstrap" then
      opts.bootstrappers[#opts.bootstrappers + 1] = value
      i = i + 2
    elseif name == "--max-autonat-servers" then
      opts.max_autonat_servers = tonumber(value) or opts.max_autonat_servers
      i = i + 2
    elseif name == "--target-autonat-responses" then
      opts.target_autonat_responses = tonumber(value) or opts.target_autonat_responses
      i = i + 2
    elseif name == "--help" or name == "-h" then
      usage()
      os.exit(0)
    else
      return nil, "unknown argument: " .. tostring(name)
    end
  end
  if not opts.listen_addr then
    opts.listen_addr = "/ip4/0.0.0.0/tcp/" .. tostring(opts.port)
  end
  return opts
end

local function print_upnp_mapping_table(host, max)
  if not host.upnp_nat then
    return
  end
  local mappings, err = host.upnp_nat:list_port_mappings({ max = max })
  if not mappings then
    print("UPnP mapping table error: " .. tostring(err))
    return
  end
  print("UPnP mapping table:")
  if #mappings == 0 then
    print("  (empty)")
    return
  end
  for _, mapping in ipairs(mappings) do
    print("  [" .. tostring(mapping.index) .. "] "
      .. tostring(mapping.internal_client) .. ":" .. tostring(mapping.internal_port)
      .. " -> " .. tostring(mapping.external_port)
      .. " (" .. tostring(mapping.protocol) .. ")"
      .. " desc=" .. tostring(mapping.description)
      .. " lease=" .. tostring(mapping.lease_duration))
  end
end

local function bootstrap_config(opts)
  if #opts.bootstrappers > 0 then
    return {
      list = opts.bootstrappers,
      dialable_only = true,
      ignore_resolve_errors = true,
    }
  end
  return {
    ignore_resolve_errors = true,
  }
end

local function upnp_service_types(opts)
  if opts.wanppp_only then
    return { "urn:schemas-upnp-org:service:WANPPPConnection:1" }
  end
  return nil
end

local function wait_task(host, task, interval)
  return host:run_until_task(task, { poll_interval = interval })
end

local function run_task(host, name, fn)
  local task = assert(host:spawn_task(name, fn, { service = "example" }))
  return wait_task(host, task)
end

local function wait_child_task(ctx, task)
  return ctx:await_task(task)
end

local function sleep_poll(host, seconds, ctx)
  local deadline = os.time() + seconds
  while os.time() < deadline do
    if ctx then
      local ok, err = ctx:sleep(0.25)
      if ok == nil and err then
        return nil, err
      end
    else
      local ok, err = run_task(host, "example.sleep", function(task_ctx)
        local slept, sleep_err = task_ctx:sleep(0.25)
        if slept == nil and sleep_err then
          return nil, sleep_err
        end
        return true
      end)
      if not ok then
        return nil, err
      end
    end
  end
  return true
end

local function wait_for_mappings(host, seconds)
  local deadline = os.time() + seconds
  while os.time() < deadline do
    if #host.address_manager:get_public_address_mappings() > 0 then
      return true
    end
    local ok, err = run_task(host, "example.wait_for_mappings", function(ctx)
      local slept, sleep_err = ctx:sleep(0.25)
      if slept == nil and sleep_err then
        return nil, sleep_err
      end
      return true
    end)
    if not ok then
      return nil, err
    end
  end
  return false
end

local function print_mappings(host)
  local transport_addrs = host.address_manager:get_transport_addrs()
  print("Transport addrs:")
  for _, addr in ipairs(transport_addrs) do
    print("  " .. addr)
  end

  print("Advertised addrs:")
  for _, addr in ipairs(host:get_multiaddrs()) do
    print("  " .. addr)
  end

  local status = host.upnp_nat and host.upnp_nat:status() or nil
  if status and status.last_error then
    print("last UPnP error: " .. tostring(status.last_error))
  end
  local mappings = host.address_manager:get_public_address_mappings()
  if #mappings == 0 then
    print("No public address mappings in address manager.")
    return
  end
  print("Public address mappings:")
  for _, addr in ipairs(mappings) do
    local meta = host.address_manager:get_reachability(addr) or {}
    print("  " .. addr)
    print("    type=" .. tostring(meta.type)
      .. " verified=" .. tostring(meta.verified)
      .. " status=" .. tostring(meta.status)
      .. " source=" .. tostring(meta.source))
    if meta.internal_addr then
      print("    internal_addr=" .. tostring(meta.internal_addr))
    end
    if meta.internal_client then
      print("    internal_client=" .. tostring(meta.internal_client)
        .. " internal_port=" .. tostring(meta.internal_port)
        .. " external_port=" .. tostring(meta.external_port)
        .. " protocol=" .. tostring(meta.protocol))
    end
  end
end

local function autonat_check_mappings_async(host, server)
  local responses = 0
  local task = assert(host.autonat:start_check(server, {
    addrs = host.address_manager:get_public_address_mappings(),
    type = "ip-mapping",
  }))
  local result, task_err = wait_task(host, task)
  if not result then
    return nil, task_err
  end
  if task.result then
    responses = 1
    print("AutoNAT: checking public mappings via " .. tostring(type(server) == "table" and server.peer_id or server))
    print("  reachable=" .. tostring(task.result.reachable)
      .. " response_status=" .. tostring(task.result.response_status)
      .. " dial_status=" .. tostring(task.result.dial_status)
      .. " dial_back_verified=" .. tostring(task.result.dial_back_verified))
  end
  return responses
end

local function discover_autonat_servers(host, limit, ctx)
  if not host.kad_dht then
    print("AutoNAT discovery: kad_dht service is required")
    return {}
  end

  print("AutoNAT discovery: waiting for DHT peers")
  sleep_poll(host, 1, ctx)
  local seed_deadline = os.time() + 30
  while #host.kad_dht.routing_table:all_peers() == 0 and os.time() < seed_deadline do
    sleep_poll(host, 0.25, ctx)
  end
  sleep_poll(host, 1, ctx)
  print("  routing_table_peers=" .. tostring(#host.kad_dht.routing_table:all_peers()))

  print("AutoNAT discovery: random walk")
  local walk_op = assert(host.kad_dht:random_walk({
    count = 20,
    alpha = 10,
    disjoint_paths = 10,
  }))
  local walk_report = walk_op:result({ ctx = ctx })
  sleep_poll(host, 1, ctx)
  if walk_report then
    print("  walk queried=" .. tostring(walk_report.queried or 0)
      .. " responses=" .. tostring(walk_report.responses or 0)
      .. " added=" .. tostring(walk_report.added or 0))
  end

  local proto = require("lua_libp2p.protocol.autonat_v2").DIAL_REQUEST_ID
  local out = {}
  for _, peer in ipairs(host.peerstore:all()) do
    if host.peerstore:supports_protocol(peer.peer_id, proto) and #host.peerstore:get_addrs(peer.peer_id) > 0 then
      out[#out + 1] = {
        peer_id = peer.peer_id,
        addrs = host.peerstore:get_addrs(peer.peer_id),
      }
      print("  found AutoNAT candidate: " .. peer.peer_id)
      if #out >= limit then
        break
      end
    end
  end
  if #out == 0 then
    print("AutoNAT discovery: no AutoNAT v2 servers found in peerstore")
  end
  return out
end

local function check_discovered_autonat_servers(host, opts)
  local checked = 0
  local real_responses = 0
  while checked < opts.max_autonat_servers and real_responses < opts.target_autonat_responses do
    local servers = run_task(host, "example.discover_autonat_servers", function(ctx)
      return discover_autonat_servers(host, opts.max_autonat_servers, ctx)
    end) or {}
    if #servers == 0 then
      break
    end
    for _, server in ipairs(servers) do
      if checked >= opts.max_autonat_servers or real_responses >= opts.target_autonat_responses then
        break
      end
      checked = checked + 1
      local responses = autonat_check_mappings_async(host, server) or 0
      real_responses = real_responses + responses
      if responses == 0 then
        print("  no AutoNAT DialResponse from " .. tostring(server.peer_id))
      end
    end
    if real_responses < opts.target_autonat_responses then
      print("AutoNAT discovery: target responses not met; continuing random walk")
      if host.kad_dht then
        local extra = assert(host.kad_dht:random_walk({ count = 20, alpha = 10, disjoint_paths = 10 }))
        extra:result({ timeout = 60 })
      else
        break
      end
    end
  end
  sleep_poll(host, opts.drain_seconds)
  print("AutoNAT discovery: checked=" .. tostring(checked) .. " real_responses=" .. tostring(real_responses))
end

local opts, err = parse_args(arg)
if not opts then
  usage()
  error(err)
end

local h = assert(host_mod.new({
  runtime = opts.runtime,
  blocking = false,
  listen_addrs = { opts.listen_addr },
  services = {
    identify = { module = identify_service },
    ping = { module = ping_service },
    autonat = { module = autonat_service },
    kad_dht = { module = kad_dht_service, config = { mode = "client" } },
    upnp_nat = {
      module = upnp_nat_service,
      config = {
        internal_client = opts.internal_client,
        auto_confirm_address = opts.auto_confirm,
        timeout = opts.gateway_timeout,
        mx = opts.mx,
        replace_existing = opts.replace_existing,
        debug_soap = opts.debug_soap,
        debug_raw = opts.debug_raw,
        service_types = upnp_service_types(opts),
        description = opts.description,
      },
    },
  },
  peer_discovery = {
    bootstrap = {
      module = peer_discovery_bootstrap,
      config = bootstrap_config(opts),
    },
  },
}))

h:on("upnp_nat:mapping:active", function(mapping)
  print("upnp mapping active: " .. tostring(mapping.external_addr))
  if mapping.internal_addr then
    print("  internal: " .. tostring(mapping.internal_addr))
  end
  if mapping.internal_client then
    print("  upnp request: " .. tostring(mapping.protocol)
      .. " " .. tostring(mapping.internal_client)
      .. ":" .. tostring(mapping.internal_port)
      .. " <- external port " .. tostring(mapping.external_port))
  end
  if mapping.actual_internal_client then
    print("  router reports: " .. tostring(mapping.actual_internal_client)
      .. ":" .. tostring(mapping.actual_internal_port))
  end
  return true
end)

h:on("upnp_nat:mapping:failed", function(payload)
  print("upnp mapping failed: " .. tostring(payload.error_message or payload.error))
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

assert(h:start())
wait_for_mappings(h, 5)

print("peer_id: " .. h:peer_id().id)
print("listen_addr: " .. opts.listen_addr)
print("auto_confirm_address: " .. tostring(opts.auto_confirm))

print_mappings(h)
if opts.list_mappings then
  print_upnp_mapping_table(h, opts.list_mappings_max)
end

if opts.autonat_server then
  autonat_check_mappings_async(h, opts.autonat_server)
  print_mappings(h)
end

if opts.discover_autonat then
  check_discovered_autonat_servers(h, opts)
  print_mappings(h)
  if opts.list_mappings then
    print_upnp_mapping_table(h, opts.list_mappings_max)
  end
end

if opts.duration > 0 then
  print("running for " .. tostring(opts.duration) .. "s; press Ctrl-C to stop")
  assert(sleep_poll(h, opts.duration))
end

print_mappings(h)
h:stop()
