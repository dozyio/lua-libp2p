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
    discovery_timeout = 90,
    seed_wait_seconds = 2,
    walk_timeout = 12,
    dht_alpha = 3,
    dht_paths = 2,
    dht_count = 20,
    stop_on_first_reachable = true,
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
    elseif name == "--discovery-timeout" then
      opts.discovery_timeout = tonumber(value) or opts.discovery_timeout
      i = i + 2
    elseif name == "--seed-wait-seconds" then
      opts.seed_wait_seconds = tonumber(value) or opts.seed_wait_seconds
      i = i + 2
    elseif name == "--walk-timeout" then
      opts.walk_timeout = tonumber(value) or opts.walk_timeout
      i = i + 2
    elseif name == "--dht-alpha" then
      opts.dht_alpha = tonumber(value) or opts.dht_alpha
      i = i + 2
    elseif name == "--dht-paths" then
      opts.dht_paths = tonumber(value) or opts.dht_paths
      i = i + 2
    elseif name == "--dht-count" then
      opts.dht_count = tonumber(value) or opts.dht_count
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

local function wait_for_mappings(host, seconds, ctx)
  local deadline = os.time() + seconds
  while os.time() < deadline do
    if #host.address_manager:get_public_address_mappings() > 0 then
      return true
    end
    local ok, err = host:sleep(0.25, { ctx = ctx })
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

local function metadata_status(host, addr)
  local metadata = host and host.address_manager and host.address_manager:get_reachability(addr) or nil
  if not metadata then
    return "missing"
  end
  return tostring(metadata.status or "unknown")
    .. ",verified=" .. tostring(metadata.verified == true)
    .. ",source=" .. tostring(metadata.source)
end

local function print_autonat_summary(stats)
  print("AutoNAT summary: checked="
    .. tostring(stats and stats.checked or 0)
    .. " responses=" .. tostring(stats and stats.responses or 0)
    .. " refused=" .. tostring(stats and stats.refused or 0)
    .. " reachable=" .. tostring(stats and stats.reachable or 0))
end

local function autonat_check_mappings_async(host, server, ctx)
  local mappings_ready, mappings_err = wait_for_mappings(host, 10, ctx)
  if mappings_ready == nil then
    return nil, mappings_err
  end
  local mappings = host.address_manager:get_public_address_mappings()
  if #mappings == 0 then
    return nil, "AutoNAT check skipped: no UPnP public mappings are active yet"
  end
  local task = assert(host.autonat:start_check(server, {
    addrs = mappings,
    type = "ip-mapping",
    timeout = 8,
    stream_opts = { ctx = ctx },
  }))
  local result, task_err = host:wait_task(task, { ctx = ctx, poll_interval = 0.05 })
  if not result then
    return nil, task_err
  end
  print("AutoNAT: checking public mappings via " .. tostring(type(server) == "table" and server.peer_id or server))
  print("  reachable=" .. tostring(result.reachable)
    .. " response_status=" .. tostring(result.response_status)
    .. " dial_status=" .. tostring(result.dial_status)
    .. " dial_back_verified=" .. tostring(result.dial_back_verified))
  if result.addr then
    print("  checked_addr=" .. tostring(result.addr) .. " status=" .. metadata_status(host, result.addr))
  end
  print_autonat_summary({
    checked = 1,
    responses = result.response_status == "E_DIAL_REFUSED" and 0 or 1,
    refused = result.response_status == "E_DIAL_REFUSED" and 1 or 0,
    reachable = result.reachable and 1 or 0,
  })
  return result
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
    kad_dht = { module = kad_dht_service, config = { mode = "auto" } },
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

h:on("autonat:reachability:checked", function(payload)
  print("autonat reachability check: "
    .. tostring(payload and payload.addr)
    .. " reachable=" .. tostring(payload and payload.reachable)
    .. " response_status=" .. tostring(payload and payload.response_status)
    .. " dial_status=" .. tostring(payload and payload.dial_status)
    .. " verified=" .. tostring(payload and payload.dial_back_verified)
    .. " metadata=" .. metadata_status(h, payload and payload.addr))
  return true
end)

h:on("autonat:address:unreachable", function(payload)
  print("autonat unreachable: " .. tostring(payload.addr))
  return true
end)

h:on("autonat:request:failed", function(payload)
  print("autonat request failed: server="
    .. tostring(payload and payload.server_peer_id)
    .. " response_status=" .. tostring(payload and payload.response_status)
    .. " dial_status=" .. tostring(payload and payload.dial_status))
  return true
end)

h:on("autonat:discovery:check_failed", function(payload)
  print("autonat discovery check failed: peer="
    .. tostring(payload and payload.peer_id)
    .. " cause=" .. tostring(payload and payload.error))
  return true
end)

h:on("autonat:discovery:candidates", function(payload)
  print("autonat discovery: peerstore_peers="
    .. tostring(payload and payload.peerstore_peers or 0)
    .. " autonat_proto_candidates="
    .. tostring(payload and payload.protocol_candidates or 0))
  return true
end)

h:on("autonat:discovery:walk", function(payload)
  local report = payload and payload.report or {}
  print("autonat discovery: random_walk queried="
    .. tostring(report.queried or 0)
    .. " responses=" .. tostring(report.responses or 0)
    .. " added=" .. tostring(report.added or 0)
    .. " discovered=" .. tostring(report.discovered or 0))
  if payload and payload.routing_table_peers then
    print("autonat discovery: post-walk peers=" .. tostring(payload.routing_table_peers))
  end
  return true
end)

h:on("autonat:discovery:progress", function(payload)
  print("autonat discovery: checked="
    .. tostring(payload and payload.checked or 0)
    .. " responses=" .. tostring(payload and payload.responses or 0)
    .. " refused=" .. tostring(payload and payload.refused or 0)
    .. " reachable=" .. tostring(payload and payload.reachable or 0))
  return true
end)

h:on("autonat:monitor:verified", function(payload)
  print("autonat monitor verified: round=" .. tostring(payload and payload.round or 0))
  print_autonat_summary(payload and payload.stats or {})
  return true
end)

h:on("kad_dht:mode_changed", function(payload)
  print("kad dht mode changed: "
    .. tostring(payload and payload.old_mode)
    .. " -> " .. tostring(payload and payload.mode)
    .. " reason=" .. tostring(payload and payload.reason))
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
  h:spawn_task("example.autonat_direct", function(ctx)
    local _, check_err = autonat_check_mappings_async(h, opts.autonat_server, ctx)
    if check_err then
      print("autonat check failed: " .. tostring(check_err))
    end
    print_mappings(h)
    return true
  end, { service = "example" })
end

if opts.discover_autonat then
  local monitor_task, monitor_err = h.autonat:start_monitor({
    poll_interval = 0.05,
    discovery_timeout = opts.discovery_timeout,
    seed_wait_seconds = opts.seed_wait_seconds,
    walk_timeout = opts.walk_timeout,
    dht_alpha = opts.dht_alpha,
    dht_paths = opts.dht_paths,
    dht_count = opts.dht_count,
    max_autonat_servers = opts.max_autonat_servers,
    target_autonat_responses = opts.target_autonat_responses,
    stop_on_first_reachable = opts.stop_on_first_reachable,
    drain_seconds = opts.drain_seconds,
    check_type = "ip-mapping",
    retry_interval_seconds = 5,
    healthy_interval_seconds = 30,
    min_success_rounds = 2,
    checked_peer_cooldown_seconds = 30,
    required_addr_proto = opts.listen_addr:find("/ip6/", 1, true) and "ip6" or "ip4",
    check_opts_builder = function()
      local mappings = h.address_manager:get_public_address_mappings()
      if #mappings == 0 then
        print("  AutoNAT: no UPnP public mappings active yet; candidate check will be skipped")
        return {
          addrs = {},
          type = "ip-mapping",
          timeout = 8,
        }
      end
      for _, addr in ipairs(mappings) do
        print("  checking mapping: " .. tostring(addr) .. " status=" .. metadata_status(h, addr))
      end
      return {
        addrs = mappings,
        type = "ip-mapping",
        timeout = 8,
      }
    end,
  })
  if not monitor_task then
    print("autonat monitor failed to start: " .. tostring(monitor_err))
  end
end

if opts.duration > 0 then
  print("running for " .. tostring(opts.duration) .. "s; press Ctrl-C to stop")
  assert(h:sleep(opts.duration))
end

print_mappings(h)
h:stop()
