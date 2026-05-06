package.path = table.concat({
  "./?.lua",
  "./?/init.lua",
  package.path,
}, ";")

local host_mod = require("lua_libp2p.host")
local identify_service = require("lua_libp2p.protocol_identify.service")
local ping_service = require("lua_libp2p.protocol_ping.service")
local kad_dht_service = require("lua_libp2p.kad_dht")
local bootstrap_defaults = require("lua_libp2p.bootstrap")
local dnsaddr = require("lua_libp2p.dnsaddr")
local peer_discovery_bootstrap = require("lua_libp2p.peer_discovery_bootstrap")
local multiaddr = require("lua_libp2p.multiaddr")

local mode = arg[1]

local function usage()
  io.stderr:write("usage:\n")
  io.stderr:write("  lua examples/kad_dht_bootstrap_demo.lua server [listen-multiaddr ...]\n")
  io.stderr:write("  lua examples/kad_dht_bootstrap_demo.lua client </bootstrap-multiaddr-with-p2p>\n")
  io.stderr:write("  lua examples/kad_dht_bootstrap_demo.lua client --default-bootstrap\n")
  io.stderr:write("  (note: bootstrap multiaddr must start with '/'; e.g. /ip4/127.0.0.1/tcp/12345/p2p/12D3KooW...)\n")
  io.stderr:write("  (server defaults to /ip4/0.0.0.0/tcp/4001 and /ip6/::/tcp/4001 when no listen addrs are provided)\n")
end

local function summarize_error(err)
  local suffix = ""
  if type(err) == "table" and type(err.context) == "table" then
    local parts = {}
    for _, key in ipairs({ "cause", "code", "protocol", "protocol_id", "received_key_type_name", "received_key_type" }) do
      if err.context[key] ~= nil then
        parts[#parts + 1] = key .. "=" .. tostring(err.context[key])
      end
    end
    if #parts > 0 then
      suffix = " (" .. table.concat(parts, ", ") .. ")"
    end
  end
  return tostring(err) .. suffix
end

local function print_error_summary(errors, max_lines)
  local counts = {}
  local order = {}
  for _, err in ipairs(errors or {}) do
    local key = summarize_error(err)
    if counts[key] == nil then
      counts[key] = 0
      order[#order + 1] = key
    end
    counts[key] = counts[key] + 1
  end

  table.sort(order, function(a, b)
    if counts[a] == counts[b] then
      return a < b
    end
    return counts[a] > counts[b]
  end)

  local limit = max_lines or 20
  for i, key in ipairs(order) do
    if i > limit then
      io.stdout:write("    ... " .. tostring(#order - limit) .. " more error type(s)\n")
      break
    end
    io.stdout:write("    " .. tostring(counts[key]) .. "x " .. key .. "\n")
  end
end

local function run_server()
  local listen_addrs = {}
  for i = 2, #arg do
    listen_addrs[#listen_addrs + 1] = arg[i]
  end
  if #listen_addrs == 0 then
    listen_addrs = {
      "/ip4/0.0.0.0/tcp/4001",
      "/ip6/::/tcp/4001",
    }
  end

  local host, host_err = host_mod.new({
    listen_addrs = listen_addrs,
    services = {
      identify = { module = identify_service },
      ping = { module = ping_service },
      kad_dht = {
        module = kad_dht_service,
        config = {
          mode = "server",
        },
      },
    },
    blocking = true,
    accept_timeout = 0.05,
    poll_interval = 0.01,
    on_started = function(h)
      io.stdout:write("kad-dht demo server listening:\n")
      for _, addr in ipairs(h:get_multiaddrs()) do
        io.stdout:write("  " .. addr .. "\n")
      end
      io.stdout:write("running; Ctrl-C to stop\n")
      io.stdout:flush()
    end,
  })
  if not host then
    io.stderr:write("host init failed: " .. tostring(host_err) .. "\n")
    os.exit(1)
  end

  local started, start_err = host:start()
  if not started then
    io.stderr:write("host stopped with error: " .. tostring(start_err) .. "\n")
    os.exit(1)
  end
end

local function run_client()
  local bootstrap_arg = arg[2]
  local bootstrap_addrs
  local parsed_input
  local dns_cache = {}
  local function dnsaddr_resolver(domain)
    if dns_cache[domain] ~= nil then
      local cached = dns_cache[domain]
      io.stdout:write("resolving dnsaddr domain: " .. tostring(domain) .. " (cached)\n")
      if cached.ok then
        io.stdout:write("  got " .. tostring(#cached.records) .. " TXT dnsaddr record(s)\n")
        return cached.records
      end
      io.stdout:write("  resolve failed: " .. tostring(cached.err) .. "\n")
      return nil, cached.err
    end

    io.stdout:write("resolving dnsaddr domain: " .. tostring(domain) .. "\n")
    local records, err = dnsaddr.default_resolver(domain)
    if not records then
      io.stdout:write("  resolve failed: " .. tostring(err) .. "\n")
      dns_cache[domain] = { ok = false, err = err }
      return nil, err
    end
    io.stdout:write("  got " .. tostring(#records) .. " TXT dnsaddr record(s)\n")
    for _, rec in ipairs(records) do
      io.stdout:write("    " .. tostring(rec) .. "\n")
    end
    dns_cache[domain] = { ok = true, records = records }
    return records
  end
  io.stdout:write("dnsaddr resolver: lua_libp2p.dnsaddr.default_resolver\n")

  if bootstrap_arg == "--default-bootstrap" then
    bootstrap_addrs = bootstrap_defaults.default_bootstrappers()
    if #bootstrap_addrs == 0 then
      io.stderr:write("no default bootstrap addresses available\n")
      os.exit(1)
    end
    io.stdout:write("using default bootstrappers:\n")
    for _, addr in ipairs(bootstrap_addrs) do
      io.stdout:write("  " .. addr .. "\n")
    end
    parsed_input = multiaddr.parse(bootstrap_addrs[#bootstrap_addrs])
  else
    local bootstrap_addr = bootstrap_arg
    if not bootstrap_addr or bootstrap_addr == "" then
      usage()
      os.exit(2)
    end

    local parse_err
    parsed_input, parse_err = multiaddr.parse(bootstrap_addr)
    if not parsed_input then
      io.stderr:write("invalid bootstrap multiaddr: " .. tostring(parse_err) .. "\n")
      usage()
      os.exit(2)
    end
    bootstrap_addrs = { bootstrap_addr }
  end

  local host, host_err = host_mod.new({
    runtime = "luv",
    peer_discovery = {
      bootstrap = {
        module = peer_discovery_bootstrap,
        config = {
          list = bootstrap_addrs,
          dialable_only = true,
          dnsaddr_resolver = dnsaddr_resolver,
          ignore_resolve_errors = false,
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
        },
      },
    },
    blocking = false,
    connect_timeout = 6,
    io_timeout = 10,
    accept_timeout = 0.05,
  })
  if not host then
    io.stderr:write("host init failed: " .. tostring(host_err) .. "\n")
    os.exit(1)
  end

  local started, start_err = host:start()
  if not started then
    io.stderr:write("host start failed: " .. tostring(start_err) .. "\n")
    os.exit(1)
  end

  local discovered, discovered_err = host.peer_discovery:discover({
    dialable_only = true,
  })
  if not discovered then
    io.stderr:write("discovery failed: " .. tostring(discovered_err) .. "\n")
    os.exit(1)
  end
  io.stdout:write("discovery candidates:\n")
  for _, candidate in ipairs(discovered) do
    io.stdout:write("  peer=" .. tostring(candidate.peer_id) .. " addr=" .. tostring((candidate.addrs or {})[1]) .. "\n")
  end

  local dht = host.kad_dht

  local seed_deadline = os.time() + 30
  while #dht.routing_table:all_peers() == 0 and os.time() < seed_deadline do
    local pumped, pump_err = host:sleep(0.25)
    if not pumped then
      io.stderr:write("host pump failed: " .. tostring(pump_err) .. "\n")
      break
    end
  end
  if #dht.routing_table:all_peers() == 0 then
    io.stdout:write("  hint: no KAD-capable peers discovered from bootstrap input\n")
  end

  local peers = dht.routing_table:all_peers()
  io.stdout:write("routing table peers:\n")
  for _, p in ipairs(peers) do
    io.stdout:write("  " .. tostring(p.peer_id) .. "\n")
  end

  local parsed_bootstrap = parsed_input
  local bootstrap_peer_id = nil
  if parsed_bootstrap and type(parsed_bootstrap.components) == "table" then
    for i = #parsed_bootstrap.components, 1, -1 do
      local c = parsed_bootstrap.components[i]
      if c.protocol == "p2p" then
        bootstrap_peer_id = c.value
        break
      end
    end
  end

  if bootstrap_peer_id then
    io.stdout:write("running one FIND_NODE query against bootstrap peer...\n")
    local lookup_op, lookup_op_err = dht:find_node(bootstrap_addrs[1], host:peer_id().id)
    local lookup, lookup_err
    if lookup_op then
      lookup, lookup_err = lookup_op:result()
    else
      lookup_err = lookup_op_err
    end
    if not lookup then
      io.stderr:write("FIND_NODE failed: " .. tostring(lookup_err) .. "\n")
    else
      io.stdout:write("FIND_NODE closer peers: " .. tostring(#lookup) .. "\n")
      for _, candidate in ipairs(lookup) do
        io.stdout:write("  " .. tostring(candidate.peer_id) .. "\n")
      end
    end

    io.stdout:write("running one random-walk refresh (target=self peer id)...\n")
    local walk_started_at = os.time()
    local walk_op, walk_op_err = dht:random_walk({
      alpha = 10,
      disjoint_paths = 10,
    })
    local walk, walk_err
    if walk_op then
      walk, walk_err = walk_op:result()
    else
      walk_err = walk_op_err
    end
    if not walk then
      io.stderr:write("random walk failed: " .. tostring(walk_err) .. "\n")
    else
      io.stdout:write("random walk report:\n")
      io.stdout:write("  queried: " .. tostring(walk.queried) .. "\n")
      io.stdout:write("  responses: " .. tostring(walk.responses) .. "\n")
      io.stdout:write("  added: " .. tostring(walk.added) .. "\n")
      io.stdout:write("  skipped: " .. tostring(walk.skipped or 0) .. "\n")
      io.stdout:write("  failed: " .. tostring(walk.failed) .. "\n")
      io.stdout:write("  active_peak: " .. tostring(walk.active_peak or 0) .. "\n")
      io.stdout:write("  termination: " .. tostring(walk.termination or "unknown") .. "\n")
      io.stdout:write("  duration: " .. tostring(os.time() - walk_started_at) .. "s\n")
      if #walk.errors > 0 then
        io.stdout:write("  errors:\n")
        print_error_summary(walk.errors)
      end

      local closest = dht:find_closest_peers(host:peer_id().id, 20) or {}
      io.stdout:write("closest routing table peers:\n")
      for _, peer in ipairs(closest) do
        io.stdout:write("  " .. tostring(peer.peer_id) .. "\n")
      end
    end
  else
    io.stdout:write("bootstrap addr has no /p2p component; skipping FIND_NODE demo\n")
  end

  host:stop()
end

if mode == "server" then
  run_server()
elseif mode == "client" then
  run_client()
else
  usage()
  os.exit(2)
end
