package.path = table.concat({
  "./?.lua",
  "./?/init.lua",
  package.path,
}, ";")

local host_mod = require("lua_libp2p.host")
local identify_service = require("lua_libp2p.protocol_identify.service")
local ping_service = require("lua_libp2p.protocol_ping.service")
local kad_dht_service = require("lua_libp2p.kad_dht")
local sqlite_datastore = require("lua_libp2p.datastore.sqlite")
local peerstore = require("lua_libp2p.peerstore")
local keys = require("lua_libp2p.crypto.keys")
local ed25519 = require("lua_libp2p.crypto.ed25519")
local bootstrap_defaults = require("lua_libp2p.bootstrap")
local dnsaddr = require("lua_libp2p.dnsaddr")
local peer_discovery_bootstrap = require("lua_libp2p.peer_discovery_bootstrap")
local multiaddr = require("lua_libp2p.multiformats.multiaddr")
local log = require("lua_libp2p.log")
local health_log = log.subsystem("health")
local perf_log = log.subsystem("perf")

local mode = arg[1]

local function now_iso8601()
  return os.date("!%Y-%m-%dT%H:%M:%SZ")
end

local function install_log_signal_toggles()
  local ok_luv, luv = pcall(require, "luv")
  if not ok_luv or not luv or type(luv.new_signal) ~= "function" then
    return nil, "luv signal handlers unavailable"
  end
  if not luv.constants then
    return nil, "luv constants unavailable"
  end

  local previous_snapshot = nil
  local usr1 = luv.new_signal()
  local usr2 = luv.new_signal()
  if not usr1 or not usr2 then
    return nil, "failed to allocate signal handlers"
  end

  usr1:start(luv.constants.SIGUSR1, function()
    if not previous_snapshot then
      previous_snapshot = log.snapshot()
    end
    log.configure("*=debug")
    io.stderr:write("[signal] SIGUSR1 -> log level set to *=debug\n")
    io.stderr:flush()
  end)

  usr2:start(luv.constants.SIGUSR2, function()
    if previous_snapshot then
      log.restore(previous_snapshot)
      previous_snapshot = nil
      io.stderr:write("[signal] SIGUSR2 -> log config restored\n")
    else
      io.stderr:write("[signal] SIGUSR2 -> no prior log snapshot\n")
    end
    io.stderr:flush()
  end)

  return {
    close = function()
      pcall(function()
        usr1:stop()
      end)
      pcall(function()
        usr2:stop()
      end)
      pcall(function()
        usr1:close()
      end)
      pcall(function()
        usr2:close()
      end)
    end,
  }
end

local function usage()
  io.stderr:write("usage:\n")
  io.stderr:write(
    "  lua examples/kad_dht_bootstrap_demo.lua server [--default-bootstrap] [--bootstrap <multiaddr> ...] [--identity-key <path>] [--datastore <sqlite-path>] [--peerstore <sqlite-path>] [listen-multiaddr ...]\n"
  )
  io.stderr:write(
    "  lua examples/kad_dht_bootstrap_demo.lua client </bootstrap-multiaddr-with-p2p> [--datastore <sqlite-path>] [--peerstore <sqlite-path>]\n"
  )
  io.stderr:write(
    "  lua examples/kad_dht_bootstrap_demo.lua client --default-bootstrap [--datastore <sqlite-path>] [--peerstore <sqlite-path>]\n"
  )
  io.stderr:write("  (note: bootstrap multiaddr must start with '/'; e.g. /ip4/127.0.0.1/tcp/12345/p2p/12D3KooW...)\n")
  io.stderr:write(
    "  (server defaults to /ip4/0.0.0.0/tcp/4001 and /ip6/::/tcp/4001 when no listen addrs are provided)\n"
  )
end

local function file_exists(path)
  local f = io.open(path, "rb")
  if not f then
    return false
  end
  f:close()
  return true
end

local function load_or_create_identity(path)
  if file_exists(path) then
    return ed25519.load_private_key(path)
  end
  local keypair, gen_err = keys.generate_keypair("ed25519")
  if not keypair then
    return nil, gen_err
  end
  local saved, save_err = ed25519.save_private_key(path, keypair)
  if not saved then
    return nil, save_err
  end
  return keypair
end

local function open_optional_datastore(path)
  if path == nil then
    return nil
  end
  local store, store_err = sqlite_datastore.new({ path = path })
  if not store then
    return nil, store_err
  end
  return store
end

local function open_optional_peerstore(path)
  if path == nil then
    return nil
  end
  local store, store_err = open_optional_datastore(path)
  if not store then
    return nil, nil, store_err
  end
  local ps, ps_err = peerstore.new({ datastore = store })
  if not ps then
    store:close()
    return nil, nil, ps_err
  end
  return ps, store
end

local function summarize_error(err)
  local suffix = ""
  if type(err) == "table" and type(err.context) == "table" then
    local parts = {}
    for _, key in ipairs({
      "cause",
      "code",
      "protocol",
      "protocol_id",
      "received_key_type_name",
      "received_key_type",
    }) do
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

local function print_server_stats(h)
  local dht = h.kad_dht
  local routing_table_size = 0
  if dht and dht.routing_table and type(dht.routing_table.all_peers) == "function" then
    local peers = dht.routing_table:all_peers() or {}
    routing_table_size = #peers
  end

  local stats = h:stats() or {}
  local connection_stats = stats.connections or {}
  local task_stats = stats.tasks or {}
  local resource_stats = stats.resources or {}
  local resource_system = resource_stats.system or {}
  local resource_limits = resource_stats.limits or {}

  io.stdout:write(
    string.format(
      "%s [stats] routing_table=%d connections=%d inbound=%d outbound=%d active_dials=%d pending_dials=%d dial_queue=%d identify_inflight=%d rcmgr_inbound=%d/%d tasks_running=%d tasks_queued=%d\n",
      now_iso8601(),
      routing_table_size,
      tonumber(connection_stats.connections) or 0,
      tonumber(connection_stats.inbound_connections) or 0,
      tonumber(connection_stats.outbound_connections) or 0,
      tonumber(connection_stats.active_dials) or 0,
      tonumber(connection_stats.pending_dials) or 0,
      tonumber(connection_stats.queued_dials) or 0,
      tonumber(task_stats.identify_inflight) or 0,
      tonumber(resource_system.connections_inbound) or 0,
      tonumber(resource_limits.connections_inbound) or 0,
      tonumber(task_stats.running) or 0,
      tonumber(task_stats.queued) or 0
    )
  )
  io.stdout:flush()
end

local function print_internal_health(h)
  local health_stats = type(h.health_stats) == "function" and h:health_stats() or {}
  local health = health_stats.health or {}
  if health.mem_kb ~= nil then
    health = {
      tasks_total = health.tasks_total,
      queue = health.queue,
      wait_read = health.wait_read,
      wait_dial = health.wait_dial,
      wait_write = health.wait_write,
      wait_done = health.wait_done,
      pending_inbound = health.pending_inbound,
      pending_relay_inbound = health.pending_relay_inbound,
      listener_pending = health.listener_pending,
      conns = health.conns,
      dial_queue = health.dial_queue,
      pending_keys = health.pending_keys,
      mem_kb = string.format("%.1f", tonumber(health.mem_kb) or 0),
    }
  end
  health_log.debug("health", health)
  health_log.debug("listener", health_stats.listener or {})
  health_log.debug("inbound", health_stats.inbound or {})
  health_log.debug("inbound fail", health_stats.inbound_fail or {})
  health_log.debug("task top", health_stats.task_top or {})

  local perf = type(h.perf_stats) == "function" and h:perf_stats({ reset = true }) or nil
  if type(perf) == "table" then
    local function phase_metric(key)
      local calls = perf[key .. "_calls"] or 0
      local total_ms = perf[key .. "_ms"] or 0
      local avg_ms = calls > 0 and (total_ms / calls) or 0
      return string.format("%s=%d/%.2f/%.3f", key, calls, total_ms, avg_ms)
    end

    local function top_perf_group(group_key, limit)
      local group = perf[group_key]
      local items = {}
      for key, value in pairs(type(group) == "table" and group or {}) do
        local calls = tonumber(value.calls) or 0
        local total_ms = tonumber(value.ms) or 0
        local avg_ms = calls > 0 and (total_ms / calls) or 0
        items[#items + 1] = {
          key = tostring(key),
          calls = calls,
          total_ms = total_ms,
          avg_ms = avg_ms,
        }
      end
      table.sort(items, function(a, b)
        if a.total_ms == b.total_ms then
          return a.key < b.key
        end
        return a.total_ms > b.total_ms
      end)
      local parts = {}
      local max_items = math.min(limit or 5, #items)
      for i = 1, max_items do
        local item = items[i]
        parts[#parts + 1] = string.format("%s=%d/%.2f/%.3f", item.key, item.calls, item.total_ms, item.avg_ms)
      end
      if #parts == 0 then
        return "none"
      end
      return table.concat(parts, ",")
    end

    local poll_calls = perf.poll_once_calls or 0
    local process_calls = perf.process_events_calls or 0
    local bg_calls = perf.background_calls or 0
    local poll_avg_ms = poll_calls > 0 and ((perf.poll_once_ms or 0) / poll_calls) or 0
    local process_avg_ms = process_calls > 0 and ((perf.process_events_ms or 0) / process_calls) or 0
    local bg_avg_ms = bg_calls > 0 and ((perf.background_ms or 0) / bg_calls) or 0

    perf_log.debug("runtime totals", {
      poll_once_calls = poll_calls,
      poll_once_ms = string.format("%.2f", perf.poll_once_ms or 0),
      process_events_calls = process_calls,
      process_events_ms = string.format("%.2f", perf.process_events_ms or 0),
      bg_calls = bg_calls,
      bg_ms = string.format("%.2f", perf.background_ms or 0),
    })
    perf_log.debug("poll phases", {
      poll_sync_watchers = phase_metric("poll_sync_watchers"),
      poll_uv_wait = phase_metric("poll_uv_wait"),
      poll_process_events = phase_metric("poll_process_events"),
    })
    perf_log.debug("process phases", {
      process_ready_waiters = phase_metric("process_ready_waiters"),
      process_pending_relay_inbound = phase_metric("process_pending_relay_inbound"),
      process_pending_inbound = phase_metric("process_pending_inbound"),
      process_listeners = phase_metric("process_listeners"),
      process_router = phase_metric("process_router"),
      process_connections = phase_metric("process_connections"),
      process_background = phase_metric("process_background"),
    })
    perf_log.debug("background phases", {
      background_sleep_scan = phase_metric("background_sleep_scan"),
      background_resume = phase_metric("background_resume"),
      background_prune = phase_metric("background_prune"),
    })
    perf_log.debug("resume phases", {
      background_resume_dequeue = phase_metric("background_resume_dequeue"),
      background_resume_skip = phase_metric("background_resume_skip"),
      background_resume_cancel = phase_metric("background_resume_cancel"),
      background_resume_coroutine = phase_metric("background_resume_coroutine"),
      background_resume_yield = phase_metric("background_resume_yield"),
      background_resume_completed = phase_metric("background_resume_completed"),
      background_resume_failed = phase_metric("background_resume_failed"),
    })
    perf_log.debug("resume top", {
      names = top_perf_group("background_resume_by_name", 6),
      services = top_perf_group("background_resume_by_service", 6),
    })
    perf_log.debug("kad phases", {
      kad_rpc_read = phase_metric("kad_rpc_read"),
      kad_rpc_dispatch = phase_metric("kad_rpc_dispatch"),
      kad_rpc_write = phase_metric("kad_rpc_write"),
      kad_rpc_close_write = phase_metric("kad_rpc_close_write"),
      kad_closest_lookup = phase_metric("kad_closest_lookup"),
      kad_closest_parse_target = phase_metric("kad_closest_parse_target"),
      kad_closest_build = phase_metric("kad_closest_build"),
      kad_closest_peer_record = phase_metric("kad_closest_peer_record"),
      kad_peer_record_peer_bytes = phase_metric("kad_peer_record_peer_bytes"),
      kad_peer_record_get_addrs = phase_metric("kad_peer_record_get_addrs"),
      kad_peer_record_filter_addrs = phase_metric("kad_peer_record_filter_addrs"),
      kad_peer_record_addr_cache_hit = phase_metric("kad_peer_record_addr_cache_hit"),
      types = top_perf_group("kad_rpc_dispatch_by_type", 6),
    })
    perf_log.debug("kad query phases", {
      kad_random_walk_seed_build = phase_metric("kad_random_walk_seed_build"),
      kad_random_walk_apply_results = phase_metric("kad_random_walk_apply_results"),
      kad_query_seed_enqueue = phase_metric("kad_query_seed_enqueue"),
      kad_query_fill_tasks = phase_metric("kad_query_fill_tasks"),
      kad_query_reap_tasks = phase_metric("kad_query_reap_tasks"),
      kad_query_strict_complete = phase_metric("kad_query_strict_complete"),
      kad_query_running_tasks = phase_metric("kad_query_running_tasks"),
      kad_query_final_sort = phase_metric("kad_query_final_sort"),
    })
    if process_avg_ms > 20 or bg_avg_ms > 20 or poll_avg_ms > 50 then
      perf_log.warn("runtime phase averages high", {
        poll_avg_ms = string.format("%.2f", poll_avg_ms),
        process_avg_ms = string.format("%.2f", process_avg_ms),
        bg_avg_ms = string.format("%.2f", bg_avg_ms),
      })
    end
  end
  io.stdout:flush()
end

local function run_server()
  local listen_addrs = {}
  local bootstrappers = {}
  local use_default_bootstrap = false
  local identity_key_path = ".kad_dht_bootstrap_demo.ed25519.key"
  local datastore_path
  local peerstore_path

  local arg_i = 2
  while arg_i <= #arg do
    local name = arg[arg_i]
    local value = arg[arg_i + 1]
    if name == "--default-bootstrap" then
      use_default_bootstrap = true
      arg_i = arg_i + 1
    elseif name == "--bootstrap" then
      if not value then
        io.stderr:write("--bootstrap requires a multiaddr\n")
        usage()
        os.exit(2)
      end
      bootstrappers[#bootstrappers + 1] = value
      arg_i = arg_i + 2
    elseif name == "--identity-key" then
      if not value or value == "" then
        io.stderr:write("--identity-key requires a path\n")
        usage()
        os.exit(2)
      end
      identity_key_path = value
      arg_i = arg_i + 2
    elseif name == "--datastore" then
      if not value or value == "" then
        io.stderr:write("--datastore requires a SQLite path\n")
        usage()
        os.exit(2)
      end
      datastore_path = value
      arg_i = arg_i + 2
    elseif name == "--peerstore" then
      if not value or value == "" then
        io.stderr:write("--peerstore requires a SQLite path\n")
        usage()
        os.exit(2)
      end
      peerstore_path = value
      arg_i = arg_i + 2
    else
      listen_addrs[#listen_addrs + 1] = name
      arg_i = arg_i + 1
    end
  end

  if use_default_bootstrap and #bootstrappers == 0 then
    bootstrappers = bootstrap_defaults.default_bootstrappers()
  end

  local bootstrap_config = nil
  if use_default_bootstrap or #bootstrappers > 0 then
    bootstrap_config = {
      module = peer_discovery_bootstrap,
      config = {
        dialable_only = true,
        ignore_resolve_errors = true,
        dial_on_start = true,
      },
    }
    if #bootstrappers > 0 then
      bootstrap_config.config.list = bootstrappers
    end
  end

  if #listen_addrs == 0 then
    listen_addrs = {
      "/ip4/0.0.0.0/tcp/4001",
      "/ip6/::/tcp/4001",
    }
  end

  local identity, identity_err = load_or_create_identity(identity_key_path)
  if not identity then
    io.stderr:write("identity init failed: " .. tostring(identity_err) .. "\n")
    os.exit(1)
  end

  local dht_datastore, datastore_err = open_optional_datastore(datastore_path)
  if datastore_path and not dht_datastore then
    io.stderr:write("datastore init failed: " .. tostring(datastore_err) .. "\n")
    os.exit(1)
  end

  local ps, ps_datastore, ps_err = open_optional_peerstore(peerstore_path)
  if peerstore_path and not ps then
    if dht_datastore and dht_datastore.close then
      dht_datastore:close()
    end
    io.stderr:write("peerstore init failed: " .. tostring(ps_err) .. "\n")
    os.exit(1)
  end

  local host, host_err = host_mod.new({
    identity = identity,
    peerstore = ps,
    listen_addrs = listen_addrs,
    peer_discovery = bootstrap_config and { bootstrap = bootstrap_config } or nil,
    services = {
      identify = { module = identify_service },
      ping = { module = ping_service },
      kad_dht = {
        module = kad_dht_service,
        config = {
          mode = "server",
          datastore = dht_datastore,
          max_concurrent_queries = 4,
        },
      },
    },
    connection_manager = {
      -- Keep the public demo comfortably below common 256-FD process limits.
      low_water = 96,
      high_water = 128,
      grace_period = 60,
      silence_period = 10,
      max_connections = 160,
      max_inbound_connections = 144,
      max_outbound_connections = 32,
      max_parallel_dials = 4,
      max_dial_queue_length = 64,
      max_peer_addrs_to_dial = 2,
    },
    blocking = false,
    accept_timeout = 0.05,
    tcp = {
      listen_backlog = 4096,
    },
    on_started = function(h)
      io.stdout:write("kad-dht demo server listening:\n")
      for _, addr in ipairs(h:get_multiaddrs()) do
        io.stdout:write("  " .. addr .. "\n")
      end
      io.stdout:write("running; Ctrl-C to stop\n")
      io.stdout:write("identity key: " .. tostring(identity_key_path) .. "\n")
      if datastore_path then
        io.stdout:write("kad datastore: " .. tostring(datastore_path) .. "\n")
      end
      if peerstore_path then
        io.stdout:write("peerstore: " .. tostring(peerstore_path) .. "\n")
      end
      io.stdout:flush()
    end,
  })
  if not host then
    if dht_datastore and dht_datastore.close then
      dht_datastore:close()
    end
    if ps_datastore and ps_datastore.close then
      ps_datastore:close()
    end
    io.stderr:write("host init failed: " .. tostring(host_err) .. "\n")
    os.exit(1)
  end

  local started, start_err = host:start()
  if not started then
    if dht_datastore and dht_datastore.close then
      dht_datastore:close()
    end
    if ps_datastore and ps_datastore.close then
      ps_datastore:close()
    end
    io.stderr:write("host stopped with error: " .. tostring(start_err) .. "\n")
    os.exit(1)
  end

  host:enable_perf_stats()

  local self_peer_watch_ok, self_peer_watch_err = host:on("self_peer_update", function(payload)
    local peer_id = payload and payload.peer_id or "unknown"
    local addrs = payload and payload.addrs or {}
    io.stdout:write(
      string.format("%s [self_peer_update] peer_id=%s addrs=%d\n", now_iso8601(), tostring(peer_id), #addrs)
    )
    for _, addr in ipairs(addrs) do
      io.stdout:write("  " .. tostring(addr) .. "\n")
    end
    io.stdout:flush()
  end)
  if not self_peer_watch_ok then
    io.stderr:write("failed to watch self_peer_update events: " .. tostring(self_peer_watch_err) .. "\n")
    os.exit(1)
  end

  local signal_toggles = install_log_signal_toggles()
  if signal_toggles then
    io.stdout:write("log toggles: SIGUSR1 => *=debug, SIGUSR2 => restore\n")
    io.stdout:flush()
  end

  local status_task, status_err = host:spawn_task("example.kad_stats", function(ctx)
    while true do
      print_server_stats(host)
      local slept, sleep_err = ctx:sleep(30)
      if slept == nil and sleep_err then
        return nil, sleep_err
      end
    end
  end, { service = "example" })
  if not status_task then
    io.stderr:write("failed to start stats task: " .. tostring(status_err) .. "\n")
    os.exit(1)
  end

  local health_task, health_err = host:spawn_task("example.health_stats", function(ctx)
    while true do
      print_internal_health(host)
      local slept, sleep_err = ctx:sleep(10)
      if slept == nil and sleep_err then
        return nil, sleep_err
      end
    end
  end, { service = "example" })
  if not health_task then
    io.stderr:write("failed to start health task: " .. tostring(health_err) .. "\n")
    os.exit(1)
  end

  while true do
    local slept, sleep_err = host:sleep(1)
    if slept == nil and sleep_err then
      if signal_toggles and signal_toggles.close then
        signal_toggles:close()
      end
      io.stderr:write("host sleep failed: " .. tostring(sleep_err) .. "\n")
      os.exit(1)
    end
  end
end

local function run_client()
  local bootstrap_arg
  local datastore_path
  local peerstore_path
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

  local arg_i = 2
  while arg_i <= #arg do
    local name = arg[arg_i]
    local value = arg[arg_i + 1]
    if name == "--datastore" then
      if not value or value == "" then
        io.stderr:write("--datastore requires a SQLite path\n")
        usage()
        os.exit(2)
      end
      datastore_path = value
      arg_i = arg_i + 2
    elseif name == "--peerstore" then
      if not value or value == "" then
        io.stderr:write("--peerstore requires a SQLite path\n")
        usage()
        os.exit(2)
      end
      peerstore_path = value
      arg_i = arg_i + 2
    elseif bootstrap_arg == nil then
      bootstrap_arg = name
      arg_i = arg_i + 1
    else
      io.stderr:write("unexpected argument: " .. tostring(name) .. "\n")
      usage()
      os.exit(2)
    end
  end

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

  local dht_datastore, datastore_err = open_optional_datastore(datastore_path)
  if datastore_path and not dht_datastore then
    io.stderr:write("datastore init failed: " .. tostring(datastore_err) .. "\n")
    os.exit(1)
  end

  local ps, ps_datastore, ps_err = open_optional_peerstore(peerstore_path)
  if peerstore_path and not ps then
    if dht_datastore and dht_datastore.close then
      dht_datastore:close()
    end
    io.stderr:write("peerstore init failed: " .. tostring(ps_err) .. "\n")
    os.exit(1)
  end

  local host, host_err = host_mod.new({
    runtime = "luv",
    peerstore = ps,
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
          datastore = dht_datastore,
        },
      },
    },
    blocking = false,
    connect_timeout = 6,
    io_timeout = 10,
    accept_timeout = 0.05,
  })
  if not host then
    if dht_datastore and dht_datastore.close then
      dht_datastore:close()
    end
    if ps_datastore and ps_datastore.close then
      ps_datastore:close()
    end
    io.stderr:write("host init failed: " .. tostring(host_err) .. "\n")
    os.exit(1)
  end

  local started, start_err = host:start()
  if not started then
    if dht_datastore and dht_datastore.close then
      dht_datastore:close()
    end
    if ps_datastore and ps_datastore.close then
      ps_datastore:close()
    end
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
    io.stdout:write(
      "  peer=" .. tostring(candidate.peer_id) .. " addr=" .. tostring((candidate.addrs or {})[1]) .. "\n"
    )
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
  if dht_datastore and dht_datastore.close then
    dht_datastore:close()
  end
  if ps_datastore and ps_datastore.close then
    ps_datastore:close()
  end
end

if mode == "server" then
  run_server()
elseif mode == "client" then
  run_client()
else
  usage()
  os.exit(2)
end
