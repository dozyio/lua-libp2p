package.path = table.concat({
  "./?.lua",
  "./?/init.lua",
  package.path,
}, ";")

local host_mod = require("lua_libp2p.host")

local function usage()
  io.stderr:write([[
Usage:
  lua examples/autorelay_dht_demo.lua [--bootstrap <multiaddr>] [--max-reservations 2] [--status-interval 30]

Bootstraps the DHT, lets identify surface relay-hop protocol support, and lets
AutoRelay reserve discovered relay candidates. The node keeps running so relay
reservations remain valid. Press Ctrl-C to stop.
]])
end

local function parse_args(args)
  local opts = {
    bootstrappers = {},
    max_reservations = 2,
    status_interval = 30,
  }
  local i = 1
  while i <= #args do
    local name = args[i]
    local value = args[i + 1]
    if name == "--bootstrap" then
      if not value then
        return nil, "--bootstrap requires a multiaddr"
      end
      opts.bootstrappers[#opts.bootstrappers + 1] = value
      i = i + 2
    elseif name == "--max-reservations" then
      opts.max_reservations = tonumber(value)
      i = i + 2
    elseif name == "--status-interval" then
      opts.status_interval = tonumber(value)
      i = i + 2
    else
      return nil, "unknown argument: " .. tostring(name)
    end
  end
  return opts
end

local function wait_task(host, task, interval)
  return host:run_until_task(task, { poll_interval = interval })
end

local function scheduler_sleep(host, seconds)
  local task, task_err = host:spawn_task("example.sleep", function(ctx)
    local slept, sleep_err = ctx:sleep(seconds)
    if slept == nil and sleep_err then
      return nil, sleep_err
    end
    return true
  end, { service = "example" })
  if not task then
    return nil, task_err
  end
  return wait_task(host, task)
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

local function print_status(host)
  local now = os.time()
  local reservations = host.autorelay and host.autorelay:get_reservations() or {}
  local relay_status = host.autorelay and host.autorelay:status() or {}
  io.stdout:write("autorelay: reservations=" .. tostring(relay_status.reservations or 0)
    .. " queued=" .. tostring(relay_status.queued or 0)
    .. " failed=" .. tostring(relay_status.failed or 0)
    .. " invalid=" .. tostring(relay_status.invalid or 0)
    .. " need_more=" .. tostring(relay_status.need_more_relays == true) .. "\n")
  if relay_status.failure_summary then
    local rows = {}
    for message, count in pairs(relay_status.failure_summary) do
      rows[#rows + 1] = { message = message, count = count }
    end
    table.sort(rows, function(a, b)
      if a.count == b.count then
        return a.message < b.message
      end
      return a.count > b.count
    end)
    local max_rows = math.min(#rows, 5)
    if max_rows > 0 then
      io.stdout:write("autorelay failures:\n")
      for i = 1, max_rows do
        io.stdout:write("  " .. tostring(rows[i].count) .. "x " .. tostring(rows[i].message) .. "\n")
      end
    end
  end
  io.stdout:write("relay reservations: " .. tostring(#reservations) .. "\n")
  for _, reservation in ipairs(reservations) do
    io.stdout:write("  relay: " .. tostring(reservation.relay_peer_id) .. "\n")
    local expire = reservation.reservation and reservation.reservation.expire
    if expire then
      io.stdout:write("    expires_in: " .. tostring(expire - now) .. "s\n")
    end
    if reservation.next_refresh_at then
      io.stdout:write("    refresh_in: " .. tostring(reservation.next_refresh_at - now) .. "s\n")
    end
    for _, addr in ipairs(reservation.relay_addrs or {}) do
      io.stdout:write("    " .. tostring(addr) .. "\n")
    end
  end

  io.stdout:write("advertised addrs:\n")
  for _, addr in ipairs(host:get_multiaddrs()) do
    io.stdout:write("  " .. tostring(addr) .. "\n")
  end
end

local opts, opts_err = parse_args(arg)
if not opts then
  io.stderr:write(tostring(opts_err) .. "\n")
  usage()
  os.exit(2)
end

local h, host_err = host_mod.new({
  runtime = "luv",
  peer_discovery = {
    bootstrap = bootstrap_config(opts),
  },
  listen_addrs = {
    "/ip4/127.0.0.1/tcp/0",
    "/p2p-circuit",
  },
  services = { "identify", "kad_dht", "autorelay" },
  kad_dht = {
    mode = "client",
    alpha = 10,
    disjoint_paths = 10,
  },
  autorelay = {
    max_reservations = opts.max_reservations,
  },
  blocking = false,
  scheduler_connection_pump = true,
  connect_timeout = 6,
  io_timeout = 10,
  accept_timeout = 0.05,
})
if not h then
  io.stderr:write("host init failed: " .. tostring(host_err) .. "\n")
  os.exit(1)
end

local started, start_err = h:start()
if not started then
  io.stderr:write("host start failed: " .. tostring(start_err) .. "\n")
  os.exit(1)
end

local dht = h.kad_dht

io.stdout:write("autorelay is active before DHT walk; reservations are created as identify discovers relay-hop peers\n")
print_status(h)

local bootstrap_task, bootstrap_task_err = dht:start_bootstrap()
if not bootstrap_task then
  io.stderr:write("dht bootstrap failed: " .. tostring(bootstrap_task_err) .. "\n")
  h:stop()
  os.exit(1)
end
local report, bootstrap_err = wait_task(h, bootstrap_task)
if not report then
  io.stderr:write("dht bootstrap failed: " .. tostring(bootstrap_err) .. "\n")
  h:stop()
  os.exit(1)
end
io.stdout:write("bootstrap: attempted=" .. tostring(report.attempted)
  .. " connected=" .. tostring(report.connected)
  .. " added=" .. tostring(report.added)
  .. " failed=" .. tostring(report.failed) .. "\n")

local walk_started_at = os.time()
local walk_task, walk_task_err = dht:start_random_walk({
  alpha = 10,
  disjoint_paths = 10,
  bootstrap_if_empty = true,
})
local walk, walk_err
if walk_task then
  walk, walk_err = wait_task(h, walk_task)
else
  walk_err = walk_task_err
end
if not walk then
  io.stderr:write("dht random walk failed: " .. tostring(walk_err) .. "\n")
else
  io.stdout:write("dht walk: queried=" .. tostring(walk.queried)
    .. " responses=" .. tostring(walk.responses)
    .. " added=" .. tostring(walk.added)
    .. " failed=" .. tostring(walk.failed)
    .. " termination=" .. tostring(walk.termination)
    .. " duration=" .. tostring(os.time() - walk_started_at) .. "s\n")
end

local pumped, pump_err = scheduler_sleep(h, 1)
if not pumped then
  io.stderr:write("host pump failed: " .. tostring(pump_err) .. "\n")
end
if h.autorelay then
  local added = h.autorelay:scan_peerstore()
  if added > 0 then
    io.stdout:write("autorelay peerstore scan queued=" .. tostring(added) .. "\n")
  end
end

print_status(h)

local running = true
local previous_int = signal and signal("int", function()
  running = false
end) or nil

io.stdout:write("running; press Ctrl-C to stop\n")
local next_status_at = os.time() + (opts.status_interval or 30)
while running do
  local ok, err = h:poll_once(0.05)
  if not ok then
    io.stderr:write("host poll failed: " .. tostring(err) .. "\n")
    break
  end
  local now = os.time()
  if opts.status_interval and opts.status_interval > 0 and now >= next_status_at then
    print_status(h)
    next_status_at = now + opts.status_interval
  end
end

if previous_int then
  signal("int", previous_int)
end
h:stop()
io.stdout:write("stopped\n")
