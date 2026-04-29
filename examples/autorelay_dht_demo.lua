package.path = table.concat({
  "./?.lua",
  "./?/init.lua",
  package.path,
}, ";")

local host_mod = require("lua_libp2p.host")
local ed25519 = require("lua_libp2p.crypto.ed25519")

local DEFAULT_RELAY = "/ip4/54.38.47.166/tcp/4001/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb"
local DEFAULT_KEY_PATH = "examples/.autorelay_dht_demo.ed25519.key"

local function usage()
  io.stderr:write([[
Usage:
  lua examples/autorelay_dht_demo.lua [--relay <multiaddr>] [--max-reservations 1] [--status-interval 30] [--key <path>]

Reserves a static circuit relay without DHT crawling. The node keeps running so
relay reservations remain valid. Press Ctrl-C to stop.
]])
end

local function parse_args(args)
  local opts = {
    relays = { DEFAULT_RELAY },
    max_reservations = 1,
    status_interval = 30,
    key_path = DEFAULT_KEY_PATH,
  }
  local i = 1
  while i <= #args do
    local name = args[i]
    local value = args[i + 1]
    if name == "--relay" or name == "--bootstrap" then
      if not value then
        return nil, name .. " requires a multiaddr"
      end
      if name == "--relay" and #opts.relays == 1 and opts.relays[1] == DEFAULT_RELAY then
        opts.relays = {}
      end
      opts.relays[#opts.relays + 1] = value
      i = i + 2
    elseif name == "--max-reservations" then
      opts.max_reservations = tonumber(value)
      i = i + 2
    elseif name == "--status-interval" then
      opts.status_interval = tonumber(value)
      i = i + 2
    elseif name == "--key" then
      if not value then
        return nil, "--key requires a path"
      end
      opts.key_path = value
      i = i + 2
    else
      return nil, "unknown argument: " .. tostring(name)
    end
  end
  return opts
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
  local keypair, key_err = ed25519.generate_keypair()
  if not keypair then
    return nil, key_err
  end
  local saved, save_err = ed25519.save_private_key(path, keypair)
  if not saved then
    return nil, save_err
  end
  return keypair
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

local identity, identity_err = load_or_create_identity(opts.key_path)
if not identity then
  io.stderr:write("identity init failed: " .. tostring(identity_err) .. "\n")
  os.exit(1)
end

local h, host_err = host_mod.new({
  runtime = "luv",
  identity = identity,
  listen_addrs = {
    "/ip4/127.0.0.1/tcp/0",
    "/p2p-circuit",
  },
  services = { "identify", "ping", "autorelay" },
  autorelay = {
    relays = opts.relays,
    max_reservations = opts.max_reservations,
    discover = false,
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

local started, start_err = h:start()
if not started then
  io.stderr:write("host start failed: " .. tostring(start_err) .. "\n")
  os.exit(1)
end

io.stdout:write("autorelay is active with static relay(s); DHT crawl is disabled\n")
io.stdout:write("identity key: " .. tostring(opts.key_path) .. "\n")
io.stdout:write("peer id: " .. tostring(h:peer_id().id) .. "\n")
for _, relay in ipairs(opts.relays) do
  io.stdout:write("static relay: " .. tostring(relay) .. "\n")
end
print_status(h)

local pumped, pump_err = scheduler_sleep(h, 1)
if not pumped then
  io.stderr:write("host pump failed: " .. tostring(pump_err) .. "\n")
end
print_status(h)

local running = true
local previous_int = signal and signal("int", function()
  running = false
end) or nil

io.stdout:write("running; press Ctrl-C to stop\n")
local run_task, run_task_err = h:spawn_task("example.autorelay_status", function(ctx)
  local next_status_at = os.time() + (opts.status_interval or 30)
  while running do
    local slept, sleep_err = ctx:sleep(0.05)
    if slept == nil and sleep_err then
      return nil, sleep_err
    end
    local now = os.time()
    if opts.status_interval and opts.status_interval > 0 and now >= next_status_at then
      print_status(h)
      next_status_at = now + opts.status_interval
    end
  end
  return true
end, { service = "example" })
if not run_task then
  io.stderr:write("status task failed: " .. tostring(run_task_err) .. "\n")
else
  local _, run_err = wait_task(h, run_task, 0.05)
  if run_err then
    io.stderr:write("host run failed: " .. tostring(run_err) .. "\n")
  end
end

if previous_int then
  signal("int", previous_int)
end
h:stop()
io.stdout:write("stopped\n")
