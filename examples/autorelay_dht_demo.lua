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
local ed25519 = require("lua_libp2p.crypto.ed25519")
local signal = rawget(_G, "signal")

local DEFAULT_KEY_PATH = "examples/.autorelay_dht_demo.ed25519.key"

local function usage()
	io.stderr:write([[
Usage:
  lua examples/autorelay_dht_demo.lua [--bootstrap <multiaddr>] [--relay <multiaddr>] [--no-dht-walk] [--dht-walk-delay 0] [--dht-alpha 3] [--dht-paths 2] [--dht-bootstrap-peers 2] [--max-reservations 2] [--status-interval 30] [--key <path>]

Bootstraps the public DHT, discovers relay-capable peers, and keeps AutoRelay
reservations active. Pass --relay to add configured relay candidates. Press
Ctrl-C to stop.
]])
end

local function parse_args(args)
	local opts = {
		bootstrap_addrs = {},
		relays = {},
		max_reservations = 2,
		status_interval = 30,
		key_path = DEFAULT_KEY_PATH,
		dht_walk = true,
		dht_walk_delay = 0,
		dht_alpha = 3,
		dht_paths = 2,
		dht_bootstrap_peers = 2,
	}
	local i = 1
	while i <= #args do
		local name = args[i]
		local value = args[i + 1]
		if name == "--relay" then
			if not value then
				return nil, name .. " requires a multiaddr"
			end
			opts.relays[#opts.relays + 1] = value
			i = i + 2
		elseif name == "--bootstrap" then
			if not value then
				return nil, "--bootstrap requires a multiaddr"
			end
			opts.bootstrap_addrs[#opts.bootstrap_addrs + 1] = value
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
		elseif name == "--no-dht-walk" then
			opts.dht_walk = false
			i = i + 1
		elseif name == "--dht-alpha" then
			opts.dht_alpha = tonumber(value)
			i = i + 2
		elseif name == "--dht-walk-delay" then
			opts.dht_walk_delay = tonumber(value)
			i = i + 2
		elseif name == "--dht-paths" then
			opts.dht_paths = tonumber(value)
			i = i + 2
		elseif name == "--dht-bootstrap-peers" then
			opts.dht_bootstrap_peers = tonumber(value)
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

local function print_status(host)
	local now = os.time()
	local reservations = host.autorelay and host.autorelay:get_reservations() or {}
	local relay_status = host.autorelay and host.autorelay:status() or {}
	local dht_peers = host.kad_dht and host.kad_dht.routing_table and host.kad_dht.routing_table:all_peers() or {}
	io.stdout:write("dht: routing_table_peers=" .. tostring(#dht_peers) .. "\n")
	io.stdout:write(
		"autorelay: reservations="
			.. tostring(relay_status.reservations or 0)
			.. " queued="
			.. tostring(relay_status.queued or 0)
			.. " failed="
			.. tostring(relay_status.failed or 0)
			.. " invalid="
			.. tostring(relay_status.invalid or 0)
			.. " need_more="
			.. tostring(relay_status.need_more_relays == true)
			.. "\n"
	)
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
	io.stdout:flush()
end

local function print_local_identify_addrs(host)
	io.stdout:write("local identify addrs:\n")
	for _, addr in ipairs(host:get_multiaddrs()) do
		if addr:match("^/ip4/127%.0%.0%.1/tcp/") then
			io.stdout:write("  " .. tostring(addr) .. "\n")
		end
	end
	io.stdout:flush()
end

local function print_dht_report(label, report)
	if not report then
		return
	end
	io.stdout:write(label .. " report:\n")
	for _, key in ipairs({
		"attempted",
		"connected",
		"queried",
		"responses",
		"added",
		"skipped",
		"failed",
		"cancelled",
		"active_peak",
		"termination",
	}) do
		if report[key] ~= nil then
			io.stdout:write("  " .. key .. ": " .. tostring(report[key]) .. "\n")
		end
	end
end

local function start_dht_discovery(host, opts)
	if not host.kad_dht then
		return nil
	end
	local options = opts or {}
	return host:spawn_task("example.dht_discovery", function(ctx)
		local deadline = os.time() + (options.dht_seed_timeout or 30)
		while #host.kad_dht.routing_table:all_peers() < (options.dht_bootstrap_peers or 2) and os.time() < deadline do
			local slept, sleep_err = ctx:sleep(0.25)
			if slept == nil and sleep_err then
				return nil, sleep_err
			end
		end
		io.stdout:write("dht seeded peers: " .. tostring(#host.kad_dht.routing_table:all_peers()) .. "\n")

		if not options.dht_walk then
			return true
		end
		if options.dht_walk_delay and options.dht_walk_delay > 0 then
			local slept, sleep_err = ctx:sleep(options.dht_walk_delay)
			if slept == nil and sleep_err then
				return nil, sleep_err
			end
		end

		local walk_op, walk_task_err = host.kad_dht:random_walk({
			alpha = options.dht_alpha or 3,
			disjoint_paths = options.dht_paths or 2,
		})
		if not walk_op then
			io.stderr:write("dht random walk failed to start: " .. tostring(walk_task_err) .. "\n")
			return nil, walk_task_err
		end
		local walk_report, walk_err = walk_op:result({ ctx = ctx })
		if not walk_report then
			io.stderr:write("dht random walk failed: " .. tostring(walk_err) .. "\n")
		else
			print_dht_report("dht random walk", walk_report)
		end
		return true
	end, { service = "example" })
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

local peer_discovery = {
	bootstrap = {
		module = peer_discovery_bootstrap,
		config = {
			dial_on_start = true,
		},
	},
}
if #opts.bootstrap_addrs > 0 then
	peer_discovery.bootstrap = {
		module = peer_discovery_bootstrap,
		config = {
			list = opts.bootstrap_addrs,
			dial_on_start = true,
		},
	}
end

local h, host_err = host_mod.new({
	runtime = "luv",
	identity = identity,
	peer_discovery = peer_discovery,
	listen_addrs = {
		"/ip6/2a00:23c7:ad38:6601:1dbf:9fc1:137b:13c4/tcp/0",
		"/ip4/127.0.0.1/tcp/0",
		"/p2p-circuit",
	},
	services = {
		identify = { module = identify_service },
		ping = { module = ping_service },
		kad_dht = {
			module = kad_dht_service,
			config = {
				mode = "client",
			},
		},
		autorelay = {
			module = autorelay_service,
			config = {
				relays = opts.relays,
				max_reservations = opts.max_reservations,
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

local started, start_err = h:start()
if not started then
	io.stderr:write("host start failed: " .. tostring(start_err) .. "\n")
	os.exit(1)
end

io.stdout:write("autorelay is active with DHT relay discovery\n")
io.stdout:write("identity key: " .. tostring(opts.key_path) .. "\n")
io.stdout:write("peer id: " .. tostring(h:peer_id().id) .. "\n")
print_local_identify_addrs(h)
if #opts.bootstrap_addrs > 0 then
	for _, bootstrap in ipairs(opts.bootstrap_addrs) do
		io.stdout:write("bootstrap: " .. tostring(bootstrap) .. "\n")
	end
else
	io.stdout:write("bootstrap: default public libp2p bootstrappers\n")
end
if opts.dht_walk then
	io.stdout:write(
		"dht walk: enabled alpha="
			.. tostring(opts.dht_alpha)
			.. " paths="
			.. tostring(opts.dht_paths)
			.. " bootstrap_peers="
			.. tostring(opts.dht_bootstrap_peers)
			.. "\n"
	)
	io.stdout:write("dht walk delay: " .. tostring(opts.dht_walk_delay) .. "s\n")
else
	io.stdout:write("dht walk: disabled\n")
end
for _, relay in ipairs(opts.relays) do
	io.stdout:write("configured relay: " .. tostring(relay) .. "\n")
end
io.stdout:flush()
print_status(h)

local dht_task, dht_task_err = start_dht_discovery(h, {
	dht_walk = opts.dht_walk,
	dht_alpha = opts.dht_alpha,
	dht_paths = opts.dht_paths,
	dht_bootstrap_peers = opts.dht_bootstrap_peers,
	dht_walk_delay = opts.dht_walk_delay,
})
if not dht_task then
	io.stderr:write("dht discovery task failed: " .. tostring(dht_task_err) .. "\n")
end

local pumped, pump_err = h:sleep(1)
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
	local _, run_err = h:wait_task(run_task, { poll_interval = 0.05 })
	if run_err then
		io.stderr:write("host run failed: " .. tostring(run_err) .. "\n")
	end
end

if previous_int then
	signal("int", previous_int)
end
h:stop()
io.stdout:write("stopped\n")
