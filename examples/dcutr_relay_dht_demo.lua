package.path = table.concat({
	"./?.lua",
	"./?/init.lua",
	package.path,
}, ";")

local host_mod = require("lua_libp2p.host")
local identify_service = require("lua_libp2p.protocol_identify.service")
local ping_service = require("lua_libp2p.protocol_ping.service")
local dcutr_service = require("lua_libp2p.protocol_dcutr.service")
local autonat_service = require("lua_libp2p.autonat.client")
local kad_dht_service = require("lua_libp2p.kad_dht")
local autorelay_service = require("lua_libp2p.transport_circuit_relay_v2.autorelay")
local peer_discovery_bootstrap = require("lua_libp2p.peer_discovery_bootstrap")
local ed25519 = require("lua_libp2p.crypto.ed25519")

local DEFAULT_KEY_PATH = "examples/.dcutr_relay_dht_demo.ed25519.key"

local function usage()
	io.stderr:write([[
Usage:
  lua examples/dcutr_relay_dht_demo.lua [--bootstrap <multiaddr>] [--relay <multiaddr>] [--status-interval 20] [--duration 0] [--key <path>] [--debug-connections]

Starts a host with Identify/Ping/KAD-DHT/AutoRelay/DCUtR. The host bootstraps
discovery, keeps relay reservations, and emits DCUtR upgrade events.

Options:
  --bootstrap <multiaddr>   Add bootstrap peer discovery address (repeatable)
  --relay <multiaddr>       Add static relay candidate for AutoRelay (repeatable)
  --status-interval <sec>   Status print interval (default: 20)
  --duration <sec>          Optional run duration (0 means run forever)
  --key <path>              Identity key file path
  --debug-connections       Print connection, stream, and identify path diagnostics
]])
end

local function parse_args(args)
	local opts = {
		bootstrap_addrs = {},
		relays = {},
		status_interval = 20,
		duration = 0,
		key_path = DEFAULT_KEY_PATH,
		dcutr_auto_init = true,
		debug_connections = false,
	}
	local i = 1
	while i <= #args do
		local name = args[i]
		local value = args[i + 1]
		if name == "--bootstrap" then
			if not value then
				return nil, "--bootstrap requires a multiaddr"
			end
			opts.bootstrap_addrs[#opts.bootstrap_addrs + 1] = value
			i = i + 2
		elseif name == "--relay" then
			if not value then
				return nil, "--relay requires a multiaddr"
			end
			opts.relays[#opts.relays + 1] = value
			i = i + 2
		elseif name == "--status-interval" then
			opts.status_interval = tonumber(value)
			i = i + 2
		elseif name == "--duration" then
			opts.duration = tonumber(value)
			i = i + 2
		elseif name == "--key" then
			if not value then
				return nil, "--key requires a path"
			end
			opts.key_path = value
			i = i + 2
		elseif name == "--dcutr-auto-init" then
			opts.dcutr_auto_init = true
			i = i + 1
		elseif name == "--debug-connections" then
			opts.debug_connections = true
			i = i + 1
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
	local reservations = host.autorelay and host.autorelay:get_reservations() or {}
	local dht_peers = host.kad_dht and host.kad_dht.routing_table and host.kad_dht.routing_table:all_peers() or {}
	io.stdout:write(
		"status: dht_peers=" .. tostring(#dht_peers) .. " relay_reservations=" .. tostring(#reservations) .. "\n"
	)
	for _, addr in ipairs(host:get_multiaddrs()) do
		io.stdout:write("  addr: " .. tostring(addr) .. "\n")
	end
	io.stdout:flush()
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

local host, host_err = host_mod.new({
	runtime = "auto",
	identity = identity,
	debug_connection_events = opts.debug_connections,
	listen_addrs = {
		"/ip4/0.0.0.0/tcp/0",
		"/ip4/0.0.0.0/tcp/0/p2p-circuit",
	},
	peer_discovery = {
		bootstrap = {
			module = peer_discovery_bootstrap,
			config = {
				addrs = (#opts.bootstrap_addrs > 0) and opts.bootstrap_addrs or nil,
			},
		},
	},
	services = {
		identify = { module = identify_service },
		ping = { module = ping_service },
		autonat = {
			module = autonat_service,
			config = {
				monitor_on_start = true,
				monitor_start_opts = {
					poll_interval = 0.05,
					discovery_timeout = 60,
					max_autonat_servers = 12,
					target_autonat_responses = 2,
					stop_on_first_reachable = false,
					retry_interval_seconds = 5,
					healthy_interval_seconds = 30,
					min_success_rounds = 2,
					checked_peer_cooldown_seconds = 30,
				},
			},
		},
		kad_dht = { module = kad_dht_service },
		autorelay = {
			module = autorelay_service,
			config = {
				min_reservations = 1,
				max_reservations = 1,
				relay_addrs = opts.relays,
			},
		},
		dcutr = {
			module = dcutr_service,
			config = {
				auto_on_relay_connection = opts.dcutr_auto_init,
				relay_grace_seconds = 5,
			},
		},
	},
	blocking = false,
})

if not host then
	io.stderr:write("host init failed: " .. tostring(host_err) .. "\n")
	os.exit(1)
end

if opts.debug_connections then
	host:on("connection_opened", function(payload)
		local state = payload and payload.state or {}
		local relay = state.relay or {}
		io.stdout:write(
			"conn opened: id="
				.. tostring(payload and payload.connection_id)
				.. " peer="
				.. tostring(payload and payload.peer_id)
				.. " direction="
				.. tostring(state.direction)
				.. " relay_limit="
				.. tostring(relay.limit_kind)
				.. " remote_addr="
				.. tostring(state.remote_addr)
				.. "\n"
		)
		return true
	end)

	host:on("connection_closed", function(payload)
		local state = payload and payload.state or {}
		local relay = state.relay or {}
		io.stdout:write(
			"conn closed: id="
				.. tostring(payload and payload.connection_id)
				.. " peer="
				.. tostring(payload and payload.peer_id)
				.. " direction="
				.. tostring(state.direction)
				.. " relay_limit="
				.. tostring(relay.limit_kind)
				.. " cause="
				.. tostring(payload and payload.cause)
				.. "\n"
		)
		return true
	end)

	host:on("stream:negotiated", function(payload)
		io.stdout:write(
			"stream negotiated: conn="
				.. tostring(payload and payload.connection_id)
				.. " peer="
				.. tostring(payload and payload.peer_id)
				.. " protocol="
				.. tostring(payload and payload.protocol)
				.. " limited="
				.. tostring(payload and payload.limited)
				.. " relay_limit="
				.. tostring(payload and payload.relay_limit_kind)
				.. " remote_addr="
				.. tostring(payload and payload.remote_addr)
				.. "\n"
		)
		return true
	end)

	host:on("identify:response:send", function(payload)
		io.stdout:write(
			"identify send: conn="
				.. tostring(payload and payload.connection_id)
				.. " peer="
				.. tostring(payload and payload.peer_id)
				.. " limited="
				.. tostring(payload and payload.limited)
				.. " relay_limit="
				.. tostring(payload and payload.relay_limit_kind)
				.. " observed="
				.. tostring(payload and payload.observed_addr)
				.. " listen_addrs="
				.. tostring(payload and payload.listen_addr_count)
				.. "\n"
		)
		return true
	end)

	host:on("dcutr:unilateral:success", function(payload)
		io.stdout:write(
			"dcutr unilateral success: peer="
				.. tostring(payload and payload.peer_id)
				.. " addr="
				.. tostring(payload and payload.addr)
				.. "\n"
		)
		return true
	end)
end

host:on("dcutr:migrated", function(payload)
	io.stdout:write(
		"dcutr migrated: peer="
			.. tostring(payload and payload.peer_id)
			.. " direction="
			.. tostring(payload and payload.direction)
			.. " addr="
			.. tostring(payload and payload.addr)
			.. "\n"
	)
	return true
end)

host:on("dcutr:attempt:retry", function(payload)
	io.stdout:write(
		"dcutr retry: attempt="
			.. tostring(payload and payload.attempt)
			.. "/"
			.. tostring(payload and payload.max_attempts)
			.. " error="
			.. tostring(payload and payload.error)
			.. "\n"
	)
	return true
end)

host:on("dcutr:attempt:failed", function(payload)
	io.stdout:write(
		"dcutr failed: reason="
			.. tostring(payload and payload.reason)
			.. " peer="
			.. tostring(payload and payload.peer_id)
			.. " error="
			.. tostring(payload and payload.error)
			.. "\n"
	)
	return true
end)

host:on("dcutr:attempt:precheck", function(payload)
	io.stdout:write(
		"dcutr precheck: peer="
			.. tostring(payload and payload.peer_id)
			.. " direction="
			.. tostring(payload and payload.direction)
			.. " relay_limit="
			.. tostring(payload and payload.relay_limit_kind)
			.. " remote_candidates="
			.. tostring(payload and payload.remote_candidate_addrs)
			.. " remote_supports_dcutr="
			.. tostring(payload and payload.remote_supports_dcutr)
			.. " local_obs="
			.. tostring(payload and payload.local_observed_addrs)
			.. "\n"
	)
	return true
end)

host:on("dcutr:attempt:skipped", function(payload)
	io.stdout:write(
		"dcutr skipped: peer="
			.. tostring(payload and payload.peer_id)
			.. " reason="
			.. tostring(payload and payload.reason)
			.. "\n"
	)
	return true
end)

host:on("autonat:address:checked", function(payload)
	io.stdout:write(
		"autonat checked: addr="
			.. tostring(payload and payload.addr)
			.. " reachable="
			.. tostring(payload and payload.reachable)
			.. " response_status="
			.. tostring(payload and payload.response_status)
			.. "\n"
	)
	return true
end)

host:on("autonat:address:reachable", function(payload)
	io.stdout:write("autonat reachable: " .. tostring(payload and payload.addr) .. "\n")
	return true
end)

host:on("autonat:address:unreachable", function(payload)
	io.stdout:write("autonat unreachable: " .. tostring(payload and payload.addr) .. "\n")
	return true
end)

host:on("dcutr:connect:send", function(payload)
	io.stdout:write(
		"dcutr connect send: direction="
			.. tostring(payload and payload.direction)
			.. " peer="
			.. tostring(payload and payload.peer_id)
			.. " addrs="
			.. tostring(payload and payload.obs_addr_count)
			.. "\n"
	)
	for _, addr in ipairs(payload and payload.obs_addrs or {}) do
		io.stdout:write("  dcutr obs: " .. tostring(addr) .. "\n")
	end
	return true
end)

local started, start_err = host:start()
if not started then
	io.stderr:write("host start failed: " .. tostring(start_err) .. "\n")
	os.exit(1)
end

io.stdout:write("peer id: " .. tostring(host:peer_id().id) .. "\n")
io.stdout:write("dcutr mode: " .. (opts.dcutr_auto_init and "active auto-init" or "passive responder") .. "\n")
for _, addr in ipairs(host:get_multiaddrs()) do
	io.stdout:write("listen: " .. tostring(addr) .. "\n")
end
io.stdout:flush()

local status_task = assert(host:spawn_task("example.status", function(ctx)
	while true do
		print_status(host)
		local slept, sleep_err = ctx:sleep(opts.status_interval)
		if slept == nil and sleep_err then
			return nil, sleep_err
		end
	end
end, { service = "example" }))

if opts.duration and opts.duration > 0 then
	host:sleep(opts.duration)
	host:cancel_task(status_task.id)
	host:stop()
	return
end

while true do
	host:sleep(1)
end
