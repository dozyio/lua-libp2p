package.path = table.concat({
	"./?.lua",
	"./?/init.lua",
	package.path,
}, ";")

local host_mod = require("lua_libp2p.host")
local ed25519 = require("lua_libp2p.crypto.ed25519")
local identify_service = require("lua_libp2p.protocol_identify.service")
local ping_service = require("lua_libp2p.protocol_ping.service")
local autonat_service = require("lua_libp2p.autonat.client")
local kad_dht_service = require("lua_libp2p.kad_dht")
local peer_discovery_bootstrap = require("lua_libp2p.peer_discovery_bootstrap")
local pcp_service = require("lua_libp2p.pcp.service")
local log = require("lua_libp2p.log")

local function usage()
	io.stderr:write([[
Usage:
  lua examples/pcp_demo.lua --gateway <gateway-ip> [--internal-client <lan-ip>] [--port 4001]
  lua examples/pcp_demo.lua --gateway <gateway-ip> --listen-ipv6 [--port 4001]
  lua examples/pcp_demo.lua --runtime luv --gateway <gateway-ip> [--listen-ipv6] [--debug]
  lua examples/pcp_demo.lua --gateway <gateway-ip> --internal-client <ip> --autonat-server <peer-id-or-multiaddr>
  lua examples/pcp_demo.lua --gateway <gateway-ip> --discover-autonat [--bootstrap <multiaddr>]
]])
end

local function parse_args(args)
	local opts = {
		port = 4001,
		runtime = "auto",
		identity_key_path = ".lua-libp2p-pcp.key",
		ttl = 7200,
		timeout = 0.25,
		retries = 6,
		duration = 30,
		listen_ipv6 = false,
		debug = false,
		discover_autonat = false,
		max_autonat_servers = nil,
		target_autonat_responses = nil,
		drain_seconds = 3,
		discovery_timeout = 90,
		seed_wait_seconds = 2,
		walk_timeout = 12,
		dht_alpha = 3,
		dht_paths = 2,
		dht_count = 20,
		stop_on_first_reachable = true,
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
		elseif name == "--port" then
			opts.port = tonumber(value) or opts.port
			i = i + 2
		elseif name == "--duration" then
			opts.duration = tonumber(value) or opts.duration
			i = i + 2
		elseif name == "--runtime" then
			opts.runtime = value or opts.runtime
			i = i + 2
		elseif name == "--identity-key" then
			if not value then
				return nil, "--identity-key requires a file path"
			end
			opts.identity_key_path = value
			i = i + 2
		elseif name == "--listen-ipv6" then
			opts.listen_ipv6 = true
			i = i + 1
		elseif name == "--debug" then
			opts.debug = true
			i = i + 1
		elseif name == "--autonat-server" then
			opts.autonat_server = value
			i = i + 2
		elseif name == "--discover-autonat" then
			opts.discover_autonat = true
			i = i + 1
		elseif name == "--bootstrap" then
			opts.bootstrap = opts.bootstrap or {}
			opts.bootstrap[#opts.bootstrap + 1] = value
			i = i + 2
		elseif name == "--max-autonat-servers" then
			opts.max_autonat_servers = tonumber(value) or opts.max_autonat_servers
			i = i + 2
		elseif name == "--target-autonat-responses" then
			opts.target_autonat_responses = tonumber(value) or opts.target_autonat_responses
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
		else
			return nil, "unknown argument: " .. tostring(name)
		end
	end
	if not opts.gateway or opts.gateway == "" then
		return nil, "--gateway is required"
	end
	return opts
end

local function load_or_create_identity(path)
	local loaded = ed25519.load_private_key(path)
	if loaded then
		return loaded
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

local opts, err = parse_args(arg)
if not opts then
	io.stderr:write(tostring(err) .. "\n")
	usage()
	os.exit(2)
end

if opts.debug then
	log.set_level("debug")
end

local bootstrap_discovery_config
if opts.bootstrap and #opts.bootstrap > 0 then
	bootstrap_discovery_config = {
		list = opts.bootstrap,
		dialable_only = true,
		ignore_resolve_errors = true,
	}
else
	bootstrap_discovery_config = {
		ignore_resolve_errors = true,
	}
end

local function metadata_status(host, addr)
	local metadata = host
		and host.address_manager
		and host.address_manager._reachability
		and host.address_manager._reachability[addr]
	if not metadata then
		return "missing"
	end
	return tostring(metadata.status or "unknown")
		.. ",verified="
		.. tostring(metadata.verified == true)
		.. ",source="
		.. tostring(metadata.source)
end

local function print_pcp_mapping_status(host, prefix)
	local mappings = host.address_manager and host.address_manager:get_public_address_mappings() or {}
	print(tostring(prefix or "pcp mappings") .. ": count=" .. tostring(#mappings))
	for _, addr in ipairs(mappings) do
		print("  " .. tostring(addr) .. " status=" .. metadata_status(host, addr))
	end
end

local function is_ipv6(ip)
	return type(ip) == "string" and ip:find(":", 1, true) ~= nil
end

local listen_addrs
if opts.listen_ipv6 and is_ipv6(opts.internal_client) then
	listen_addrs = {
		"/ip4/0.0.0.0/tcp/" .. tostring(opts.port),
		"/ip6/" .. tostring(opts.internal_client) .. "/tcp/" .. tostring(opts.port),
	}
elseif opts.listen_ipv6 then
	listen_addrs = {
		"/ip4/0.0.0.0/tcp/" .. tostring(opts.port),
		"/ip6/::/tcp/" .. tostring(opts.port),
	}
else
	listen_addrs = {
		"/ip4/0.0.0.0/tcp/" .. tostring(opts.port),
	}
end

local identity, identity_err = load_or_create_identity(opts.identity_key_path)
if not identity then
	io.stderr:write("identity key failed: " .. tostring(identity_err) .. "\n")
	os.exit(1)
end

local h, host_err = host_mod.new({
	runtime = opts.runtime,
	identity = identity,
	listen_addrs = listen_addrs,
	services = {
		identify = { module = identify_service },
		ping = { module = ping_service },
		autonat = {
			module = autonat_service,
			config = {
				monitor_on_start = opts.discover_autonat,
				monitor_start_opts = {
					poll_interval = 0.05,
					discovery_timeout = opts.discovery_timeout,
					seed_wait_seconds = opts.seed_wait_seconds,
					initial_delay_seconds = opts.seed_wait_seconds,
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
					required_addr_proto = opts.listen_ipv6 and "ip6" or "ip4",
				},
			},
		},
		kad_dht = { module = kad_dht_service, config = { mode = "auto" } },
		pcp = {
			module = pcp_service,
			config = {
				gateway = opts.gateway,
				internal_client = opts.internal_client,
				ttl = opts.ttl,
				timeout = opts.timeout,
				retries = opts.retries,
			},
		},
	},
	peer_discovery = {
		bootstrap = {
			module = peer_discovery_bootstrap,
			config = bootstrap_discovery_config,
		},
	},
	blocking = false,
	accept_timeout = 0.05,
	connect_timeout = 6,
	io_timeout = 10,
})
if not h then
	io.stderr:write("host init failed: " .. tostring(host_err) .. "\n")
	os.exit(1)
end

if opts.discover_autonat and h.autonat and h.autonat._monitor_start_opts then
	h.autonat._monitor_start_opts.check_opts_builder = function()
		local mappings = h.address_manager:get_public_address_mappings()
		for _, addr in ipairs(mappings) do
			print("  checking mapping: " .. tostring(addr) .. " status=" .. metadata_status(h, addr))
		end
		return {
			addrs = mappings,
			type = "ip-mapping",
			timeout = 8,
		}
	end
end

local print_autonat_summary

h:on("pcp:mapping:active", function(mapping)
	print("pcp mapping active: " .. tostring(mapping.external_addr))
	return true
end)
h:on("pcp:mapping:failed", function(payload)
	print("pcp mapping failed: " .. tostring(payload.error_message or payload.error))
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

h:on("autonat:discovery:candidates", function(payload)
	print(
		"autonat discovery: peerstore_peers="
			.. tostring(payload and payload.peerstore_peers or 0)
			.. " autonat_proto_candidates="
			.. tostring(payload and payload.protocol_candidates or 0)
	)
	return true
end)

h:on("autonat:discovery:walk", function(payload)
	local report = payload and payload.report or {}
	print(
		"autonat discovery: random_walk queried="
			.. tostring(report.queried or 0)
			.. " responses="
			.. tostring(report.responses or 0)
			.. " added="
			.. tostring(report.added or 0)
			.. " discovered="
			.. tostring(report.discovered or 0)
	)
	if payload and payload.routing_table_peers then
		print("autonat discovery: post-walk peers=" .. tostring(payload.routing_table_peers))
	end
	return true
end)

h:on("autonat:discovery:progress", function(payload)
	print(
		"autonat discovery: checked="
			.. tostring(payload and payload.checked or 0)
			.. " responses="
			.. tostring(payload and payload.responses or 0)
			.. " refused="
			.. tostring(payload and payload.refused or 0)
			.. " reachable="
			.. tostring(payload and payload.reachable or 0)
	)
	return true
end)

h:on("autonat:monitor:verified", function(payload)
	local stats = payload and payload.stats or {}
	print("autonat monitor verified: round=" .. tostring(payload and payload.round or 0))
	print_autonat_summary(stats)
	return true
end)

h:on("peer_identify_failed", function(payload)
	print(
		"peer identify failed: "
			.. tostring(payload.peer_id)
			.. " cause="
			.. tostring(payload.error_message or payload.error)
	)
	return true
end)

h:on("connection_closed", function(payload)
	if not opts.debug then
		return true
	end
	local state = payload and payload.state or {}
	print(
		"connection closed: peer="
			.. tostring(payload and payload.peer_id)
			.. " security="
			.. tostring(state.security)
			.. " muxer="
			.. tostring(state.muxer)
			.. " cause="
			.. tostring(payload and payload.cause)
	)
	return true
end)

function print_autonat_summary(stats)
	print(
		"autonat summary: checked="
			.. tostring(stats and stats.checked or 0)
			.. " responses="
			.. tostring(stats and stats.responses or 0)
			.. " refused="
			.. tostring(stats and stats.refused or 0)
			.. " reachable="
			.. tostring(stats and stats.reachable or 0)
	)
end

assert(h:start())

print("peer id: " .. tostring(h:peer_id().id))
print("listen addrs:")
for _, addr in ipairs(h:get_multiaddrs()) do
	print("  " .. tostring(addr))
end

if opts.autonat_server and h.autonat then
	h:spawn_task("example.autonat_direct", function(ctx)
		local mappings = h.address_manager:get_public_address_mappings()
		local task, check_err = h.autonat:start_check(opts.autonat_server, {
			addrs = mappings,
			type = "ip-mapping",
			timeout = 8,
			stream_opts = { ctx = ctx },
		})
		if not task then
			print("autonat check failed: " .. tostring(check_err))
			return true
		end
		local result, run_err = h:wait_task(task, { poll_interval = 0.05, ctx = ctx })
		if not result then
			print("autonat check failed: " .. tostring(run_err))
			return true
		end
		if result.addr then
			print("autonat checked addr: " .. tostring(result.addr) .. " status=" .. metadata_status(h, result.addr))
		end
		local summary = {
			checked = 1,
			responses = result.response_status == "E_DIAL_REFUSED" and 0 or 1,
			refused = result.response_status == "E_DIAL_REFUSED" and 1 or 0,
			reachable = result.reachable and 1 or 0,
		}
		print_autonat_summary(summary)
		print_pcp_mapping_status(h, "pcp mapping summary")
		return true
	end, { service = "example" })
end

h:sleep(opts.duration, { poll_interval = 0.05 })

print("listen addrs before stop:")
for _, addr in ipairs(h:get_multiaddrs()) do
	print("  " .. tostring(addr))
end

h:stop()
