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
		ttl = 7200,
		timeout = 0.25,
		retries = 6,
		duration = 30,
		listen_ipv6 = false,
		debug = false,
		discover_autonat = false,
		max_autonat_servers = 3,
		target_autonat_responses = 1,
		drain_seconds = 3,
		discovery_timeout = 90,
		seed_wait_seconds = 2,
		walk_timeout = 12,
		dht_alpha = 3,
		dht_paths = 2,
		dht_count = 20,
		stop_on_first_reachable = true,
		probe_bootstrap = false,
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
		elseif name == "--probe-bootstrap" then
			opts.probe_bootstrap = true
			i = i + 1
		else
			return nil, "unknown argument: " .. tostring(name)
		end
	end
	if not opts.gateway or opts.gateway == "" then
		return nil, "--gateway is required"
	end
	return opts
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

local bootstrap_discovery_config = nil
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

local autonat_stats = {
	checked = 0,
	responses = 0,
	refused = 0,
	reachable = 0,
}

local checked_autonat_peers = {}

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

local identify_success_count = 0

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

local h, host_err = host_mod.new({
	runtime = opts.runtime,
	listen_addrs = listen_addrs,
	services = {
		identify = { module = identify_service },
		ping = { module = ping_service },
		autonat = { module = autonat_service },
		kad_dht = { module = kad_dht_service, config = { mode = "client" } },
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

h:on("peer_connected", function(payload)
	if opts.debug then
		print("peer connected: " .. tostring(payload.peer_id))
	end
	return true
end)

h:on("peer_identified", function()
	identify_success_count = identify_success_count + 1
	return true
end)

h:on("peer_identify_failed", function(payload)
	print("peer identify failed: " .. tostring(payload.peer_id) .. " cause=" .. tostring(payload.error_message or payload.error))
	return true
end)

h:on("connection_opened", function(payload)
	if not opts.debug then
		return true
	end
	local state = payload and payload.state or {}
	local remote_addr = nil
	if payload and payload.connection and type(payload.connection.raw) == "function" then
		local raw = payload.connection:raw()
		if raw and type(raw.remote_multiaddr) == "function" then
			remote_addr = raw:remote_multiaddr()
		end
	end
	print(
		"connection opened: peer="
			.. tostring(payload and payload.peer_id)
			.. " transport="
			.. tostring(remote_addr or "unknown")
			.. " security="
			.. tostring(state.security)
			.. " muxer="
			.. tostring(state.muxer)
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

local function sleep_poll(host, seconds, ctx)
	if ctx and type(ctx.sleep) == "function" then
		return ctx:sleep(seconds)
	end
	local task, task_err = host:spawn_task("example.sleep", function(task_ctx)
		return task_ctx:sleep(seconds)
	end, { service = "example", ctx = ctx })
	if not task then
		return nil, task_err
	end
	return host:run_until_task(task, { poll_interval = 0.05, ctx = ctx })
end

local function sorted_protocol_ids(handler_map)
	local out = {}
	for protocol_id, _ in pairs(handler_map or {}) do
		out[#out + 1] = protocol_id
	end
	table.sort(out)
	return out
end

local function has_protocol(protocols, target)
	for _, protocol_id in ipairs(protocols or {}) do
		if protocol_id == target then
			return true
		end
	end
	return false
end

local function ensure_core_protocol_handlers(host)
	local protocols = sorted_protocol_ids(host and host._handlers)
	if opts.debug then
		print("protocol handlers: " .. table.concat(protocols, ","))
	end
	if not has_protocol(protocols, "/ipfs/id/1.0.0") then
		return nil, "identify protocol handler not registered: /ipfs/id/1.0.0"
	end
	if not has_protocol(protocols, "/ipfs/ping/1.0.0") then
		return nil, "ping protocol handler not registered: /ipfs/ping/1.0.0"
	end
	return true
end

local function wait_task(host, task, ctx)
	if not task then
		return nil, "task is required"
	end
	if ctx and type(ctx.await_task) == "function" then
		local _, wait_err = ctx:await_task(task)
		if task.status ~= "completed" then
			return nil, wait_err or task.error
		end
		return task.result
	end
	local ok, run_err = host:run_until_task(task, { poll_interval = 0.05 })
	if not ok then
		return nil, run_err
	end
	return task.result
end

local function autonat_check_mappings(host, server, ctx)
	local server_peer_id = type(server) == "table" and server.peer_id or server
	if checked_autonat_peers[server_peer_id] then
		return nil, "already checked AutoNAT peer"
	end
	checked_autonat_peers[server_peer_id] = true
	autonat_stats.checked = autonat_stats.checked + 1
	local mappings = host.address_manager:get_public_address_mappings()
	print(
		"autonat check starting: peer="
			.. tostring(type(server) == "table" and server.peer_id or server)
			.. " mapping_count="
			.. tostring(#mappings)
	)
	for _, addr in ipairs(mappings) do
		print("  checking mapping: " .. tostring(addr) .. " status=" .. metadata_status(host, addr))
	end
	local task, start_err = host.autonat:start_check(server, {
		addrs = mappings,
		type = "ip-mapping",
		timeout = 8,
	})
	if not task then
		return nil, start_err
	end
	local result, run_err = wait_task(host, task, ctx)
	if not result then
		return nil, run_err
	end
	local useful_response = result.response_status ~= "E_DIAL_REFUSED"
	if useful_response then
		autonat_stats.responses = autonat_stats.responses + 1
	else
		autonat_stats.refused = autonat_stats.refused + 1
	end
	if result.reachable == true then
		autonat_stats.reachable = autonat_stats.reachable + 1
	end
	print(
		"autonat check: peer="
			.. tostring(type(server) == "table" and server.peer_id or server)
			.. " reachable="
			.. tostring(result.reachable)
			.. " response_status="
			.. tostring(result.response_status)
			.. " dial_status="
			.. tostring(result.dial_status)
	)
	if result.addr then
		print("autonat checked addr: " .. tostring(result.addr) .. " status=" .. metadata_status(host, result.addr))
	end
	return result
end

local function discover_autonat_servers(host, limit, ctx, opts)
	if not host.kad_dht then
		print("autonat discovery: kad_dht service is required")
		return {}
	end
	sleep_poll(host, opts.seed_wait_seconds or 2, ctx)
	local seed_deadline = os.time() + (opts.seed_wait_seconds or 2)
	while #host.kad_dht.routing_table:all_peers() == 0 and os.time() < seed_deadline do
		sleep_poll(host, 0.25, ctx)
	end
	print("autonat discovery: routing_table_peers=" .. tostring(#host.kad_dht.routing_table:all_peers()))
	local walk_op = assert(host.kad_dht:random_walk({
		count = opts.dht_count or 20,
		alpha = opts.dht_alpha or 3,
		disjoint_paths = opts.dht_paths or 2,
	}))
	walk_op:result({ ctx = ctx, timeout = opts.walk_timeout or 12 })
	print("autonat discovery: post-walk peers=" .. tostring(#host.kad_dht.routing_table:all_peers()))
	local proto = require("lua_libp2p.protocol.autonat_v2").DIAL_REQUEST_ID
	local out = {}
	local peerstore_all = host.peerstore and host.peerstore:all() or {}
	print("autonat discovery: peerstore_peers=" .. tostring(#peerstore_all))
	local proto_candidates = 0
	for _, peer in ipairs(host.peerstore:all()) do
		if not checked_autonat_peers[peer.peer_id]
			and host.peerstore:supports_protocol(peer.peer_id, proto)
			and #host.peerstore:get_addrs(peer.peer_id) > 0
		then
			proto_candidates = proto_candidates + 1
			out[#out + 1] = {
				peer_id = peer.peer_id,
				addrs = host.peerstore:get_addrs(peer.peer_id),
			}
			if #out >= limit then
				break
			end
		end
	end
	print("autonat discovery: autonat_proto_candidates=" .. tostring(proto_candidates))
	if #out == 0 then
		local fallback_count = 0
		for _, peer in ipairs(peerstore_all) do
			local addrs = host.peerstore:get_addrs(peer.peer_id)
			if #addrs > 0 and not checked_autonat_peers[peer.peer_id] then
				fallback_count = fallback_count + 1
				out[#out + 1] = {
					peer_id = peer.peer_id,
					addrs = addrs,
					fallback = true,
				}
				if #out >= limit then
					break
				end
			end
		end
		print("autonat discovery: fallback_candidates=" .. tostring(fallback_count))
	end
	return out
end

local function check_discovered_autonat_servers(host, opts, ctx)
	local ready_deadline = os.time() + 6
	while identify_success_count == 0 and os.time() < ready_deadline do
		sleep_poll(host, 0.25, ctx)
	end
	print("autonat discovery: identified_peers=" .. tostring(identify_success_count))

	local deadline = os.time() + (opts.discovery_timeout or 90)
	while
		autonat_stats.checked < opts.max_autonat_servers
		and (autonat_stats.responses < opts.target_autonat_responses or autonat_stats.reachable == 0)
	do
		if opts.stop_on_first_reachable and autonat_stats.reachable > 0 then
			break
		end
		local discover_task = assert(host:spawn_task("example.discover_autonat", function(ctx)
			return discover_autonat_servers(host, opts.max_autonat_servers, ctx, opts)
		end, { service = "example" }))
		local servers = wait_task(host, discover_task, ctx) or {}
		if #servers == 0 then
			print("autonat discovery: no candidates found yet")
			if os.time() >= deadline then
				print("autonat discovery: timed out waiting for candidates")
				break
			end
			sleep_poll(host, 2, ctx)
			goto continue_discovery
		end
		for _, server in ipairs(servers) do
			if
				autonat_stats.checked >= opts.max_autonat_servers
				or (opts.stop_on_first_reachable and autonat_stats.reachable > 0)
			then
				break
			end
			local result = autonat_check_mappings(host, server, ctx)
			if not result then
				print("  no AutoNAT response from " .. tostring(server.peer_id) .. (server.fallback and " (fallback)" or ""))
			end
		end
		if autonat_stats.responses < opts.target_autonat_responses and host.kad_dht then
			local extra = assert(host.kad_dht:random_walk({
				count = opts.dht_count or 20,
				alpha = opts.dht_alpha or 3,
				disjoint_paths = opts.dht_paths or 2,
			}))
			extra:result({ ctx = ctx, timeout = 60 })
		end
		::continue_discovery::
		if os.time() >= deadline then
			break
		end
	end
	if opts.drain_seconds and opts.drain_seconds > 0 then
		sleep_poll(host, opts.drain_seconds, ctx)
	end
	print(
		"autonat discovery: checked="
			.. tostring(autonat_stats.checked)
			.. " responses="
			.. tostring(autonat_stats.responses)
			.. " refused="
			.. tostring(autonat_stats.refused)
			.. " reachable="
			.. tostring(autonat_stats.reachable)
	)
end

assert(h:start())

do
	local ok_handlers, handler_err = ensure_core_protocol_handlers(h)
	if not ok_handlers then
		error(handler_err)
	end
end

print("peer id: " .. tostring(h:peer_id().id))
print("listen addrs:")
for _, addr in ipairs(h:get_multiaddrs()) do
	print("  " .. tostring(addr))
end

if opts.probe_bootstrap and h.peer_discovery then
	local probe_task = assert(h:spawn_task("example.bootstrap_probe", function()
		local boot_peers, boot_err = h.peer_discovery:discover({
			dialable_only = true,
			ignore_resolve_errors = false,
			ignore_source_errors = false,
		})
		if not boot_peers then
			print("bootstrap discovery probe failed: " .. tostring(boot_err))
		else
			print("bootstrap discovery probe peers=" .. tostring(#boot_peers))
		end
		return true
	end, { service = "example" }))
	wait_task(h, probe_task)
end

if opts.autonat_server and h.autonat then
	h:spawn_task("example.autonat_direct", function(ctx)
		local _, check_err = autonat_check_mappings(h, opts.autonat_server, ctx)
		if check_err then
			print("autonat check failed: " .. tostring(check_err))
		end
		print(
			"autonat summary: checked="
				.. tostring(autonat_stats.checked)
				.. " responses="
				.. tostring(autonat_stats.responses)
				.. " refused="
				.. tostring(autonat_stats.refused)
				.. " reachable="
				.. tostring(autonat_stats.reachable)
		)
		print_pcp_mapping_status(h, "pcp mapping summary")
		return true
	end, { service = "example" })
end

if opts.discover_autonat then
	h:spawn_task("example.discover_autonat_loop", function(ctx)
		check_discovered_autonat_servers(h, opts, ctx)
		print(
			"autonat summary: checked="
				.. tostring(autonat_stats.checked)
				.. " responses="
				.. tostring(autonat_stats.responses)
				.. " refused="
				.. tostring(autonat_stats.refused)
				.. " reachable="
				.. tostring(autonat_stats.reachable)
		)
		print_pcp_mapping_status(h, "pcp mapping summary")
		return true
	end, { service = "example" })
end

local task = assert(h:spawn_task("example.sleep", function(ctx)
	return ctx:sleep(opts.duration)
end, { service = "example" }))
h:run_until_task(task, { poll_interval = 0.05 })
h:stop()
