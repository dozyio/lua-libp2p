package.path = table.concat({
	"./?.lua",
	"./?/init.lua",
	package.path,
}, ";")

local host_mod = require("lua_libp2p.host")
local ed25519 = require("lua_libp2p.crypto.ed25519")

local listen_addr = arg[1] or "/ip4/127.0.0.1/tcp/64333"
local key_path = arg[2] or "examples/.identify_ping_server.ed25519.key"

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

	local keypair, gen_err = ed25519.generate_keypair()
	if not keypair then
		return nil, gen_err
	end

	local ok, save_err = ed25519.save_private_key(path, keypair)
	if not ok then
		return nil, save_err
	end

	return keypair
end

local identity, identity_err = load_or_create_identity(key_path)
if not identity then
	io.stderr:write("identity init failed: " .. tostring(identity_err) .. "\n")
	os.exit(1)
end

local function on_started(h)
	io.stdout:write("listening:\n")
	for _, addr in ipairs(h:get_multiaddrs()) do
		io.stdout:write("  " .. addr .. "\n")
	end
	io.stdout:write("identity key: " .. key_path .. "\n")
	io.stdout:write("peer id: " .. h:peer_id().id .. "\n")
	io.stdout:write("running; Ctrl-C to stop\n")
	io.stdout:flush()
end

local host, host_err = host_mod.new({
	identity = identity,
	listen_addrs = { listen_addr },
	services = { "identify", "ping", "perf" },
	blocking = true,
	poll_interval = 0.01,
	accept_timeout = 0.05,
	on_started = on_started,
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
