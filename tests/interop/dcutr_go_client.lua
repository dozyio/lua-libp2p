package.path = table.concat({
  "./?.lua",
  "./?/init.lua",
  package.path,
}, ";")

local ed25519 = require("lua_libp2p.crypto.ed25519")
local host_mod = require("lua_libp2p.host")
local identify_service = require("lua_libp2p.protocol_identify.service")
local dcutr_service = require("lua_libp2p.protocol_dcutr.service")

local target_addr = arg[1]
if not target_addr or target_addr == "" then
  io.stderr:write("usage: lua tests/interop/dcutr_go_client.lua <multiaddr-with-p2p>\n")
  os.exit(2)
end

local runtime = os.getenv("LUA_LIBP2P_INTEROP_RUNTIME") or "poll"
if runtime ~= "poll" and runtime ~= "luv" then
  io.stderr:write("invalid LUA_LIBP2P_INTEROP_RUNTIME, expected 'luv' or 'poll'\n")
  os.exit(2)
end

local identity = assert(ed25519.generate_keypair())
local host, host_err = host_mod.new({
  runtime = runtime,
  identity = identity,
  blocking = false,
  services = {
    identify = identify_service,
    dcutr = { module = dcutr_service, config = { auto_on_relay_connection = false } },
  },
})
if not host then
  io.stderr:write(tostring(host_err) .. "\n")
  os.exit(1)
end

local started, start_err = host:start()
if not started then
  io.stderr:write(tostring(start_err) .. "\n")
  os.exit(1)
end

local original_attempt = host.dcutr._attempt_direct_dials
host.dcutr._attempt_direct_dials = function(_, peer_id, addrs)
  return {
    connection = {},
    state = { remote_peer_id = peer_id },
    addr = addrs and addrs[1] or nil,
  }
end

local task, task_err = host.dcutr:start_hole_punch(target_addr, {
  max_attempts = 1,
  timeout = 6,
  dial_timeout = 2,
})
if not task then
  host:stop()
  io.stderr:write(tostring(task_err) .. "\n")
  os.exit(1)
end

local result, run_err = host:run_until_task(task, { poll_interval = 0.01, timeout = 8 })
host.dcutr._attempt_direct_dials = original_attempt
host:stop()

if not result then
  io.stderr:write(tostring(run_err) .. "\n")
  os.exit(1)
end

print("ok")
