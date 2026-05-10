local ed25519 = require("lua_libp2p.crypto.ed25519")
local host_mod = require("lua_libp2p.host")
local ping_service = require("lua_libp2p.protocol_ping.service")

local function close_hosts(a, b)
  if a then
    pcall(function()
      a:close()
    end)
  end
  if b then
    pcall(function()
      b:close()
    end)
  end
end

local function run()
  local server, server_err = host_mod.new({
    identity = assert(ed25519.generate_keypair()),
    runtime = "luv",
    listen_addrs = { "/ip4/127.0.0.1/tcp/0" },
    announce_addrs = { "/ip4/203.0.113.10/tcp/4001" },
    services = {
      ping = { module = ping_service },
    },
    blocking = false,
    accept_timeout = 0.01,
    poll_interval = 0.01,
  })
  if not server then
    return nil, server_err
  end

  local client, client_err = host_mod.new({
    identity = assert(ed25519.generate_keypair()),
    runtime = "luv",
    services = {
      ping = { module = ping_service },
    },
    blocking = false,
    accept_timeout = 0.01,
    poll_interval = 0.01,
  })
  if not client then
    close_hosts(server)
    return nil, client_err
  end

  local ok, err = server:start()
  if not ok then
    close_hosts(server, client)
    return nil, err
  end
  ok, err = client:start()
  if not ok then
    close_hosts(server, client)
    return nil, err
  end

  local target = server:get_listen_addrs()[1]
  if type(target) ~= "string" then
    close_hosts(server, client)
    return nil, "expected server listen addr"
  end
  target = target .. "/p2p/" .. server:peer_id().id

  local first_listener = server._listeners and server._listeners[1]
  if not first_listener then
    close_hosts(server, client)
    return nil, "expected server listener"
  end

  for i = 1, 20 do
    server.address_manager:set_announce_addrs({ string.format("/ip4/203.0.113.%d/tcp/4001", 10 + i) })
    ok, err = server:_emit_self_peer_update_if_changed()
    if not ok then
      close_hosts(server, client)
      return nil, err
    end
    if server._listeners[1] ~= first_listener then
      close_hosts(server, client)
      return nil, "listener changed after advertised address update"
    end
    local result, ping_err = client.services.ping:ping(target, { timeout = 2, poll_interval = 0.001 })
    if not result then
      close_hosts(server, client)
      return nil, "ping failed after advertised address update: " .. tostring(ping_err)
    end
  end

  close_hosts(server, client)
  return true
end

return {
  name = "host advertised address updates keep listeners reachable",
  run = run,
}
