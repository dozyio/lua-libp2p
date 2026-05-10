local ed25519 = require("lua_libp2p.crypto.ed25519")
local host_mod = require("lua_libp2p.host")
local identify_service = require("lua_libp2p.protocol_identify.service")
local kad_dht_service = require("lua_libp2p.kad_dht")
local ping_service = require("lua_libp2p.protocol_ping.service")

local function close_host(h)
  if h then
    pcall(function()
      h:close()
    end)
  end
end

local function new_client()
  local client, err = host_mod.new({
    identity = assert(ed25519.generate_keypair()),
    runtime = "luv",
    services = {
      identify = { module = identify_service },
      ping = { module = ping_service },
    },
    blocking = false,
    accept_timeout = 0.005,
    poll_interval = 0.005,
  })
  if not client then
    return nil, err
  end
  local ok, start_err = client:start()
  if not ok then
    close_host(client)
    return nil, start_err
  end
  return client
end

local function counter_summary(h)
  local counters = rawget(h, "_debug_counters") or {}
  local cm = h.connection_manager and h.connection_manager:stats() or {}
  return string.format(
    " counters=accept:%d upgrade_started:%d upgrade_completed:%d upgrade_failed:%d resource_reject:%d registration_failed:%d pump_terminal:%d pruned:%d conns:%d inbound:%d",
    tonumber(counters.inbound_accept) or 0,
    tonumber(counters.inbound_upgrade_started) or 0,
    tonumber(counters.inbound_upgrade_completed) or 0,
    tonumber(counters.inbound_upgrade_failed) or 0,
    tonumber(counters.inbound_resource_reject) or 0,
    tonumber(counters.inbound_registration_failed) or 0,
    tonumber(counters.connection_pump_terminal) or 0,
    tonumber(cm.pruned) or 0,
    tonumber(cm.connections) or 0,
    tonumber(cm.inbound_connections) or 0
  )
end

local function run(opts)
  local options = opts or {}
  local iterations = tonumber(options.iterations) or tonumber(os.getenv("LUA_LIBP2P_TEST_INBOUND_CHURN_ITERS")) or 40
  local progress_interval = tonumber(options.progress_interval)
    or tonumber(os.getenv("LUA_LIBP2P_TEST_INBOUND_CHURN_PROGRESS"))
    or 0
  local server, server_err = host_mod.new({
    identity = assert(ed25519.generate_keypair()),
    runtime = "luv",
    listen_addrs = { "/ip4/127.0.0.1/tcp/0" },
    announce_addrs = { "/ip4/203.0.113.20/tcp/4001" },
    services = {
      identify = { module = identify_service },
      ping = { module = ping_service },
      kad_dht = {
        module = kad_dht_service,
        config = { mode = "server" },
      },
    },
    connection_manager_options = {
      low_water = 16,
      high_water = 32,
      grace_period = 1,
      silence_period = 1,
      max_inbound_connections = 48,
      max_outbound_connections = 48,
    },
    blocking = false,
    accept_timeout = 0.005,
    poll_interval = 0.005,
  })
  if not server then
    return nil, server_err
  end
  local ok, err = server:start()
  if not ok then
    close_host(server)
    return nil, err
  end

  local listen_addr = server:get_listen_addrs()[1]
  if type(listen_addr) ~= "string" then
    close_host(server)
    return nil, "expected server listen addr"
  end
  local target = listen_addr .. "/p2p/" .. server:peer_id().id

  for i = 1, iterations do
    if progress_interval > 0 and i > 1 and ((i - 1) % progress_interval) == 0 then
      io.stdout:write("  inbound_churn progress iteration=" .. tostring(i - 1) .. counter_summary(server) .. "\n")
      io.stdout:flush()
    end
    if i % 10 == 0 then
      server.address_manager:set_announce_addrs({ string.format("/ip4/203.0.113.%d/tcp/4001", 20 + (i % 200)) })
      ok, err = server:_emit_self_peer_update_if_changed()
      if not ok then
        close_host(server)
        return nil, err
      end
    end

    local client, client_err = new_client()
    if not client then
      close_host(server)
      return nil, client_err
    end

    local id_result, id_err = client.services.identify:identify(target, {
      timeout = 3,
      poll_interval = 0.001,
      force = true,
    })
    if not id_result then
      close_host(client)
      close_host(server)
      return nil, "identify failed at iteration " .. tostring(i) .. ": " .. tostring(id_err) .. counter_summary(server)
    end

    local ping_result, ping_err = client.services.ping:ping(target, {
      timeout = 3,
      poll_interval = 0.001,
      force = true,
    })
    if not ping_result then
      close_host(client)
      close_host(server)
      return nil, "ping failed at iteration " .. tostring(i) .. ": " .. tostring(ping_err) .. counter_summary(server)
    end

    close_host(client)
    for _ = 1, 5 do
      ok, err = server:_poll_once(0)
      if not ok then
        close_host(server)
        return nil, err
      end
    end
  end

  if progress_interval > 0 then
    io.stdout:write("  inbound_churn progress iteration=" .. tostring(iterations) .. counter_summary(server) .. "\n")
    io.stdout:flush()
  end

  local counters = rawget(server, "_debug_counters") or {}
  if (tonumber(counters.inbound_resource_reject) or 0) ~= 0 then
    close_host(server)
    return nil, "unexpected inbound resource rejects: " .. tostring(counters.inbound_resource_reject)
  end
  if (tonumber(counters.inbound_registration_failed) or 0) ~= 0 then
    close_host(server)
    return nil, "unexpected inbound registration failures: " .. tostring(counters.inbound_registration_failed)
  end
  if (tonumber(counters.inbound_upgrade_failed) or 0) ~= 0 then
    close_host(server)
    return nil, "unexpected inbound upgrade failures: " .. tostring(counters.inbound_upgrade_failed)
  end

  close_host(server)
  return true
end

return {
  name = "host inbound churn stability",
  run = run,
}
