local ed25519 = require("lua_libp2p.crypto.ed25519")
local host_mod = require("lua_libp2p.host")
local multiaddr = require("lua_libp2p.multiaddr")
local ping_service = require("lua_libp2p.protocol_ping.service")

local function close_host(h)
  if h then
    pcall(function()
      h:close()
    end)
  end
end

local function extract_tcp_port(addrs)
  for _, addr in ipairs(addrs or {}) do
    local parsed = multiaddr.parse(addr)
    local endpoint = parsed and multiaddr.to_tcp_endpoint(parsed) or nil
    if endpoint and endpoint.port and endpoint.port > 0 then
      return endpoint.port
    end
  end
  return nil
end

local function run()
  local iterations = tonumber(os.getenv("LUA_LIBP2P_TEST_DUAL_STACK_ITERS")) or 20
  local server, server_err = host_mod.new({
    identity = assert(ed25519.generate_keypair()),
    runtime = "luv",
    listen_addrs = {
      "/ip6/::/tcp/0",
      "/ip4/0.0.0.0/tcp/0",
    },
    services = {
      ping = { module = ping_service },
    },
    resource_manager_options = {
      connections_per_peer = 128,
      connections_inbound_per_peer = 128,
      connections_outbound_per_peer = 128,
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
    if tostring(err):find("ipv6", 1, true) then
      return true
    end
    return nil, err
  end

  local listen_addrs = server:get_listen_addrs()
  local port = extract_tcp_port(listen_addrs)
  if not port then
    close_host(server)
    return nil, "expected concrete shared tcp port"
  end

  local expected_v4 = "/ip4/0.0.0.0/tcp/" .. tostring(port)
  local expected_v6 = "/ip6/::/tcp/" .. tostring(port)
  local have_v4 = false
  local have_v6 = false
  for _, addr in ipairs(listen_addrs) do
    if addr == expected_v4 then
      have_v4 = true
    elseif addr == expected_v6 then
      have_v6 = true
    end
  end
  if not have_v4 or not have_v6 then
    close_host(server)
    return nil, "expected IPv4 and IPv6 listen addrs on same port"
  end

  local client, client_err = host_mod.new({
    identity = assert(ed25519.generate_keypair()),
    runtime = "luv",
    services = {
      ping = { module = ping_service },
    },
    resource_manager_options = {
      connections_per_peer = 128,
      connections_inbound_per_peer = 128,
      connections_outbound_per_peer = 128,
    },
    blocking = false,
    accept_timeout = 0.005,
    poll_interval = 0.005,
  })
  if not client then
    close_host(server)
    return nil, client_err
  end
  ok, err = client:start()
  if not ok then
    close_host(client)
    close_host(server)
    return nil, err
  end

  local peer = server:peer_id().id
  local targets = {
    "/ip4/127.0.0.1/tcp/" .. tostring(port) .. "/p2p/" .. peer,
    "/ip6/::1/tcp/" .. tostring(port) .. "/p2p/" .. peer,
  }
  for i = 1, iterations do
    local target = targets[((i - 1) % #targets) + 1]
    local result, ping_err = client.services.ping:ping(target, {
      timeout = 3,
      poll_interval = 0.001,
      force = true,
    })
    if not result then
      close_host(client)
      close_host(server)
      return nil, "dual-stack ping failed for " .. target .. ": " .. tostring(ping_err)
    end
  end

  local counters = rawget(server, "_debug_counters") or {}
  if
    (tonumber(counters.inbound_resource_reject) or 0) ~= 0
    or (tonumber(counters.inbound_registration_failed) or 0) ~= 0
    or (tonumber(counters.inbound_upgrade_failed) or 0) ~= 0
  then
    close_host(client)
    close_host(server)
    return nil, "unexpected inbound failure counter during dual-stack test"
  end

  close_host(client)
  close_host(server)
  return true
end

return {
  name = "host dual-stack same-port listener stability",
  run = run,
}
