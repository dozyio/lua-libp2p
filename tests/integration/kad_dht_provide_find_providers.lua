local host_mod = require("lua_libp2p.host")
local identify_service = require("lua_libp2p.protocol_identify.service")
local kad_dht_service = require("lua_libp2p.kad_dht")
local ping_service = require("lua_libp2p.protocol_ping.service")
local tcp_luv = require("lua_libp2p.transport_tcp.luv")

local function new_host(mode)
  local host, host_err = host_mod.new({
    runtime = "luv",
    blocking = false,
    listen_addrs = { "/ip4/127.0.0.1/tcp/0" },
    services = {
      identify = { module = identify_service },
      ping = { module = ping_service },
      kad_dht = {
        module = kad_dht_service,
        config = {
          mode = mode or "client",
          address_filter = "all",
        },
      },
    },
    accept_timeout = 0.05,
    io_timeout = 2,
  })
  if not host then
    return nil, host_err
  end
  local started, start_err = host:start()
  if not started then
    host:stop()
    return nil, start_err
  end
  local addrs = host:get_multiaddrs()
  if #addrs == 0 then
    host:stop()
    return nil, "expected listening multiaddr"
  end
  return host, addrs[1]
end

local function stop_all(hosts)
  for _, host in ipairs(hosts) do
    if host then
      pcall(function()
        host:stop()
      end)
    end
  end
end

local function run()
  local has_luv = pcall(require, "luv")
  if not has_luv or tcp_luv.BACKEND ~= "luv-native" then
    return true
  end

  local server, server_addr_or_err = new_host("server")
  if not server then
    return nil, server_addr_or_err
  end
  local provider, provider_addr_or_err = new_host("client")
  if not provider then
    stop_all({ server })
    return nil, provider_addr_or_err
  end
  local client, client_addr_or_err = new_host("client")
  if not client then
    stop_all({ provider, server })
    return nil, client_addr_or_err
  end

  local server_peer = server:peer_id().id
  local content_key = "integration-provider-key"
  local server_target = {
    peer_id = server_peer,
    addrs = { server_addr_or_err },
  }

  local provide_op, provide_err = provider.kad_dht:provide(content_key, {
    peers = { server_target },
    count = 1,
  })
  if not provide_op then
    stop_all({ client, provider, server })
    return nil, provide_err
  end
  local provide_report, provide_result_err = provide_op:result({ timeout = 6, poll_interval = 0.01 })
  if not provide_report then
    stop_all({ client, provider, server })
    return nil, provide_result_err
  end
  if provide_report.succeeded ~= 1 or provide_report.failed ~= 0 then
    stop_all({ client, provider, server })
    return nil, "expected provider announce to succeed"
  end

  local find_op, find_err = client.kad_dht:find_providers(content_key, {
    peers = { server_target },
    limit = 1,
  })
  if not find_op then
    stop_all({ client, provider, server })
    return nil, find_err
  end
  local found, found_err = find_op:result({ timeout = 6, poll_interval = 0.01 })
  stop_all({ client, provider, server })
  if not found then
    return nil, found_err
  end
  if #found.providers ~= 1 then
    return nil, "expected to find one provider"
  end
  if found.providers[1].peer_id ~= provider:peer_id().id then
    return nil, "found provider peer id mismatch"
  end
  if type(found.providers[1].addrs) ~= "table" or #found.providers[1].addrs == 0 then
    return nil, "found provider should include addresses"
  end

  return true
end

return {
  name = "kad-dht provide/find providers over local hosts",
  run = run,
}
