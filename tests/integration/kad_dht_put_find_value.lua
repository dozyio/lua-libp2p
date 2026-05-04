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
  local writer, writer_addr_or_err = new_host("client")
  if not writer then
    stop_all({ server })
    return nil, writer_addr_or_err
  end
  local reader, reader_addr_or_err = new_host("client")
  if not reader then
    stop_all({ writer, server })
    return nil, reader_addr_or_err
  end

  local server_target = {
    peer_id = server:peer_id().id,
    addrs = { server_addr_or_err },
  }
  local key = "integration-record-key"
  local value = "integration-record-value"

  local put_op, put_err = writer.kad_dht:put_value(key, value, {
    peers = { server_target },
    count = 1,
  })
  if not put_op then
    stop_all({ reader, writer, server })
    return nil, put_err
  end
  local put_report, put_result_err = put_op:result({ timeout = 6, poll_interval = 0.01 })
  if not put_report then
    stop_all({ reader, writer, server })
    return nil, put_result_err
  end
  if put_report.succeeded ~= 1 or put_report.failed ~= 0 then
    stop_all({ reader, writer, server })
    return nil, "expected value publication to succeed"
  end

  local find_op, find_err = reader.kad_dht:find_value(key, {
    peers = { server_target },
  })
  if not find_op then
    stop_all({ reader, writer, server })
    return nil, find_err
  end
  local found, found_err = find_op:result({ timeout = 6, poll_interval = 0.01 })
  stop_all({ reader, writer, server })
  if not found then
    return nil, found_err
  end
  if not found.record or found.record.value ~= value then
    return nil, "expected to find published value"
  end

  return true
end

return {
  name = "kad-dht put/find value over local hosts",
  run = run,
}
