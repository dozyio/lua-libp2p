local mdns = require("lua_libp2p.peer_discovery_mdns")
local dns = require("lua_libp2p.peer_discovery_mdns.dns")
local host_mod = require("lua_libp2p.host")

local PEER_A = "QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"
local PEER_B = "QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN"

local function assert_true(value, message)
  if not value then
    return nil, message
  end
  return true
end

local function run()
  local query, query_err = mdns.build_query()
  if not query then
    return nil, query_err
  end
  local decoded_query, decode_query_err = dns.decode_message(query)
  if not decoded_query then
    return nil, decode_query_err
  end
  if #decoded_query.questions ~= 1 or decoded_query.questions[1].name ~= mdns.SERVICE_NAME then
    return nil, "expected mdns PTR query question"
  end

  local addr_b = "/ip4/192.168.1.20/tcp/4001/p2p/" .. PEER_B
  local response, response_err = mdns.build_response("peerb", mdns.SERVICE_NAME, { addr_b })
  if not response then
    return nil, response_err
  end
  local decoded_response, decode_response_err = dns.decode_message(response)
  if not decoded_response then
    return nil, decode_response_err
  end
  local peers = mdns.parse_peers(decoded_response, { local_peer_id = PEER_A })
  if #peers ~= 1 or peers[1].peer_id ~= PEER_B or peers[1].addrs[1] ~= addr_b then
    return nil, "expected mdns response to parse discovered peer"
  end
  local self_peers = mdns.parse_peers(decoded_response, { local_peer_id = PEER_B })
  if #self_peers ~= 0 then
    return nil, "expected mdns parser to ignore local peer id"
  end
  local addr_b6 = "/ip6/fd00::20/tcp/4001/p2p/" .. PEER_B
  local response6, response6_err = mdns.build_response("peerb6", mdns.SERVICE_NAME, { addr_b6 })
  if not response6 then
    return nil, response6_err
  end
  local decoded_response6, decode_response6_err = dns.decode_message(response6)
  if not decoded_response6 then
    return nil, decode_response6_err
  end
  local found_aaaa = false
  for _, record in ipairs(decoded_response6.additionals) do
    if record.type == dns.TYPE_AAAA and #record.rdata == 16 then
      found_aaaa = true
    end
  end
  if not found_aaaa then
    return nil, "expected ipv6 mdns response to include AAAA additional"
  end
  local peers6 = mdns.parse_peers(decoded_response6, { local_peer_id = PEER_A })
  if #peers6 ~= 1 or peers6[1].peer_id ~= PEER_B or peers6[1].addrs[1] ~= addr_b6 then
    return nil, "expected ipv6 mdns response to parse discovered peer"
  end

  local ok, err = assert_true(mdns.is_suitable_addr(addr_b), "expected private tcp addr to be suitable")
  if not ok then
    return nil, err
  end
  ok, err = assert_true(
    not mdns.is_suitable_addr("/ip4/192.168.1.20/tcp/4001/p2p/" .. PEER_B .. "/p2p-circuit"),
    "expected relay addr to be unsuitable"
  )
  if not ok then
    return nil, err
  end
  ok, err = assert_true(
    not mdns.is_suitable_addr("/dns/example.com/tcp/4001/p2p/" .. PEER_B),
    "expected non-local dns addr to be unsuitable"
  )
  if not ok then
    return nil, err
  end

  local sent = {}
  local host = {
    _running = true,
    peerstore = {
      merged = nil,
      merge = function(self, peer_id, data, opts)
        self.merged = { peer_id = peer_id, data = data, opts = opts }
        return true
      end,
    },
    emitted = nil,
    peer_id = function()
      return { id = PEER_A }
    end,
    get_multiaddrs = function()
      return { "/ip4/192.168.1.10/tcp/4001/p2p/" .. PEER_A }
    end,
    emit = function(self, name, payload)
      self.emitted = { name = name, payload = payload }
      return true
    end,
  }
  local service, service_err = mdns.new(host, {
    peer_name = "peera",
    interval = 0,
    socket_factory = function()
      return {
        send = function(_, packet)
          sent[#sent + 1] = packet
          return true
        end,
        close = function()
          return true
        end,
      }
    end,
  })
  if not service then
    return nil, service_err
  end
  local started, start_err = service:start()
  if not started then
    return nil, start_err
  end
  if #sent ~= 1 then
    return nil, "expected mdns start to send initial query"
  end

  local query_msg, query_msg_err = dns.decode_message(sent[1])
  if not query_msg then
    return nil, query_msg_err
  end
  if query_msg.questions[1].name ~= mdns.SERVICE_NAME then
    return nil, "expected sent packet to be mdns query"
  end

  service:_handle_packet(response)
  if not host.peerstore.merged or host.peerstore.merged.peer_id ~= PEER_B then
    return nil, "expected discovered peer to be merged into peerstore"
  end
  if not host.emitted or host.emitted.name ~= "peer_discovered" then
    return nil, "expected discovered peer event"
  end
  local cached = service:discover()
  if #cached ~= 1 or cached[1].peer_id ~= PEER_B then
    return nil, "expected mdns discover cache to include discovered peer"
  end

  local own_query = mdns.build_query()
  local before = #sent
  service:_handle_packet(own_query)
  if #sent ~= before + 1 then
    return nil, "expected service to respond to its own query"
  end
  local own_response, own_response_err = dns.decode_message(sent[#sent])
  if not own_response then
    return nil, own_response_err
  end
  if #own_response.answers ~= 1 or #own_response.additionals < 2 then
    return nil, "expected mdns response with PTR and DNS-SD additionals"
  end
  local found_txt = false
  for _, record in ipairs(own_response.additionals) do
    if record.type == dns.TYPE_TXT then
      found_txt = true
    end
  end
  if not found_txt then
    return nil, "expected mdns response with dnsaddr TXT"
  end

  service:stop()

  local lifecycle = { sent = 0, closed = 0 }
  local managed_host, managed_host_err = host_mod.new({
    runtime = "luv",
    blocking = false,
    listen_addrs = { "/ip4/127.0.0.1/tcp/0" },
    services = {
      mdns = {
        module = mdns,
        config = {
          peer_name = "managed",
          interval = 0,
          socket_factory = function()
            return {
              send = function(_, packet)
                local msg, msg_err = dns.decode_message(packet)
                if not msg then
                  error(tostring(msg_err))
                end
                lifecycle.sent = lifecycle.sent + 1
                return true
              end,
              close = function()
                lifecycle.closed = lifecycle.closed + 1
                return true
              end,
            }
          end,
        },
      },
    },
  })
  if not managed_host then
    return nil, managed_host_err
  end
  if lifecycle.sent ~= 0 then
    return nil, "expected mdns service to defer socket start until host starts"
  end
  local managed_started, managed_start_err = managed_host:start()
  if not managed_started then
    return nil, managed_start_err
  end
  if lifecycle.sent ~= 1 then
    managed_host:stop()
    return nil, "expected host start to open mdns socket and send query"
  end
  local managed_stopped, managed_stop_err = managed_host:stop()
  if not managed_stopped then
    return nil, managed_stop_err
  end
  if lifecycle.closed ~= 1 then
    return nil, "expected host stop to close mdns socket"
  end

  return true
end

return {
  name = "mdns discovery packet handling",
  run = run,
}
