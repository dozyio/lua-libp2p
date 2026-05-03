local address_manager = require("lua_libp2p.address_manager")
local relay_client = require("lua_libp2p.transport_circuit_relay_v2.client")
local relay_proto = require("lua_libp2p.transport_circuit_relay_v2.protocol")
local varint = require("lua_libp2p.multiformats.varint")

local relay_peer = "12D3KooWCryG7Mon9orvQxcS1rYZjotPgpwoJNHHKcLLfE4Hf5mV"
local local_peer = "12D3KooWQWZLu9qXWPTDnF9rTRrAiVGZrXCbHAvkqYrsG8cW4UHg"
local relay_addr = "/ip4/203.0.113.1/tcp/4001/p2p/" .. relay_peer

local Stream = {}
Stream.__index = Stream

function Stream:write(data)
  self.writes[#self.writes + 1] = data
  return true
end

function Stream:read(n)
  local chunk = self.input:sub(1, n)
  self.input = self.input:sub(n + 1)
  return chunk
end

function Stream:close()
  self.closed = true
  return true
end

local function relay_response(message)
  local payload = assert(relay_proto.encode_hop_message(message))
  return assert(varint.encode_u64(#payload)) .. payload
end

local function run()
  local stream = setmetatable({
    input = relay_response({
      type = relay_proto.HOP_TYPE.STATUS,
      status = relay_proto.STATUS.OK,
      reservation = {
        expire = 12345,
        addrs = { relay_addr, "/ip4/203.0.113.1/udp/4001/quic-v1/p2p/" .. relay_peer },
        voucher = "voucher",
      },
    }),
    writes = {},
  }, Stream)

  local seen_target, seen_protocols
  local host = {
    address_manager = address_manager.new(),
  }
  function host:peer_id()
    return { id = local_peer }
  end
  function host:new_stream(target, protocols)
    seen_target = target
    seen_protocols = protocols
    return stream, protocols[1], nil, { remote_peer_id = relay_peer }
  end

  local client, client_err = relay_client.new(host, {
    relays = { relay_addr },
  })
  if not client then
    return nil, client_err
  end

  local report, report_err = client:reserve_all()
  if not report then
    return nil, report_err
  end
  if report.attempted ~= 1 or report.reserved ~= 1 or report.failed ~= 0 then
    return nil, "unexpected reserve_all report"
  end
  if type(seen_target) ~= "table" or seen_target.peer_id ~= relay_peer or seen_target.addr ~= relay_addr then
    return nil, "relay client should dial relay target from multiaddr"
  end
  if seen_protocols[1] ~= relay_proto.HOP_ID then
    return nil, "relay client should open hop protocol stream"
  end
  if not stream.closed then
    return nil, "relay client should close reservation control stream"
  end

  local request_len, next_i = assert(varint.decode_u64(stream.writes[1], 1))
  local request = assert(relay_proto.decode_hop_message(stream.writes[1]:sub(next_i, next_i + request_len - 1)))
  if request.type ~= relay_proto.HOP_TYPE.RESERVE then
    return nil, "relay client should send RESERVE"
  end

  local advertised = host.address_manager:get_relay_addrs()
  local expected = relay_addr .. "/p2p-circuit/p2p/" .. local_peer
  local expected_quic = "/ip4/203.0.113.1/udp/4001/quic-v1/p2p/" .. relay_peer .. "/p2p-circuit/p2p/" .. local_peer
  if #advertised ~= 2 or advertised[1] ~= expected then
    return nil, "relay client should publish public relayed addrs"
  end
  if advertised[2] ~= expected_quic then
    return nil, "relay client should publish relay-provided quic relayed addr"
  end
  if report.reservations[1].relay_addrs[1] ~= expected then
    return nil, "reservation result should include relayed addr"
  end

  local reservations = client:get_reservations()
  if #reservations ~= 1 or reservations[1].relay_peer_id ~= relay_peer then
    return nil, "relay client should retain reservation by relay peer"
  end

  return true
end

return {
  name = "circuit relay client reservations",
  run = run,
}
