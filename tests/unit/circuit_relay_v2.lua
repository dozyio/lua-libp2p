local relay = require("lua_libp2p.protocol.circuit_relay_v2")
local varint = require("lua_libp2p.multiformats.varint")

local relay_peer = "12D3KooWCryG7Mon9orvQxcS1rYZjotPgpwoJNHHKcLLfE4Hf5mV"
local dst_peer = "12D3KooWQWZLu9qXWPTDnF9rTRrAiVGZrXCbHAvkqYrsG8cW4UHg"

local MemoryStream = {}
MemoryStream.__index = MemoryStream

function MemoryStream:write(data)
  self.writes[#self.writes + 1] = data
  return true
end

function MemoryStream:read(n)
  if #self.input == 0 then
    return nil, "empty input"
  end
  local chunk = self.input:sub(1, n)
  self.input = self.input:sub(n + 1)
  return chunk
end

local function stream_with_response(message)
  local payload = assert(relay.encode_hop_message(message))
  local len = assert(varint.encode_u64(#payload))
  return setmetatable({ input = len .. payload, writes = {} }, MemoryStream)
end

local function run()
  local msg = {
    type = relay.HOP_TYPE.STATUS,
    status = relay.STATUS.OK,
    reservation = {
      expire = 12345,
      addrs = { "/ip4/203.0.113.1/tcp/4001/p2p/" .. relay_peer },
      voucher = "voucher-bytes",
    },
    limit = { duration = 60, data = 1024 },
  }
  local encoded, enc_err = relay.encode_hop_message(msg)
  if not encoded then
    return nil, enc_err
  end
  local decoded, dec_err = relay.decode_hop_message(encoded)
  if not decoded then
    return nil, dec_err
  end
  if decoded.type ~= relay.HOP_TYPE.STATUS or decoded.status ~= relay.STATUS.OK then
    return nil, "relay status message roundtrip mismatch"
  end
  if not encoded:find(string.char(34), 1, true) then
    return nil, "hop limit should use protobuf field 4"
  end
  if not encoded:find(string.char(40), 1, true) then
    return nil, "hop status should use protobuf field 5"
  end
  if decoded.reservation.expire ~= 12345 or decoded.reservation.voucher ~= "voucher-bytes" then
    return nil, "relay reservation roundtrip mismatch"
  end
  if decoded.limit.duration ~= 60 or decoded.limit.data ~= 1024 then
    return nil, "relay limit roundtrip mismatch"
  end

  local peer_payload, peer_err = relay.encode_peer({ peer_id = dst_peer, addrs = { "/ip4/127.0.0.1/tcp/4001" } })
  if not peer_payload then
    return nil, peer_err
  end
  local peer_decoded, peer_dec_err = relay.decode_peer(peer_payload)
  if not peer_decoded then
    return nil, peer_dec_err
  end
  if type(peer_decoded.id) ~= "string" or #peer_decoded.addrs ~= 1 then
    return nil, "relay peer roundtrip mismatch"
  end

  local reserve_stream = stream_with_response(msg)
  local reservation, reserve_response_or_err = relay.reserve(reserve_stream)
  if not reservation then
    return nil, reserve_response_or_err
  end
  if reservation.expire ~= 12345 then
    return nil, "reserve should return reservation"
  end
  local request = assert(relay.decode_hop_message(reserve_stream.writes[1]:sub(2)))
  if request.type ~= relay.HOP_TYPE.RESERVE then
    return nil, "reserve should write RESERVE request"
  end

  local connect_stream = stream_with_response({ type = relay.HOP_TYPE.STATUS, status = relay.STATUS.OK })
  local connected, connect_err = relay.connect(connect_stream, dst_peer)
  if not connected then
    return nil, connect_err
  end
  local connect_request = assert(relay.decode_hop_message(connect_stream.writes[1]:sub(2)))
  if connect_request.type ~= relay.HOP_TYPE.CONNECT or not connect_request.peer then
    return nil, "connect should write CONNECT request with destination peer"
  end

  local denied_stream = stream_with_response({ type = relay.HOP_TYPE.STATUS, status = relay.STATUS.NO_RESERVATION })
  local denied, denied_err = relay.connect(denied_stream, dst_peer)
  if denied or not denied_err then
    return nil, "expected relay connect failure on non-OK status"
  end

  local stop_payload, stop_err = relay.encode_stop_message({
    type = relay.STOP_TYPE.STATUS,
    status = relay.STATUS.OK,
    limit = { duration = 30, data = 2048 },
  })
  if not stop_payload then
    return nil, stop_err
  end
  if not stop_payload:find(string.char(26), 1, true) then
    return nil, "stop limit should use protobuf field 3"
  end
  if not stop_payload:find(string.char(32), 1, true) then
    return nil, "stop status should use protobuf field 4"
  end
  local stop_decoded, stop_dec_err = relay.decode_stop_message(stop_payload)
  if not stop_decoded then
    return nil, stop_dec_err
  end
  if stop_decoded.type ~= relay.STOP_TYPE.STATUS or stop_decoded.status ~= relay.STATUS.OK then
    return nil, "stop status message roundtrip mismatch"
  end
  if relay.classify_limit(nil) ~= "unlimited" then
    return nil, "missing relay limit should classify as unlimited"
  end
  if relay.classify_limit({ duration = 0, data = 0 }) ~= "unlimited" then
    return nil, "zero relay limit should classify as unlimited"
  end
  if relay.classify_limit({ duration = 30, data = 0 }) ~= "limited" then
    return nil, "non-zero relay limit should classify as limited"
  end

  local stop_stream = stream_with_response({
    type = relay.STOP_TYPE.CONNECT,
    peer = { peer_id = dst_peer },
    limit = { duration = 15, data = 4096 },
  })
  stop_stream.input = assert(varint.encode_u64(#assert(relay.encode_stop_message({
    type = relay.STOP_TYPE.CONNECT,
    peer = { peer_id = dst_peer },
    limit = { duration = 15, data = 4096 },
  })))) .. assert(relay.encode_stop_message({
    type = relay.STOP_TYPE.CONNECT,
    peer = { peer_id = dst_peer },
    limit = { duration = 15, data = 4096 },
  }))
  stop_stream.writes = {}
  local accepted, accept_err = relay.accept_stop(stop_stream)
  if not accepted then
    return nil, accept_err
  end
  if accepted.limit_kind ~= "limited" or accepted.limit.duration ~= 15 then
    return nil, "accept_stop should expose relay limit metadata"
  end
  local ok_len, ok_next_i = assert(varint.decode_u64(stop_stream.writes[1], 1))
  local ok_response = assert(relay.decode_stop_message(stop_stream.writes[1]:sub(ok_next_i, ok_next_i + ok_len - 1)))
  if ok_response.type ~= relay.STOP_TYPE.STATUS or ok_response.status ~= relay.STATUS.OK then
    return nil, "accept_stop should write OK status"
  end

  return true
end

return {
  name = "circuit relay v2 protocol codec",
  run = run,
}
