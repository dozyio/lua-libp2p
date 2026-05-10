local host_mod = require("lua_libp2p.host")
local upgrader = require("lua_libp2p.network.upgrader")
local relay_proto = require("lua_libp2p.transport_circuit_relay_v2.protocol")
local varint = require("lua_libp2p.multiformats.varint")

local relay_peer = "12D3KooWCryG7Mon9orvQxcS1rYZjotPgpwoJNHHKcLLfE4Hf5mV"
local dst_peer = "12D3KooWQWZLu9qXWPTDnF9rTRrAiVGZrXCbHAvkqYrsG8cW4UHg"
local relay_addr = "/ip4/203.0.113.1/tcp/4001/p2p/" .. relay_peer
local relayed_addr = relay_addr .. "/p2p-circuit/p2p/" .. dst_peer

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
  local h, h_err = host_mod.new({
    runtime = "luv",
    blocking = false,
  })
  if not h then
    return nil, h_err
  end

  local stream = setmetatable({
    input = relay_response({
      type = relay_proto.HOP_TYPE.STATUS,
      status = relay_proto.STATUS.OK,
      limit = { duration = 30, data = 1024 },
    }),
    writes = {},
  }, Stream)
  local seen_target, seen_protocols
  function h:new_stream(target, protocols)
    seen_target = target
    seen_protocols = protocols
    return stream, protocols[1], { relay = true }, { remote_peer_id = relay_peer }
  end

  local raw, state, dial_err = h:_dial_relay_raw(relayed_addr)
  if not raw then
    return nil, dial_err
  end
  if raw ~= stream then
    return nil, "relay raw dial should return hop stream"
  end
  if seen_target ~= relay_addr then
    return nil, "relay raw dial should open stream to relay addr"
  end
  if seen_protocols[1] ~= relay_proto.HOP_ID then
    return nil, "relay raw dial should use hop protocol"
  end
  if state.relay_peer_id ~= relay_peer or state.destination_peer_id ~= dst_peer then
    return nil, "relay raw dial should return relay state"
  end
  if state.limit_kind ~= "limited" or state.limit.duration ~= 30 then
    return nil, "relay raw dial should tag limited connections"
  end

  local request_len, next_i = assert(varint.decode_u64(stream.writes[1], 1))
  local request = assert(relay_proto.decode_hop_message(stream.writes[1]:sub(next_i, next_i + request_len - 1)))
  if request.type ~= relay_proto.HOP_TYPE.CONNECT or not request.peer then
    return nil, "relay raw dial should send CONNECT request"
  end

  local bad_stream = setmetatable({
    input = relay_response({ type = relay_proto.HOP_TYPE.STATUS, status = relay_proto.STATUS.NO_RESERVATION }),
    writes = {},
  }, Stream)
  function h:new_stream(_, protocols)
    return bad_stream, protocols[1], nil, {}
  end
  local failed, _, failed_err = h:_dial_relay_raw(relayed_addr)
  if failed or not failed_err then
    return nil, "expected relay raw dial to fail on non-OK connect status"
  end
  if not bad_stream.closed then
    return nil, "relay raw dial should close failed hop stream"
  end

  local stop_payload = assert(relay_proto.encode_stop_message({
    type = relay_proto.STOP_TYPE.CONNECT,
    peer = { peer_id = dst_peer },
    limit = { duration = 10, data = 99 },
  }))
  local stop_stream = setmetatable({
    input = assert(varint.encode_u64(#stop_payload)) .. stop_payload,
    writes = {},
  }, Stream)
  local original_upgrade_inbound = upgrader.upgrade_inbound
  local upgraded_stream, upgraded_opts
  upgrader.upgrade_inbound = function(raw_conn, opts)
    upgraded_stream = raw_conn
    upgraded_opts = opts
    return {
      close = function()
        return true
      end,
    }, { remote_peer_id = dst_peer }
  end
  local handled, handle_err = h:_handle_relay_stop(stop_stream, {
    state = { remote_peer_id = relay_peer },
  })
  upgrader.upgrade_inbound = original_upgrade_inbound
  if not handled then
    return nil, handle_err
  end
  if upgraded_stream ~= stop_stream then
    return nil, "relay stop handler should upgrade the stop stream as raw connection"
  end
  if not upgraded_opts or upgraded_opts.local_keypair ~= h.identity then
    return nil, "relay stop handler should pass host identity into inbound upgrade"
  end
  local entry = h:_find_connection(dst_peer)
  if not entry or entry.state.relay.relay_peer_id ~= relay_peer or entry.state.relay.limit_kind ~= "limited" then
    return nil, "relay stop handler should register inbound relay metadata"
  end

  return true
end

return {
  name = "host relay raw dial",
  run = run,
}
