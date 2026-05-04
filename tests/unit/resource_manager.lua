local resource_manager = require("lua_libp2p.resource_manager")

local function expect_default_limits()
  local defaults = resource_manager.default_limits()
  if defaults.connections ~= 128 or defaults.connections_inbound ~= 64 then
    return nil, "unexpected default connection limits"
  end
  if defaults.streams ~= 2048 or defaults.streams_inbound ~= 1024 then
    return nil, "unexpected default stream limits"
  end
  if defaults.connections_per_peer ~= 8 or defaults.connections_inbound_per_peer ~= 8 then
    return nil, "unexpected default peer connection limits"
  end
  if defaults.streams_per_peer ~= 512 or defaults.streams_inbound_per_peer ~= 256 then
    return nil, "unexpected default peer limits"
  end

  local rm = resource_manager.new()
  local stats = rm:stats()
  if stats.limits.connections ~= 128 or stats.limits.protocol.default.streams_inbound ~= 512 then
    return nil, "expected resource manager to use default limits"
  end
  if stats.limits.protocol_peer.default.streams_inbound ~= 64 then
    return nil, "expected default protocol-peer inbound stream limit"
  end
  return true
end

local function run()
  local defaults_ok, defaults_err = expect_default_limits()
  if not defaults_ok then
    return nil, defaults_err
  end

  local rm = resource_manager.new({
    connections = 2,
    connections_inbound = 1,
    transient_connections = 1,
    streams = 2,
    streams_inbound = 1,
    transient_streams = 1,
    streams_per_peer = 1,
    protocol = {
      ["/limited/1.0.0"] = { streams = 1 },
    },
  })

  local inbound, inbound_err = rm:open_connection("inbound", nil, { transient = true })
  if not inbound then
    return nil, inbound_err
  end
  local blocked_transient = rm:open_connection("inbound", nil, { transient = true })
  if blocked_transient then
    return nil, "expected transient inbound connection limit to block"
  end
  local set_ok, set_err = rm:set_connection_peer(inbound, "peer-a")
  if not set_ok then
    return nil, set_err
  end
  local second_inbound = rm:open_connection("inbound", "peer-b")
  if second_inbound then
    return nil, "expected inbound connection limit to block"
  end

  local outbound, outbound_err = rm:open_connection("outbound", "peer-b")
  if not outbound then
    return nil, outbound_err
  end
  local blocked_system = rm:open_connection("outbound", "peer-c")
  if blocked_system then
    return nil, "expected system connection limit to block"
  end
  rm:close_connection(outbound)

  local stream_a, stream_a_err = rm:open_stream("peer-a", "inbound")
  if not stream_a then
    return nil, stream_a_err
  end
  local blocked_stream = rm:open_stream("peer-a", "inbound")
  if blocked_stream then
    return nil, "expected peer stream limit to block"
  end
  local proto_ok, proto_err = rm:set_stream_protocol(stream_a, "/limited/1.0.0")
  if not proto_ok then
    return nil, proto_err
  end
  local stream_b, stream_b_err = rm:open_stream("peer-b", "outbound", "/limited/1.0.0")
  if stream_b then
    return nil, "expected protocol stream limit to block"
  end
  if not stream_b_err or stream_b_err.kind ~= "resource" then
    return nil, "expected protocol limit resource error"
  end

  rm:close_stream(stream_a)
  rm:close_connection(inbound)
  local stats = rm:stats()
  if stats.system.connections ~= 0 or stats.system.streams ~= 0 then
    return nil, "expected resource stats to return to zero"
  end

  local directional = resource_manager.new({
    default_limits = false,
    streams = 4,
    streams_inbound = 4,
    streams_per_peer = 4,
    streams_inbound_per_peer = 2,
  })
  local stream_one = directional:open_stream("peer-direction", "inbound", "/test/1.0.0")
  local stream_two = directional:open_stream("peer-direction", "inbound", "/test/1.0.0")
  if not stream_one or not stream_two then
    return nil, "expected first two inbound peer streams to fit"
  end
  local blocked_inbound, blocked_inbound_err = directional:open_stream("peer-direction", "inbound", "/test/1.0.0")
  if blocked_inbound then
    return nil, "expected directional peer inbound stream limit to block"
  end
  if not blocked_inbound_err or blocked_inbound_err.kind ~= "resource" then
    return nil, "expected directional peer inbound resource error"
  end
  directional:close_stream(stream_one)
  directional:close_stream(stream_two)

  local protocol_peer = resource_manager.new({
    default_limits = false,
    streams = 8,
    streams_inbound = 8,
    streams_per_peer = 8,
    streams_inbound_per_peer = 8,
    protocol = {
      default = { streams = 8, streams_inbound = 8 },
    },
    protocol_peer = {
      default = { streams = 2, streams_inbound = 2 },
    },
  })
  local proto_stream_one = protocol_peer:open_stream("peer-proto", "inbound", "/proto/1.0.0")
  local proto_stream_two = protocol_peer:open_stream("peer-proto", "inbound", "/proto/1.0.0")
  if not proto_stream_one or not proto_stream_two then
    return nil, "expected first two protocol-peer streams to fit"
  end
  local blocked_proto_peer, blocked_proto_peer_err = protocol_peer:open_stream(
    "peer-proto",
    "inbound",
    "/proto/1.0.0"
  )
  if blocked_proto_peer then
    return nil, "expected protocol-peer inbound stream limit to block"
  end
  if not blocked_proto_peer_err or blocked_proto_peer_err.context.scope ~= "protocol_peer" then
    return nil, "expected protocol-peer resource error"
  end
  local other_peer_stream, other_peer_err = protocol_peer:open_stream("peer-other", "inbound", "/proto/1.0.0")
  if not other_peer_stream then
    return nil, other_peer_err
  end
  local proto_stats = protocol_peer:stats()
  if proto_stats.protocol_peer["/proto/1.0.0"]["peer-proto"].streams_inbound ~= 2 then
    return nil, "expected protocol-peer stream stats"
  end
  protocol_peer:close_stream(proto_stream_one)
  protocol_peer:close_stream(proto_stream_two)
  protocol_peer:close_stream(other_peer_stream)

  return true
end

return {
  name = "resource manager count limits",
  run = run,
}
