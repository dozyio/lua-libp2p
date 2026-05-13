local autonat = require("lua_libp2p.protocol.autonat_v1")
local multiaddr = require("lua_libp2p.multiformats.multiaddr")

local function run()
  local addr = assert(multiaddr.to_bytes("/ip4/203.0.113.10/tcp/4001"))
  local encoded = assert(autonat.encode_message({
    type = autonat.MESSAGE_TYPE.DIAL,
    dial = {
      peer = {
        id = "\001\002\003",
        addrs = { addr },
      },
    },
  }))
  local decoded = assert(autonat.decode_message(encoded))
  if decoded.type ~= autonat.MESSAGE_TYPE.DIAL then
    return nil, "autonat v1 should decode dial type"
  end
  if not decoded.dial or not decoded.dial.peer or decoded.dial.peer.addrs[1] ~= addr then
    return nil, "autonat v1 should roundtrip dial request"
  end

  encoded = assert(autonat.encode_message({
    type = autonat.MESSAGE_TYPE.DIAL_RESPONSE,
    dialResponse = {
      status = autonat.RESPONSE_STATUS.OK,
      statusText = "ok",
      addr = addr,
    },
  }))
  decoded = assert(autonat.decode_message(encoded))
  if not decoded.dialResponse or decoded.dialResponse.status ~= autonat.RESPONSE_STATUS.OK then
    return nil, "autonat v1 should roundtrip dial response"
  end

  return true
end

return {
  name = "autonat v1 protocol codec",
  run = run,
}
