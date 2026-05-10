local autonat = require("lua_libp2p.protocol.autonat_v2")
local multiaddr = require("lua_libp2p.multiaddr")

local function run()
  local addr = assert(multiaddr.to_bytes("/ip4/203.0.113.10/tcp/4001"))
  local nonce = 123456789
  local encoded = assert(autonat.encode_message({
    dialRequest = {
      addrs = { addr },
      nonce = nonce,
    },
  }))
  local decoded = assert(autonat.decode_message(encoded))
  if not decoded.dialRequest or decoded.dialRequest.nonce ~= nonce or decoded.dialRequest.addrs[1] ~= addr then
    return nil, "autonat should roundtrip dial request"
  end

  encoded = assert(autonat.encode_message({
    dialDataRequest = {
      addrIdx = 0,
      numBytes = 30000,
    },
  }))
  decoded = assert(autonat.decode_message(encoded))
  if
    not decoded.dialDataRequest
    or decoded.dialDataRequest.addrIdx ~= 0
    or decoded.dialDataRequest.numBytes ~= 30000
  then
    return nil, "autonat should roundtrip dial data request"
  end

  encoded = assert(autonat.encode_message({
    dialResponse = {
      status = autonat.RESPONSE_STATUS.OK,
      addrIdx = 1,
      dialStatus = autonat.DIAL_STATUS.OK,
    },
  }))
  decoded = assert(autonat.decode_message(encoded))
  if
    not decoded.dialResponse
    or decoded.dialResponse.addrIdx ~= 1
    or decoded.dialResponse.dialStatus ~= autonat.DIAL_STATUS.OK
  then
    return nil, "autonat should roundtrip dial response"
  end

  local back = assert(autonat.decode_dial_back(assert(autonat.encode_dial_back({ nonce = nonce }))))
  if back.nonce ~= nonce then
    return nil, "autonat should roundtrip dial-back nonce"
  end

  local back_response = assert(autonat.decode_dial_back_response(assert(autonat.encode_dial_back_response({}))))
  if back_response.status ~= autonat.DIAL_BACK_STATUS.OK then
    return nil, "autonat should roundtrip dial-back response"
  end

  return true
end

return {
  name = "autonat v2 protocol codec",
  run = run,
}
