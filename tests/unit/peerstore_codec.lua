local codec = require("lua_libp2p.peerstore.codec")

local peer_id = "12D3KooWCryG7Mon9orvQxcS1rYZjotPgpwoJNHHKcLLfE4Hf5mV"

local function run()
  local record = {
    peer_id = "peer-a",
    addrs = {
      ["/ip4/127.0.0.1/tcp/4001"] = {
        addr = "/ip4/127.0.0.1/tcp/4001",
        certified = true,
        expires_at = 12345,
        last_seen = 12000,
      },
      ["/ip4/127.0.0.1/tcp/4002/p2p/" .. peer_id] = {
        addr = "/ip4/127.0.0.1/tcp/4002/p2p/" .. peer_id,
        certified = false,
        last_seen = 12001,
      },
    },
    protocols = {
      ["/ipfs/id/1.0.0"] = true,
      ["/ipfs/ping/1.0.0"] = true,
    },
    tags = {
      bootstrap = {
        name = "bootstrap",
        value = 50,
      },
    },
    metadata = {
      agent = "test",
      score = 1.5,
      active = true,
    },
    public_key = "public-key",
    peer_record_envelope = "envelope",
    updated_at = 999,
  }

  local encoded, encode_err = codec.encode(record)
  if type(encoded) ~= "string" then
    return nil, encode_err or "expected encoded peerstore record bytes"
  end
  local encoded_again = assert(codec.encode(record))
  if encoded ~= encoded_again then
    return nil, "peerstore codec should be deterministic"
  end
  if encoded:find("/ip4/127.0.0.1", 1, true) then
    return nil, "peerstore codec should store multiaddrs as binary bytes, not text"
  end

  local decoded, decode_err = codec.decode(encoded)
  if not decoded then
    return nil, decode_err
  end
  if decoded.peer_id ~= record.peer_id
    or decoded.metadata.agent ~= "test"
    or decoded.metadata.score ~= 1.5
    or decoded.metadata.active ~= true
    or decoded.protocols["/ipfs/ping/1.0.0"] ~= true
    or decoded.addrs["/ip4/127.0.0.1/tcp/4001"].certified ~= true
    or decoded.addrs["/ip4/127.0.0.1/tcp/4002/p2p/" .. peer_id] == nil
  then
    return nil, "decoded peerstore record should match original"
  end

  local bad, bad_err = codec.decode("not-a-peerstore-record")
  if bad ~= nil or not bad_err or bad_err.kind ~= "decode" then
    return nil, "invalid codec bytes should fail decode"
  end

  local unsupported, unsupported_err = codec.encode({
    peer_id = "peer-b",
    metadata = { nested = {} },
  })
  if unsupported ~= nil or not unsupported_err or unsupported_err.kind ~= "input" then
    return nil, "unsupported metadata values should fail encode"
  end

  return true
end

return {
  name = "peerstore record codec",
  run = run,
}
