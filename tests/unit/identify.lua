local ed25519 = require("lua_libp2p.crypto.ed25519")
local identify = require("lua_libp2p.protocol_identify.protocol")
local key_pb = require("lua_libp2p.crypto.key_pb")
local multiaddr = require("lua_libp2p.multiaddr")
local peer_record = require("lua_libp2p.record.peer_record")
local peerid = require("lua_libp2p.peerid")
local signed_envelope = require("lua_libp2p.record.signed_envelope")
local varint = require("lua_libp2p.multiformats.varint")

local function new_scripted_conn(incoming)
  local conn = {
    _in = incoming or "",
    _out = "",
  }

  function conn:read(n)
    local want = n or #self._in
    if #self._in < want then
      return nil, "unexpected EOF"
    end
    local out = self._in:sub(1, want)
    self._in = self._in:sub(want + 1)
    return out
  end

  function conn:write(payload)
    self._out = self._out .. payload
    return true
  end

  function conn:writes()
    return self._out
  end

  return conn
end

local function run()
  local key, key_err = ed25519.generate_keypair()
  if not key then
    return nil, key_err
  end

  local public_key, pub_err = key_pb.encode_public_key(key_pb.KEY_TYPE.Ed25519, key.public_key)
  if not public_key then
    return nil, pub_err
  end

  local msg = {
    protocolVersion = "/my-network/0.1.0",
    agentVersion = "lua-libp2p/test",
    publicKey = public_key,
    listenAddrs = {
      assert(multiaddr.to_bytes("/ip4/127.0.0.1/tcp/4001")),
      assert(multiaddr.to_bytes("/dns4/example.com/tcp/443/tls/ws")),
    },
    observedAddr = assert(multiaddr.to_bytes("/ip4/203.0.113.9/tcp/50000")),
    protocols = {
      "/ipfs/ping/1.0.0",
      "/ipfs/id/1.0.0",
    },
  }

  local pid, pid_err = peerid.from_ed25519_public_key(key.public_key)
  if not pid then
    return nil, pid_err
  end
  local rec = assert(peer_record.make_record(pid.id, 7, { "/ip4/127.0.0.1/tcp/4001" }))
  local env = assert(peer_record.sign_ed25519(key, rec))
  local env_bytes = assert(signed_envelope.encode(env))
  msg.signedPeerRecord = env_bytes

  local payload, enc_err = identify.encode(msg)
  if not payload then
    return nil, enc_err
  end
  local decoded, dec_err = identify.decode(payload)
  if not decoded then
    return nil, dec_err
  end

  if decoded.protocolVersion ~= msg.protocolVersion or decoded.agentVersion ~= msg.agentVersion then
    return nil, "identify string fields mismatch"
  end
  if decoded.publicKey ~= msg.publicKey then
    return nil, "identify publicKey mismatch"
  end
  if #decoded.listenAddrs ~= 2 or decoded.listenAddrs[2] ~= msg.listenAddrs[2] then
    return nil, "identify listenAddrs mismatch"
  end
  if #decoded.protocols ~= 2 or decoded.protocols[1] ~= msg.protocols[1] then
    return nil, "identify protocols mismatch"
  end
  if decoded.signedPeerRecord ~= msg.signedPeerRecord then
    return nil, "identify signedPeerRecord mismatch"
  end

  local verified_record, record_err = identify.verify_signed_peer_record(decoded, { expected_peer_id = pid.id })
  if not verified_record then
    return nil, record_err
  end
  if verified_record.record.seq ~= 7 then
    return nil, "identify signedPeerRecord verify mismatch"
  end

  local writer = new_scripted_conn("")
  local ok, write_err = identify.write(writer, msg)
  if not ok then
    return nil, write_err
  end
  local reader = new_scripted_conn(writer:writes())
  local read_back, read_err = identify.read(reader)
  if not read_back then
    return nil, read_err
  end
  if read_back.observedAddr ~= msg.observedAddr then
    return nil, "identify framed read/write mismatch"
  end

  local oversized_identify_payload = string.rep("\0", identify.MAX_MESSAGE_SIZE + 1)
  local oversized_identify_frame = assert(varint.encode_u64(#oversized_identify_payload)) .. oversized_identify_payload
  local oversized_reader = new_scripted_conn(oversized_identify_frame)
  local oversized_msg, oversized_err = identify.read(oversized_reader)
  if oversized_msg ~= nil then
    return nil, "expected oversized identify message to fail"
  end
  if not oversized_err or oversized_err.kind ~= "decode" then
    return nil, "expected decode error for oversized identify message"
  end

  local unknown_tag = assert(varint.encode_u64(99 * 8 + 0))
  local unknown_val = assert(varint.encode_u64(1))
  local with_unknown = payload .. unknown_tag .. unknown_val
  local decoded_unknown, unknown_err = identify.decode(with_unknown)
  if not decoded_unknown then
    return nil, unknown_err
  end
  if decoded_unknown.protocolVersion ~= msg.protocolVersion then
    return nil, "identify decode should ignore unknown fields"
  end

  local merged, merge_err = identify.merge({
    {
      protocolVersion = "/net/1",
      listenAddrs = { msg.listenAddrs[1] },
      protocols = { "/ipfs/id/1.0.0" },
    },
    {
      protocolVersion = "/net/2",
      listenAddrs = { msg.listenAddrs[1], msg.listenAddrs[2] },
      protocols = { "/ipfs/id/1.0.0", "/ipfs/ping/1.0.0" },
      signedPeerRecord = "later-record",
    },
  })
  if not merged then
    return nil, merge_err
  end
  if merged.protocolVersion ~= "/net/2" then
    return nil, "identify merge should prefer later singular fields"
  end
  if merged.signedPeerRecord ~= "later-record" then
    return nil, "identify merge should keep latest signedPeerRecord"
  end
  if #merged.listenAddrs ~= 2 or #merged.protocols ~= 2 then
    return nil, "identify merge should append and dedupe repeated fields"
  end

  local parsed_ma, ma_err = multiaddr.from_bytes(decoded.listenAddrs[1])
  if not parsed_ma then
    return nil, ma_err
  end
  if parsed_ma.text ~= "/ip4/127.0.0.1/tcp/4001" then
    return nil, "identify listenAddr binary multiaddr mismatch"
  end

  return true
end

return {
  name = "identify message encode/decode",
  run = run,
}
