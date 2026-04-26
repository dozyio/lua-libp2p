local hex = require("tests.helpers.hex")
local key_pb = require("lua_libp2p.crypto.key_pb")
local multihash = require("lua_libp2p.multiformats.multihash")
local peerid = require("lua_libp2p.peerid")

local function run()
  -- Vector from https://github.com/libp2p/specs/blob/master/peer-ids/peer-ids.md
  local public_key_proto = hex.decode("08021221037777e994e452c21604f91de093ce415f5432f701dd8cd1a7a6fea0e630bfca99")
  local private_key_proto = hex.decode("0802122053dadf1d5a164d6b4acdb15e24aa4c5b1d3461bdbd42abedb0a4404d56ced8fb")

  local decoded_pub, pub_err = key_pb.decode_public_key(public_key_proto)
  if not decoded_pub then
    return nil, pub_err
  end
  if decoded_pub.type ~= key_pb.KEY_TYPE.Secp256k1 then
    return nil, "expected secp256k1 public key type in spec vector"
  end
  if #decoded_pub.data ~= 33 or decoded_pub.data:byte(1) ~= 0x03 then
    return nil, "expected compressed secp256k1 public key bytes"
  end

  local decoded_priv, priv_err = key_pb.decode_private_key(private_key_proto)
  if not decoded_priv then
    return nil, priv_err
  end
  if decoded_priv.type ~= key_pb.KEY_TYPE.Secp256k1 or #decoded_priv.data ~= 32 then
    return nil, "expected secp256k1 private key type and scalar length"
  end

  local reencoded_pub, enc_err = key_pb.encode_public_key(decoded_pub.type, decoded_pub.data)
  if not reencoded_pub then
    return nil, enc_err
  end
  if reencoded_pub ~= public_key_proto then
    return nil, "secp256k1 public key protobuf roundtrip mismatch"
  end

  local pid, pid_err = peerid.from_public_key_proto(public_key_proto, "secp256k1")
  if not pid then
    return nil, pid_err
  end
  if pid.type ~= "secp256k1" then
    return nil, "expected secp256k1 peer id type"
  end
  if pid.id ~= "16Uiu2HAmLhLvBoYaoZfaMUKuibM6ac163GwKY74c5kiSLg5KvLpY" then
    return nil, "unexpected secp256k1 peer id from spec vector"
  end
  if pid.cid ~= "bafzaajiiaijcca3xo7uzjzcsyilaj6i54cj44qk7kqzpoao5rti2pjx6udtdbp6kte" then
    return nil, "unexpected secp256k1 peer id CID from spec vector"
  end

  local mh, mh_err = multihash.decode(pid.bytes)
  if not mh then
    return nil, mh_err
  end
  if mh.code ~= multihash.IDENTITY then
    return nil, "expected secp256k1 spec peer id to use identity multihash"
  end

  local parsed, parse_err = peerid.parse(pid.id)
  if not parsed then
    return nil, parse_err
  end
  if parsed.id ~= pid.id or parsed.type ~= "secp256k1" then
    return nil, "secp256k1 peer id should parse roundtrip"
  end

  return true
end

return {
  name = "spec secp256k1 key vectors",
  run = run,
}
