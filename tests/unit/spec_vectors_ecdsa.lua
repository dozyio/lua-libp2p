local hex = require("tests.helpers.hex")
local key_pb = require("lua_libp2p.crypto.key_pb")
local multihash = require("lua_libp2p.multiformats.multihash")
local peerid = require("lua_libp2p.peerid")

local function run()
  -- Vector from https://github.com/libp2p/specs/blob/master/peer-ids/peer-ids.md
  local public_key_proto = hex.decode(
    "0803125b3059301306072a8648ce3d020106082a8648ce3d03010703420004de3d300fa36ae0e8f5d530899d83abab44abf3161f162a4bc901d8e6ecda020e8b6d5f8da30525e71d6851510c098e5c47c646a597fb4dcec034e9f77c409e62"
  )
  local private_key_proto = hex.decode(
    "08031279307702010104203e5b1fe9712e6c314942a750bd67485de3c1efe85b1bfb520ae8f9ae3dfa4a4ca00a06082a8648ce3d030107a14403420004de3d300fa36ae0e8f5d530899d83abab44abf3161f162a4bc901d8e6ecda020e8b6d5f8da30525e71d6851510c098e5c47c646a597fb4dcec034e9f77c409e62"
  )

  local decoded_pub, pub_err = key_pb.decode_public_key(public_key_proto)
  if not decoded_pub then
    return nil, pub_err
  end
  if decoded_pub.type ~= key_pb.KEY_TYPE.ECDSA then
    return nil, "expected ecdsa public key type in spec vector"
  end

  local decoded_priv, priv_err = key_pb.decode_private_key(private_key_proto)
  if not decoded_priv then
    return nil, priv_err
  end
  if decoded_priv.type ~= key_pb.KEY_TYPE.ECDSA then
    return nil, "expected ecdsa private key type in spec vector"
  end

  local reencoded_pub, enc_err = key_pb.encode_public_key(decoded_pub.type, decoded_pub.data)
  if not reencoded_pub then
    return nil, enc_err
  end
  if reencoded_pub ~= public_key_proto then
    return nil, "ecdsa public key protobuf roundtrip mismatch"
  end

  local pid, pid_err = peerid.from_public_key_proto(public_key_proto, "ecdsa")
  if not pid then
    return nil, pid_err
  end
  if pid.type ~= "ecdsa" then
    return nil, "expected ecdsa peer id type"
  end
  if pid.id ~= "QmVMT29id3TUASyfZZ6k9hmNyc2nYabCo4uMSpDw4zrgDk" then
    return nil, "unexpected ecdsa peer id from spec vector"
  end
  if pid.cid ~= "bafzbeidigywdclqvl5hxfefwp5onbffcfife7pza57mmfb4tiqmtkdjw64" then
    return nil, "unexpected ecdsa peer id CID from spec vector"
  end

  local mh, mh_err = multihash.decode(pid.bytes)
  if not mh then
    return nil, mh_err
  end
  if mh.code ~= multihash.SHA2_256 or #mh.digest ~= 32 then
    return nil, "expected ecdsa spec peer id to use sha2-256"
  end

  local parsed, parse_err = peerid.parse(pid.id)
  if not parsed then
    return nil, parse_err
  end
  if parsed.id ~= pid.id then
    return nil, "ecdsa peer id should parse roundtrip"
  end

  return true
end

return {
  name = "spec ecdsa key vectors",
  run = run,
}
