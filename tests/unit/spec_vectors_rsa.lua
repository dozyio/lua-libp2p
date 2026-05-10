local hex = require("tests.helpers.hex")
local key_pb = require("lua_libp2p.crypto.key_pb")
local peerid = require("lua_libp2p.peerid")

local function run()
  local public_key_proto = hex.decode(
    "080012a60430820222300d06092a864886f70d01010105000382020f003082020a0282020100e1beab071d08200bde24eef00d049449b07770ff9910257b2d7d5dda242ce8f0e2f12e1af4b32d9efd2c090f66b0f29986dbb645dae9880089704a94e5066d594162ae6ee8892e6ec70701db0a6c445c04778eb3de1293aa1a23c3825b85c6620a2bc3f82f9b0c309bc0ab3aeb1873282bebd3da03c33e76c21e9beb172fd44c9e43be32e2c99827033cf8d0f0c606f4579326c930eb4e854395ad941256542c793902185153c474bed109d6ff5141ebf9cd256cf58893a37f83729f97e7cb435ec679d2e33901d27bb35aa0d7e20561da08885ef0abbf8e2fb48d6a5487047a9ecb1ad41fa7ed84f6e3e8ecd5d98b3982d2a901b4454991766da295ab78822add5612a2df83bcee814cf50973e80d7ef38111b1bd87da2ae92438a2c8cbcc70b31ee319939a3b9c761dbc13b5c086d6b64bf7ae7dacc14622375d92a8ff9af7eb962162bbddebf90acb32adb5e4e4029f1c96019949ecfbfeffd7ac1e3fbcc6b6168c34be3d5a2e5999fcbb39bba7adbca78eab09b9bc39f7fa4b93411f4cc175e70c0a083e96bfaefb04a9580b4753c1738a6a760ae1afd851a1a4bdad231cf56e9284d832483df215a46c1c21bdf0c6cfe951c18f1ee4078c79c13d63edb6e14feaeffabc90ad317e4875fe648101b0864097e998f0ca3025ef9638cd2b0caecd3770ab54a1d9c6ca959b0f5dcbc90caeefc4135baca6fd475224269bbe1b0203010001"
  )

  local decoded_pub, dec_err = key_pb.decode_public_key(public_key_proto)
  if not decoded_pub then
    return nil, dec_err
  end
  if decoded_pub.type ~= key_pb.KEY_TYPE.RSA then
    return nil, "expected rsa key type in spec vector"
  end

  local reencoded_pub, enc_err = key_pb.encode_public_key(decoded_pub.type, decoded_pub.data)
  if not reencoded_pub then
    return nil, enc_err
  end
  if reencoded_pub ~= public_key_proto then
    return nil, "rsa public key protobuf roundtrip mismatch"
  end

  local pid, pid_err = peerid.from_public_key_proto(public_key_proto, "rsa")
  if not pid then
    return nil, pid_err
  end
  if pid.type ~= "rsa" then
    return nil, "expected rsa peer id type"
  end
  if pid.id ~= "QmaeANgBs1DTSxWSrPPtobgQuxW8XTfsS4ydbK4rCHzqxG" then
    return nil, "unexpected rsa peer id from spec vector"
  end

  local parsed, parse_err = peerid.parse(pid.id)
  if not parsed then
    return nil, parse_err
  end
  if parsed.id ~= pid.id then
    return nil, "rsa peer id should parse roundtrip"
  end

  return true
end

return {
  name = "spec rsa key vectors",
  run = run,
}
