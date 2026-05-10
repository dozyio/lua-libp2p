local hex = require("tests.helpers.hex")
local key_pb = require("lua_libp2p.crypto.key_pb")

local function run()
  local ed25519_public = hex.decode("1ed1e8fae2c4a144b8be8fd4b47bf3d3b34b871c3cacf6010f0e42d474fce27e")
  local expected_public_proto = hex.decode("080112201ed1e8fae2c4a144b8be8fd4b47bf3d3b34b871c3cacf6010f0e42d474fce27e")

  local encoded_pub, enc_pub_err = key_pb.encode_public_key("ed25519", ed25519_public)
  if not encoded_pub then
    return nil, enc_pub_err
  end
  if encoded_pub ~= expected_public_proto then
    return nil, "public key protobuf encoding mismatch"
  end

  local decoded_pub, dec_pub_err = key_pb.decode_public_key(encoded_pub)
  if not decoded_pub then
    return nil, dec_pub_err
  end
  if decoded_pub.type ~= key_pb.KEY_TYPE.Ed25519 then
    return nil, "decoded public key type mismatch"
  end
  if decoded_pub.data ~= ed25519_public then
    return nil, "decoded public key payload mismatch"
  end

  local ed25519_private = hex.decode(
    "7e0830617c4a7de83925dfb2694556b12936c477a0e1feb2e148ec9da60fee7d1ed1e8fae2c4a144b8be8fd4b47bf3d3b34b871c3cacf6010f0e42d474fce27e"
  )
  local expected_private_proto = hex.decode(
    "080112407e0830617c4a7de83925dfb2694556b12936c477a0e1feb2e148ec9da60fee7d1ed1e8fae2c4a144b8be8fd4b47bf3d3b34b871c3cacf6010f0e42d474fce27e"
  )

  local encoded_priv, enc_priv_err = key_pb.encode_private_key(key_pb.KEY_TYPE.Ed25519, ed25519_private)
  if not encoded_priv then
    return nil, enc_priv_err
  end
  if encoded_priv ~= expected_private_proto then
    return nil, "private key protobuf encoding mismatch"
  end

  local decoded_priv, dec_priv_err = key_pb.decode_private_key(encoded_priv)
  if not decoded_priv then
    return nil, dec_priv_err
  end
  if decoded_priv.type_name ~= "ed25519" then
    return nil, "decoded private key type name mismatch"
  end
  if decoded_priv.data ~= ed25519_private then
    return nil, "decoded private key payload mismatch"
  end

  local _, unknown_field_err = key_pb.decode_public_key(string.char(0x08, 0x01, 0x18, 0x00))
  if not unknown_field_err then
    return nil, "expected unknown field decode failure"
  end

  return true
end

return {
  name = "key protobuf deterministic encoding",
  run = run,
}
