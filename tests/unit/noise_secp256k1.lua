local keys = require("lua_libp2p.crypto.keys")
local key_pb = require("lua_libp2p.crypto.key_pb")
local noise = require("lua_libp2p.security.noise")

local function run()
  local identity, key_err = keys.generate_keypair("secp256k1")
  if not identity then
    return nil, key_err
  end

  local static_pub = string.rep("\x33", 32)
  local signature, sig_err = keys.sign(identity, noise.SIG_PREFIX .. static_pub)
  if not signature then
    return nil, sig_err
  end

  local public_proto, proto_err = key_pb.encode_public_key(key_pb.KEY_TYPE.Secp256k1, identity.public_key)
  if not public_proto then
    return nil, proto_err
  end

  local verified, verify_err = noise.verify_handshake_payload({ identity_key = public_proto, identity_sig = signature }, static_pub)
  if not verified then
    return nil, verify_err
  end
  if verified.peer_id.type ~= "secp256k1" then
    return nil, "expected secp256k1 peer id after noise payload verification"
  end

  local bad_sig = string.char((signature:byte(1) ~ 0x01)) .. signature:sub(2)
  local _, bad_err = noise.verify_handshake_payload({ identity_key = public_proto, identity_sig = bad_sig }, static_pub)
  if not bad_err then
    return nil, "expected secp256k1 signature verification failure"
  end

  return true
end

return {
  name = "noise secp256k1 identity verification",
  run = run,
}
