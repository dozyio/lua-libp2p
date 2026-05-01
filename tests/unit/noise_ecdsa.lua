local key_pb = require("lua_libp2p.crypto.key_pb")
local noise = require("lua_libp2p.connection_encrypter_noise.protocol")
local mime = require("mime")

local function public_pem_to_der(pem)
  local body = pem:gsub("%-%-%-%-%-BEGIN PUBLIC KEY%-%-%-%-%-", "")
    :gsub("%-%-%-%-%-END PUBLIC KEY%-%-%-%-%-", "")
    :gsub("%s+", "")
  return mime.unb64(body)
end

local function run()
  local ok_pkey, pkey = pcall(require, "openssl.pkey")
  local ok_digest, digest = pcall(require, "openssl.digest")
  if not ok_pkey or not ok_digest then
    return nil, "luaossl is required for ecdsa noise test"
  end

  local private_key, key_err = pkey.new({ type = "EC", curve = "prime256v1" })
  if not private_key then
    return nil, key_err
  end

  local public_der = public_pem_to_der(private_key:toPEM("public"))
  if type(public_der) ~= "string" or public_der == "" then
    return nil, "failed to export generated ecdsa public key"
  end

  local public_proto, proto_err = key_pb.encode_public_key(key_pb.KEY_TYPE.ECDSA, public_der)
  if not public_proto then
    return nil, proto_err
  end

  local static_pub = string.rep("\x22", 32)
  local signature_message = noise.SIG_PREFIX .. static_pub
  local sign_ctx, sign_ctx_err = digest.new("sha256")
  if not sign_ctx then
    return nil, sign_ctx_err
  end
  sign_ctx:update(signature_message)
  local signature, sign_err = private_key:sign(sign_ctx)
  if not signature then
    return nil, sign_err
  end

  local verified, verify_err = noise.verify_handshake_payload({
    identity_key = public_proto,
    identity_sig = signature,
  }, static_pub)
  if not verified then
    return nil, verify_err
  end
  if verified.peer_id.type ~= "ecdsa" then
    return nil, "expected ecdsa peer id after noise payload verification"
  end

  local bad_sig = string.char((signature:byte(1) ~ 0x01)) .. signature:sub(2)
  local _, bad_err = noise.verify_handshake_payload({
    identity_key = public_proto,
    identity_sig = bad_sig,
  }, static_pub)
  if not bad_err then
    return nil, "expected ecdsa signature verification failure"
  end

  return true
end

return {
  name = "noise ecdsa identity verification",
  run = run,
}
