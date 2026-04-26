local key_pb = require("lua_libp2p.crypto.key_pb")
local multihash = require("lua_libp2p.multiformats.multihash")
local peerid = require("lua_libp2p.peerid")
local mime = require("mime")

local function public_pem_to_der(pem)
  local body = pem:gsub("%-%-%-%-%-BEGIN PUBLIC KEY%-%-%-%-%-", "")
    :gsub("%-%-%-%-%-END PUBLIC KEY%-%-%-%-%-", "")
    :gsub("%s+", "")
  return mime.unb64(body)
end

local function run()
  local ok_pkey, pkey = pcall(require, "openssl.pkey")
  if not ok_pkey then
    return nil, "luaossl is required for ecdsa peer id test"
  end

  local private_key, key_err = pkey.new({ type = "EC", curve = "prime256v1" })
  if not private_key then
    return nil, key_err
  end

  local public_der = public_pem_to_der(private_key:toPEM("public"))
  if type(public_der) ~= "string" or public_der == "" then
    return nil, "failed to export generated ecdsa public key"
  end

  local pid, pid_err = peerid.from_ecdsa_public_key_der(public_der)
  if not pid then
    return nil, pid_err
  end
  if pid.type ~= "ecdsa" then
    return nil, "expected ecdsa peer id type"
  end
  if pid.public_key ~= nil then
    return nil, "expected hashed ecdsa peer id to not inline public key"
  end

  local decoded_key, decode_err = key_pb.decode_public_key(pid.public_key_proto)
  if not decoded_key then
    return nil, decode_err
  end
  if decoded_key.type ~= key_pb.KEY_TYPE.ECDSA or decoded_key.data ~= public_der then
    return nil, "ecdsa public key proto mismatch"
  end

  local mh, mh_err = multihash.decode(pid.bytes)
  if not mh then
    return nil, mh_err
  end
  if mh.code ~= multihash.SHA2_256 or #mh.digest ~= 32 then
    return nil, "expected ecdsa peer id to use sha2-256 multihash"
  end

  local from_proto, proto_err = peerid.from_public_key_proto(pid.public_key_proto)
  if not from_proto then
    return nil, proto_err
  end
  if from_proto.id ~= pid.id or from_proto.type ~= "ecdsa" then
    return nil, "ecdsa peer id proto derivation mismatch"
  end

  local parsed, parse_err = peerid.parse(pid.id)
  if not parsed then
    return nil, parse_err
  end
  if parsed.bytes ~= pid.bytes then
    return nil, "parsed ecdsa peer id bytes mismatch"
  end

  local bad_pid, bad_err = peerid.from_ecdsa_public_key_der("not der")
  if bad_pid ~= nil or not bad_err then
    return nil, "expected invalid ecdsa DER to fail"
  end

  return true
end

return {
  name = "peerid from ecdsa public key",
  run = run,
}
