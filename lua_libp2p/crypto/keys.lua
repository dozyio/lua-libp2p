--- Key loading and signing helpers.
-- @module lua_libp2p.crypto.keys
local ed25519 = require("lua_libp2p.crypto.ed25519")
local error_mod = require("lua_libp2p.error")
local key_pb = require("lua_libp2p.crypto.key_pb")
local mime = require("mime")

local ok_ossl_pkey, ossl_pkey = pcall(require, "openssl.pkey")
local ok_ossl_digest, ossl_digest = pcall(require, "openssl.digest")

local M = {}

local function normalize_type(key_type)
  if key_type == key_pb.KEY_TYPE.Ed25519 or key_type == "ed25519" then
    return key_pb.KEY_TYPE.Ed25519, "ed25519"
  end
  if key_type == key_pb.KEY_TYPE.RSA or key_type == "rsa" then
    return key_pb.KEY_TYPE.RSA, "rsa"
  end
  if key_type == key_pb.KEY_TYPE.ECDSA or key_type == "ecdsa" then
    return key_pb.KEY_TYPE.ECDSA, "ecdsa"
  end
  if key_type == key_pb.KEY_TYPE.Secp256k1 or key_type == "secp256k1" then
    return key_pb.KEY_TYPE.Secp256k1, "secp256k1"
  end
  return nil, nil
end

local function der_public_key_to_pem(der)
  local b64 = mime.b64(der)
  local lines = {}
  for i = 1, #b64, 64 do
    lines[#lines + 1] = b64:sub(i, i + 63)
  end
  return "-----BEGIN PUBLIC KEY-----\n" .. table.concat(lines, "\n") .. "\n-----END PUBLIC KEY-----\n"
end

local function bytes_to_pem(der)
  local b64 = mime.b64(der)
  local lines = {}
  for i = 1, #b64, 64 do
    lines[#lines + 1] = b64:sub(i, i + 63)
  end
  return "-----BEGIN PUBLIC KEY-----\n" .. table.concat(lines, "\n") .. "\n-----END PUBLIC KEY-----\n"
end

local function secp256k1_public_key_to_pem(public_key)
  if type(public_key) ~= "string" then
    return nil, error_mod.new("input", "secp256k1 public key bytes are required")
  end
  if #public_key == 33 and (public_key:byte(1) == 0x02 or public_key:byte(1) == 0x03) then
    return bytes_to_pem(string.char(
      0x30, 0x36, 0x30, 0x10, 0x06, 0x07, 0x2a, 0x86,
      0x48, 0xce, 0x3d, 0x02, 0x01, 0x06, 0x05, 0x2b,
      0x81, 0x04, 0x00, 0x0a, 0x03, 0x22, 0x00
    ) .. public_key)
  end
  if #public_key == 65 and public_key:byte(1) == 0x04 then
    return bytes_to_pem(string.char(
      0x30, 0x56, 0x30, 0x10, 0x06, 0x07, 0x2a, 0x86,
      0x48, 0xce, 0x3d, 0x02, 0x01, 0x06, 0x05, 0x2b,
      0x81, 0x04, 0x00, 0x0a, 0x03, 0x42, 0x00
    ) .. public_key)
  end
  return nil, error_mod.new("input", "secp256k1 public key must be compressed or uncompressed EC point bytes")
end

local function decimal_to_bytes(decimal, width)
  local digits = tostring(decimal or "")
  if not digits:match("^%d+$") then
    return nil
  end
  if digits == "0" then
    return string.rep("\0", width or 1)
  end
  local out = {}
  while digits ~= "0" do
    local quotient = {}
    local carry = 0
    for i = 1, #digits do
      local n = carry * 10 + tonumber(digits:sub(i, i))
      local q = math.floor(n / 256)
      carry = n % 256
      if #quotient > 0 or q > 0 then
        quotient[#quotient + 1] = tostring(q)
      end
    end
    out[#out + 1] = string.char(carry)
    digits = #quotient > 0 and table.concat(quotient) or "0"
  end
  local bytes = table.concat(out):reverse()
  if width then
    if #bytes > width then
      return nil
    end
    bytes = string.rep("\0", width - #bytes) .. bytes
  end
  return bytes
end

local function pem_body_to_der(pem, label)
  local body = tostring(pem or "")
    :gsub("%-%-%-%-%-BEGIN " .. label .. "%-%-%-%-%-", "")
    :gsub("%-%-%-%-%-END " .. label .. "%-%-%-%-%-", "")
    :gsub("%s+", "")
  if body == "" then
    return nil
  end
  return mime.unb64(body)
end

local function public_key_bytes(public_key)
  if type(public_key) == "table" then
    return public_key.public_key or public_key.data
  end
  return public_key
end

local function private_key_pem(identity)
  if type(identity) == "table" then
    return identity.private_key_pem or identity.private_key
  end
  return identity
end

local function public_der_from_pkey(key)
  local public_pem = key:toPEM("public")
  return pem_body_to_der(public_pem, "PUBLIC KEY")
end

local function make_digest(message)
  local digest_ctx, digest_err = ossl_digest.new("sha256")
  if not digest_ctx then
    return nil, error_mod.new("io", "failed to create sha256 digest context", { cause = digest_err })
  end
  digest_ctx:update(message)
  return digest_ctx
end

local function verify_digest_signature(public_key_der, message, signature, label, pem_builder)
  if not ok_ossl_pkey or not ok_ossl_digest then
    return nil, error_mod.new("unsupported", "luaossl is required for " .. label .. " signature verification")
  end
  if type(public_key_der) ~= "string" or public_key_der == "" then
    return nil, error_mod.new("input", label .. " public key DER bytes are required")
  end
  if type(signature) ~= "string" or signature == "" then
    return nil, error_mod.new("input", label .. " signature bytes are required")
  end

  local pem, pem_err
  if pem_builder then
    pem, pem_err = pem_builder(public_key_der)
    if not pem then
      return nil, pem_err
    end
  else
    pem = der_public_key_to_pem(public_key_der)
  end
  local ok_key, pub_key_or_err = pcall(ossl_pkey.new, pem)
  if not ok_key or not pub_key_or_err then
    return nil, error_mod.new("input", "failed to parse " .. label .. " public key", { cause = pub_key_or_err })
  end

  local digest_ctx, digest_err = make_digest(message)
  if not digest_ctx then
    return nil, digest_err
  end

  local ok, verified = pcall(function()
    return pub_key_or_err:verify(signature, digest_ctx)
  end)
  if not ok then
    return nil, error_mod.new("io", label .. " signature verification failed", { cause = verified })
  end
  return verified == true
end

function M.verify_signature(public_key, message, signature, key_type)
  if type(message) ~= "string" then
    return nil, error_mod.new("input", "signature message must be bytes")
  end

  local kt = key_type
  if kt == nil and type(public_key) == "table" then
    kt = public_key.type
  end

  if kt == key_pb.KEY_TYPE.Ed25519 or kt == "ed25519" then
    return ed25519.verify({ public_key = public_key_bytes(public_key) }, message, signature)
  end
  if kt == key_pb.KEY_TYPE.RSA or kt == "rsa" then
    return verify_digest_signature(public_key_bytes(public_key), message, signature, "rsa")
  end
  if kt == key_pb.KEY_TYPE.ECDSA or kt == "ecdsa" then
    return verify_digest_signature(public_key_bytes(public_key), message, signature, "ecdsa")
  end
  if kt == key_pb.KEY_TYPE.Secp256k1 or kt == "secp256k1" then
    return verify_digest_signature(public_key_bytes(public_key), message, signature, "secp256k1", secp256k1_public_key_to_pem)
  end

  return nil, error_mod.new("unsupported", "unsupported public key type for signature verification", {
    received_key_type = kt,
    received_key_type_name = key_pb.type_name(kt),
    supported = { "ed25519", "rsa", "ecdsa", "secp256k1" },
  })
end

function M.public_key_proto(identity)
  if type(identity) ~= "table" then
    return nil, error_mod.new("input", "identity must be a table")
  end
  if type(identity.public_key_proto) == "string" then
    return identity.public_key_proto
  end

  local code, name = normalize_type(identity.type or identity.key_type)
  if not code then
    if type(identity.public_key) == "string" and #identity.public_key == 32 then
      code, name = key_pb.KEY_TYPE.Ed25519, "ed25519"
    elseif code ~= key_pb.KEY_TYPE.Secp256k1 then
      return nil, error_mod.new("input", "identity key type is required")
    end
  end

  local public_key = identity.public_key
  if type(public_key) ~= "string" or public_key == "" then
    return nil, error_mod.new("input", name .. " identity public key is required")
  end
  return key_pb.encode_public_key(code, public_key)
end

function M.peer_id(identity)
  local peerid = require("lua_libp2p.peerid")
  local proto, proto_err = M.public_key_proto(identity)
  if not proto then
    return nil, proto_err
  end
  return peerid.from_public_key_proto(proto)
end

--- Generate new identity keypair.
-- `opts.bits` (`number`) configures RSA key size (default `2048`).
-- `opts.curve` (`string`) configures ECDSA curve (default `prime256v1`).
-- @tparam[opt] string key_type `ed25519|rsa|ecdsa|secp256k1`.
-- @tparam[opt] table opts
-- @treturn table|nil keypair
-- @treturn[opt] table err
function M.generate_keypair(key_type, opts)
  local _, name = normalize_type(key_type or "ed25519")
  local options = opts or {}
  if name == "ed25519" then
    local kp, err = ed25519.generate_keypair()
    if not kp then
      return nil, err
    end
    kp.type = "ed25519"
    kp.public_key_proto = assert(key_pb.encode_public_key(key_pb.KEY_TYPE.Ed25519, kp.public_key))
    return kp
  end

  if not ok_ossl_pkey then
    return nil, error_mod.new("unsupported", "luaossl is required for " .. tostring(name) .. " key generation")
  end

  local pkey_opts
  if name == "rsa" then
    pkey_opts = { type = "RSA", bits = options.bits or 2048 }
  elseif name == "ecdsa" then
    pkey_opts = { type = "EC", curve = options.curve or "prime256v1" }
  elseif name == "secp256k1" then
    pkey_opts = { type = "EC", curve = "secp256k1" }
  else
    return nil, error_mod.new("unsupported", "unsupported key type for generation", { key_type = key_type })
  end

  local private_key, key_err = ossl_pkey.new(pkey_opts)
  if not private_key then
    return nil, error_mod.new("io", "failed to generate " .. name .. " key", { cause = key_err })
  end
  local public_der
  if name == "secp256k1" then
    local params = private_key:getParameters()
    public_der = params and decimal_to_bytes(params.pub_key)
  else
    public_der = public_der_from_pkey(private_key)
  end
  if not public_der then
    return nil, error_mod.new("io", "failed to export " .. name .. " public key")
  end
  local code = name == "rsa" and key_pb.KEY_TYPE.RSA or (name == "ecdsa" and key_pb.KEY_TYPE.ECDSA or key_pb.KEY_TYPE.Secp256k1)
  local public_proto, proto_err = key_pb.encode_public_key(code, public_der)
  if not public_proto then
    return nil, proto_err
  end
  return {
    type = name,
    public_key = public_der,
    private_key = private_key:toPEM("private"),
    private_key_pem = private_key:toPEM("private"),
    public_key_proto = public_proto,
  }
end

function M.sign(identity, message)
  if type(message) ~= "string" then
    return nil, error_mod.new("input", "signature message must be bytes")
  end
  local _, name = normalize_type(type(identity) == "table" and (identity.type or identity.key_type) or nil)
  if not name and type(identity) == "table" and type(identity.public_key) == "string" and #identity.public_key == 32 then
    name = "ed25519"
  end
  if name == "ed25519" then
    return ed25519.sign(identity, message)
  end
  if name ~= "rsa" and name ~= "ecdsa" and name ~= "secp256k1" then
    return nil, error_mod.new("unsupported", "unsupported key type for signing", { key_type = name })
  end
  if not ok_ossl_pkey or not ok_ossl_digest then
    return nil, error_mod.new("unsupported", "luaossl is required for " .. name .. " signing")
  end
  local pem = private_key_pem(identity)
  if type(pem) ~= "string" or pem == "" then
    return nil, error_mod.new("input", name .. " private key PEM is required")
  end
  local ok_key, key_or_err = pcall(ossl_pkey.new, pem)
  if not ok_key or not key_or_err then
    return nil, error_mod.new("input", "failed to parse " .. name .. " private key", { cause = key_or_err })
  end
  local digest_ctx, digest_err = make_digest(message)
  if not digest_ctx then
    return nil, digest_err
  end
  local ok, signature = pcall(function()
    return key_or_err:sign(digest_ctx)
  end)
  if not ok or not signature then
    return nil, error_mod.new("io", name .. " signing failed", { cause = signature })
  end
  return signature
end

return M
