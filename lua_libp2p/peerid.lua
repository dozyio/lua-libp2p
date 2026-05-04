--- Peer ID parsing and derivation helpers.
-- @module lua_libp2p.peerid
local error_mod = require("lua_libp2p.error")
local key_pb = require("lua_libp2p.crypto.key_pb")
local base58btc = require("lua_libp2p.multiformats.base58btc")
local multihash = require("lua_libp2p.multiformats.multihash")
local cid = require("lua_libp2p.multiformats.cid")
local mime = require("mime")

local M = {}

local LIBP2P_KEY_CODEC = 0x72

local ok_pkey, ossl_pkey = pcall(require, "openssl.pkey")

local function der_public_key_to_pem(der)
  return "-----BEGIN PUBLIC KEY-----\n" .. mime.b64(der) .. "\n-----END PUBLIC KEY-----\n"
end

local function marshal_ed25519_public_key(raw_public_key)
  if type(raw_public_key) ~= "string" or #raw_public_key ~= 32 then
    return nil, error_mod.new("input", "ed25519 public key must be 32 bytes")
  end
  return key_pb.encode_public_key(key_pb.KEY_TYPE.Ed25519, raw_public_key)
end

local function marshal_ecdsa_public_key(public_key_der)
  if type(public_key_der) ~= "string" or public_key_der == "" then
    return nil, error_mod.new("input", "ecdsa public key must be DER bytes")
  end
  if not ok_pkey then
    return nil, error_mod.new("unsupported", "luaossl is required for ecdsa public key validation")
  end
  local ok, parsed_or_err = pcall(ossl_pkey.new, der_public_key_to_pem(public_key_der))
  if not ok or not parsed_or_err then
    return nil, error_mod.new("input", "ecdsa public key must be DER-encoded SubjectPublicKeyInfo", { cause = parsed_or_err })
  end
  return key_pb.encode_public_key(key_pb.KEY_TYPE.ECDSA, public_key_der)
end

local function marshal_secp256k1_public_key(public_key)
  if type(public_key) ~= "string" then
    return nil, error_mod.new("input", "secp256k1 public key must be bytes")
  end
  local first = public_key:byte(1)
  if not ((#public_key == 33 and (first == 0x02 or first == 0x03)) or (#public_key == 65 and first == 0x04)) then
    return nil, error_mod.new("input", "secp256k1 public key must be compressed or uncompressed EC point bytes")
  end
  return key_pb.encode_public_key(key_pb.KEY_TYPE.Secp256k1, public_key)
end

local function build_peer_id_record(multihash_bytes, key_type, public_key, public_key_proto)
  local cid_text, cid_err = cid.encode_v1(LIBP2P_KEY_CODEC, multihash_bytes, "base32")
  if not cid_text then
    return nil, cid_err
  end

  return {
    type = key_type,
    bytes = multihash_bytes,
    id = base58btc.encode(multihash_bytes),
    cid = cid_text,
    public_key = public_key,
    public_key_proto = public_key_proto,
  }
end

--- Build a peer id from protobuf-encoded public key bytes.
function M.from_public_key_proto(public_key_proto, key_type)
  if type(public_key_proto) ~= "string" then
    return nil, error_mod.new("input", "public key proto must be bytes")
  end

  local decoded_key, key_err = key_pb.decode_public_key(public_key_proto)
  if not decoded_key then
    return nil, key_err
  end

  local multihash_bytes, mh_err
  if #public_key_proto <= 42 then
    multihash_bytes, mh_err = multihash.identity(public_key_proto)
  else
    multihash_bytes, mh_err = multihash.sha2_256(public_key_proto)
  end
  if not multihash_bytes then
    return nil, mh_err
  end

  local detected_key_type = decoded_key.type_name or key_type
  local detected_public_key
  if decoded_key.type == key_pb.KEY_TYPE.Ed25519 and #decoded_key.data == 32 then
    detected_key_type = "ed25519"
    detected_public_key = decoded_key.data
  end

  return build_peer_id_record(multihash_bytes, detected_key_type, detected_public_key, public_key_proto)
end

--- Build a peer id from a raw ed25519 public key.
function M.from_ed25519_public_key(raw_public_key)
  local public_key_proto, marshal_err = marshal_ed25519_public_key(raw_public_key)
  if not public_key_proto then
    return nil, marshal_err
  end
  return M.from_public_key_proto(public_key_proto, "ed25519")
end

--- Build a peer id from DER-encoded ECDSA public key bytes.
function M.from_ecdsa_public_key(public_key_der)
  local public_key_proto, marshal_err = marshal_ecdsa_public_key(public_key_der)
  if not public_key_proto then
    return nil, marshal_err
  end
  return M.from_public_key_proto(public_key_proto, "ecdsa")
end

--- Alias for @{from_ecdsa_public_key}.
function M.from_ecdsa_public_key_der(public_key_der)
  return M.from_ecdsa_public_key(public_key_der)
end

--- Build a peer id from secp256k1 public key bytes.
function M.from_secp256k1_public_key(public_key)
  local public_key_proto, marshal_err = marshal_secp256k1_public_key(public_key)
  if not public_key_proto then
    return nil, marshal_err
  end
  return M.from_public_key_proto(public_key_proto, "secp256k1")
end

--- Encode peer id bytes as base58btc text.
function M.to_base58(peer_id_bytes)
  return base58btc.encode(peer_id_bytes)
end

--- Encode peer id bytes as CIDv1 (`libp2p-key`).
function M.to_cid(peer_id_bytes)
  return cid.encode_v1(LIBP2P_KEY_CODEC, peer_id_bytes, "base32")
end

--- Parse raw peer id multihash bytes.
function M.from_bytes(peer_id_bytes)
  if type(peer_id_bytes) ~= "string" or peer_id_bytes == "" then
    return nil, error_mod.new("input", "peer id bytes must be non-empty")
  end
  local mh, mh_err = multihash.decode(peer_id_bytes)
  if not mh then
    return nil, mh_err
  end

  local key_type
  local public_key
  local public_key_proto
  if mh.code == multihash.IDENTITY then
    public_key_proto = mh.digest
    local decoded_key = key_pb.decode_public_key(public_key_proto)
    if decoded_key then
      key_type = decoded_key.type_name
      if decoded_key.type == key_pb.KEY_TYPE.Ed25519 and #decoded_key.data == 32 then
        key_type = "ed25519"
        public_key = decoded_key.data
      end
    end
  end

  return build_peer_id_record(peer_id_bytes, key_type, public_key, public_key_proto)
end

--- Parse a peer id string (base58 or CIDv1).
function M.parse(text)
  if type(text) ~= "string" or text == "" then
    return nil, error_mod.new("input", "peer id text must be non-empty")
  end

  local multihash_bytes
  if text:sub(1, 1) == "1" or text:sub(1, 2) == "Qm" then
    local decoded, decode_err = base58btc.decode(text)
    if not decoded then
      return nil, decode_err
    end
    multihash_bytes = decoded
  else
    local decoded_cid, cid_err = cid.decode_v1(text)
    if not decoded_cid then
      return nil, cid_err
    end
    if decoded_cid.codec ~= LIBP2P_KEY_CODEC then
      return nil, error_mod.new("decode", "CID codec must be libp2p-key", { codec = decoded_cid.codec })
    end
    multihash_bytes = decoded_cid.multihash
  end

  local mh, mh_err = multihash.decode(multihash_bytes)
  if not mh then
    return nil, mh_err
  end

  local key_type
  local public_key
  local public_key_proto
  if mh.code == multihash.IDENTITY then
    public_key_proto = mh.digest
    local decoded_key = key_pb.decode_public_key(public_key_proto)
    if decoded_key then
      key_type = decoded_key.type_name
      if decoded_key.type == key_pb.KEY_TYPE.Ed25519 and #decoded_key.data == 32 then
        key_type = "ed25519"
        public_key = decoded_key.data
      end
    end
  end

  return build_peer_id_record(multihash_bytes, key_type, public_key, public_key_proto)
end

--- Alias for @{parse}.
function M.from_text(text)
  return M.parse(text)
end

return M
