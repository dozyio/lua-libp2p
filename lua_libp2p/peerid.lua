local error_mod = require("lua_libp2p.error")
local key_pb = require("lua_libp2p.crypto.key_pb")
local base58btc = require("lua_libp2p.multiformats.base58btc")
local multihash = require("lua_libp2p.multiformats.multihash")
local cid = require("lua_libp2p.multiformats.cid")

local M = {}

local LIBP2P_KEY_CODEC = 0x72

local function marshal_ed25519_public_key(raw_public_key)
  if type(raw_public_key) ~= "string" or #raw_public_key ~= 32 then
    return nil, error_mod.new("input", "ed25519 public key must be 32 bytes")
  end
  return key_pb.encode_public_key(key_pb.KEY_TYPE.Ed25519, raw_public_key)
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

function M.from_ed25519_public_key(raw_public_key)
  local public_key_proto, marshal_err = marshal_ed25519_public_key(raw_public_key)
  if not public_key_proto then
    return nil, marshal_err
  end
  return M.from_public_key_proto(public_key_proto, "ed25519")
end

function M.to_base58(peer_id_bytes)
  return base58btc.encode(peer_id_bytes)
end

function M.to_cid(peer_id_bytes)
  return cid.encode_v1(LIBP2P_KEY_CODEC, peer_id_bytes, "base32")
end

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

function M.from_text(text)
  return M.parse(text)
end

return M
