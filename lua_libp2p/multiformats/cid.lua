--- CIDv1 helpers.
local error_mod = require("lua_libp2p.error")
local varint = require("lua_libp2p.multiformats.varint")
local base32 = require("lua_libp2p.multiformats.base32")

local M = {}

M.VERSION_1 = 1
M.MULTIBASE_BASE32_LOWER = "b"

--- codec integer
--- multihash_bytes string
--- multibase? string
--- string|nil cid
--- table|nil err
function M.encode_v1(codec, multihash_bytes, multibase)
  if type(codec) ~= "number" then
    return nil, error_mod.new("input", "cid codec must be a number")
  end
  if type(multihash_bytes) ~= "string" or multihash_bytes == "" then
    return nil, error_mod.new("input", "cid multihash must be non-empty")
  end

  local mb = multibase or "base32"
  if mb ~= "base32" then
    return nil, error_mod.new("input", "only base32 multibase is supported")
  end

  local version, version_err = varint.encode_u64(M.VERSION_1)
  if not version then
    return nil, version_err
  end
  local codec_varint, codec_err = varint.encode_u64(codec)
  if not codec_varint then
    return nil, codec_err
  end

  local binary = version .. codec_varint .. multihash_bytes
  return M.MULTIBASE_BASE32_LOWER .. base32.encode_nopad(binary)
end

--- text string
--- table|nil cid
--- table|nil err
function M.decode_v1(text)
  if type(text) ~= "string" or text == "" then
    return nil, error_mod.new("input", "cid text must be non-empty")
  end

  local prefix = text:sub(1, 1)
  if prefix ~= "b" and prefix ~= "B" then
    return nil, error_mod.new("decode", "unsupported multibase prefix", { prefix = prefix })
  end

  local binary, b32_err = base32.decode_nopad(text:sub(2))
  if not binary then
    return nil, b32_err
  end

  local version, after_version_or_err = varint.decode_u64(binary, 1)
  if not version then
    return nil, after_version_or_err
  end
  if version ~= M.VERSION_1 then
    return nil, error_mod.new("decode", "unsupported CID version", { version = version })
  end

  local codec, after_codec_or_err = varint.decode_u64(binary, after_version_or_err)
  if not codec then
    return nil, after_codec_or_err
  end

  local multihash_bytes = binary:sub(after_codec_or_err)
  if multihash_bytes == "" then
    return nil, error_mod.new("decode", "cid has empty multihash")
  end

  return {
    version = version,
    codec = codec,
    multihash = multihash_bytes,
    multibase = "base32",
  }
end

return M
