local error_mod = require("lua_libp2p.error")
local varint = require("lua_libp2p.multiformats.varint")

local ok, sodium = pcall(require, "luasodium")
if not ok then
  error("luasodium is required for multihash support")
end

local M = {}

M.IDENTITY = 0x00
M.SHA2_256 = 0x12

function M.encode(code, digest)
  if type(code) ~= "number" then
    return nil, error_mod.new("input", "multihash code must be a number")
  end
  if type(digest) ~= "string" then
    return nil, error_mod.new("input", "multihash digest must be bytes")
  end

  local code_varint, code_err = varint.encode_u64(code)
  if not code_varint then
    return nil, code_err
  end
  local len_varint, len_err = varint.encode_u64(#digest)
  if not len_varint then
    return nil, len_err
  end
  return code_varint .. len_varint .. digest
end

function M.decode(multihash_bytes)
  if type(multihash_bytes) ~= "string" or multihash_bytes == "" then
    return nil, error_mod.new("input", "multihash bytes must be non-empty")
  end

  local code, offset_or_err = varint.decode_u64(multihash_bytes, 1)
  if not code then
    return nil, offset_or_err
  end

  local length, payload_offset_or_err = varint.decode_u64(multihash_bytes, offset_or_err)
  if not length then
    return nil, payload_offset_or_err
  end

  local digest = multihash_bytes:sub(payload_offset_or_err)
  if #digest ~= length then
    return nil, error_mod.new("decode", "multihash length mismatch", { expected = length, actual = #digest })
  end

  return {
    code = code,
    length = length,
    digest = digest,
  }
end

function M.identity(payload)
  return M.encode(M.IDENTITY, payload)
end

function M.sha2_256(payload)
  return M.encode(M.SHA2_256, sodium.crypto_hash_sha256(payload))
end

return M
