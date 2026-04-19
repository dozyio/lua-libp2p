local hex = require("tests.helpers.hex")
local varint = require("lua_libp2p.multiformats.varint")
local base58btc = require("lua_libp2p.multiformats.base58btc")
local base32 = require("lua_libp2p.multiformats.base32")
local multihash = require("lua_libp2p.multiformats.multihash")
local cid = require("lua_libp2p.multiformats.cid")

local function run()
  local values = { 0, 1, 127, 128, 300, 16384 }
  for _, n in ipairs(values) do
    local enc, enc_err = varint.encode_u64(n)
    if not enc then
      return nil, enc_err
    end
    local dec, next_i = varint.decode_u64(enc, 1)
    if dec ~= n then
      return nil, "varint roundtrip mismatch"
    end
    if next_i ~= #enc + 1 then
      return nil, "varint decode did not consume full input"
    end
  end

  local _, min_err = varint.decode_u64(string.char(0x80, 0x00), 1)
  if not min_err then
    return nil, "expected non-minimal varint decoding error"
  end

  local b58 = base58btc.encode("hello world")
  if b58 ~= "StV1DL6CwTryKyV" then
    return nil, "unexpected base58 encoding"
  end
  local b58_back, b58_err = base58btc.decode(b58)
  if not b58_back then
    return nil, b58_err
  end
  if b58_back ~= "hello world" then
    return nil, "base58 decode mismatch"
  end

  local leading = "\0\0abc"
  local leading_rt, lead_err = base58btc.decode(base58btc.encode(leading))
  if not leading_rt then
    return nil, lead_err
  end
  if leading_rt ~= leading then
    return nil, "base58 leading-zero roundtrip mismatch"
  end

  local b32 = base32.encode_nopad("foobar")
  if b32 ~= "mzxw6ytboi" then
    return nil, "unexpected base32 encoding"
  end
  local b32_back, b32_err = base32.decode_nopad(b32)
  if not b32_back then
    return nil, b32_err
  end
  if b32_back ~= "foobar" then
    return nil, "base32 decode mismatch"
  end

  local id_mh, id_err = multihash.identity("abc")
  if not id_mh then
    return nil, id_err
  end
  local id_dec, id_dec_err = multihash.decode(id_mh)
  if not id_dec then
    return nil, id_dec_err
  end
  if id_dec.code ~= multihash.IDENTITY or id_dec.digest ~= "abc" then
    return nil, "identity multihash mismatch"
  end

  local sha_mh, sha_err = multihash.sha2_256("abc")
  if not sha_mh then
    return nil, sha_err
  end
  local expected_sha = hex.decode("1220ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
  if sha_mh ~= expected_sha then
    return nil, "sha2-256 multihash mismatch"
  end

  local cid_text, cid_err = cid.encode_v1(0x72, id_mh, "base32")
  if not cid_text then
    return nil, cid_err
  end
  if cid_text:sub(1, 1) ~= "b" then
    return nil, "cid should be base32 multibase"
  end

  local cid_dec, cid_dec_err = cid.decode_v1(cid_text)
  if not cid_dec then
    return nil, cid_dec_err
  end
  if cid_dec.version ~= 1 or cid_dec.codec ~= 0x72 or cid_dec.multihash ~= id_mh then
    return nil, "cid roundtrip mismatch"
  end

  return true
end

return {
  name = "multiformats roundtrip and vectors",
  run = run,
}
