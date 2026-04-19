local hex = require("tests.helpers.hex")
local peerid = require("lua_libp2p.peerid")

local function run()
  local public_key = hex.decode("d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a")
  local result, err = peerid.from_ed25519_public_key(public_key)
  if not result then
    return nil, err
  end

  if result.type ~= "ed25519" then
    return nil, "unexpected peer id key type"
  end

  if #result.bytes ~= 38 then
    return nil, "unexpected peer id byte length"
  end

  if result.bytes:byte(1) ~= 0x00 then
    return nil, "expected identity multihash code"
  end

  if result.bytes:byte(2) ~= 0x24 then
    return nil, "expected identity digest length byte"
  end

  local expected = "12D3KooWQK1wnefoLrcVHbbnf5tLzbopUd3K3bFAoJpA7YJgL5pV"
  if result.id ~= expected then
    return nil, "unexpected peer id string"
  end

  if not result.public_key_proto or #result.public_key_proto ~= 36 then
    return nil, "expected serialized public key proto"
  end

  local from_proto, proto_err = peerid.from_public_key_proto(result.public_key_proto)
  if not from_proto then
    return nil, proto_err
  end
  if from_proto.id ~= expected then
    return nil, "peer id mismatch when deriving from public key proto"
  end

  local cid = peerid.to_cid(result.bytes)
  if type(cid) ~= "string" or cid:sub(1, 1) ~= "b" then
    return nil, "expected CIDv1 base32 peer id"
  end

  local parsed_legacy, legacy_err = peerid.parse(expected)
  if not parsed_legacy then
    return nil, legacy_err
  end
  if parsed_legacy.id ~= expected then
    return nil, "legacy parse should round-trip"
  end

  local parsed_cid, cid_err = peerid.parse(cid)
  if not parsed_cid then
    return nil, cid_err
  end
  if parsed_cid.id ~= expected then
    return nil, "CID parse should decode same peer id"
  end

  local cid_roundtrip = peerid.to_cid(parsed_cid.bytes)
  if cid_roundtrip ~= cid then
    return nil, "CID roundtrip mismatch"
  end

  return true
end

return {
  name = "peerid from ed25519 public key",
  run = run,
}
