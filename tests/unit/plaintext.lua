local hex = require("tests.helpers.hex")
local keys = require("lua_libp2p.crypto.keys")
local plaintext = require("lua_libp2p.connection_encrypter_plaintext.protocol")
local peerid = require("lua_libp2p.peerid")

local function new_scripted_conn(incoming)
  local conn = {
    _in = incoming or "",
    _out = "",
  }

  function conn:read(n)
    local want = n or #self._in
    if #self._in < want then
      return nil, "unexpected EOF"
    end
    local out = self._in:sub(1, want)
    self._in = self._in:sub(want + 1)
    return out
  end

  function conn:write(payload)
    self._out = self._out .. payload
    return true
  end

  function conn:writes()
    return self._out
  end

  return conn
end

local function run()
  local pub_a = hex.decode("1ed1e8fae2c4a144b8be8fd4b47bf3d3b34b871c3cacf6010f0e42d474fce27e")
  local pub_b = hex.decode("3d4017c3e843895a92b70aa74d1b7ebc9c982ccf2ec4968cc0cd55f12af4660c")

  local ex_a, ex_a_err = plaintext.make_exchange_from_ed25519_public_key(pub_a)
  if not ex_a then
    return nil, ex_a_err
  end
  local ex_b, ex_b_err = plaintext.make_exchange_from_ed25519_public_key(pub_b)
  if not ex_b then
    return nil, ex_b_err
  end

  local payload, payload_err = plaintext.encode_exchange(ex_a)
  if not payload then
    return nil, payload_err
  end
  local decoded, decode_err = plaintext.decode_exchange(payload)
  if not decoded then
    return nil, decode_err
  end
  if decoded.id ~= ex_a.id or decoded.pubkey ~= ex_a.pubkey then
    return nil, "plaintext exchange encode/decode mismatch"
  end

  local pid_a = peerid.to_base58(ex_a.id)
  local verified, verify_err = plaintext.verify_exchange(decoded, pid_a)
  if not verified then
    return nil, verify_err
  end
  if verified.peer_id.id ~= pid_a then
    return nil, "verified peer id mismatch"
  end

  local _, mismatch_err = plaintext.verify_exchange(decoded, ex_b.id)
  if not mismatch_err then
    return nil, "expected mismatch verification failure"
  end

  local tampered = {
    id = ex_b.id,
    pubkey = ex_a.pubkey,
  }
  local _, tampered_err = plaintext.verify_exchange(tampered)
  if not tampered_err then
    return nil, "expected tampered exchange verification failure"
  end

  for _, key_type in ipairs({ "rsa", "ecdsa", "secp256k1" }) do
    local identity, identity_err = keys.generate_keypair(key_type)
    if not identity then
      return nil, identity_err
    end
    local ex, ex_err = plaintext.make_exchange_from_identity(identity)
    if not ex then
      return nil, ex_err
    end
    local verified_key, verify_key_err = plaintext.verify_exchange(ex)
    if not verified_key then
      return nil, verify_key_err
    end
    if verified_key.key_type ~= key_type then
      return nil, "expected plaintext exchange key type " .. key_type
    end
  end

  local writer = new_scripted_conn("")
  local ok, write_err = plaintext.write_exchange(writer, ex_a)
  if not ok then
    return nil, write_err
  end
  local reader = new_scripted_conn(writer:writes())
  local read_back, read_err = plaintext.read_exchange(reader)
  if not read_back then
    return nil, read_err
  end
  if read_back.id ~= ex_a.id or read_back.pubkey ~= ex_a.pubkey then
    return nil, "plaintext framed exchange roundtrip mismatch"
  end

  return true
end

return {
  name = "plaintext exchange encode/verify",
  run = run,
}
