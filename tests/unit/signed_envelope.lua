local ed25519 = require("lua_libp2p.crypto.ed25519")
local signed_envelope = require("lua_libp2p.record.signed_envelope")

local function run()
  local keypair, key_err = ed25519.generate_keypair()
  if not keypair then
    return nil, key_err
  end

  local domain = "libp2p-routing-record"
  local payload_type = "/test/payload"
  local payload = "hello-envelope"

  local envelope, env_err = signed_envelope.sign_ed25519(keypair, domain, payload_type, payload)
  if not envelope then
    return nil, env_err
  end

  local encoded, enc_err = signed_envelope.encode(envelope)
  if not encoded then
    return nil, enc_err
  end
  local decoded, dec_err = signed_envelope.decode(encoded)
  if not decoded then
    return nil, dec_err
  end

  local verified, verify_err = signed_envelope.verify(decoded, domain)
  if not verified then
    return nil, verify_err
  end
  if verified.payload ~= payload then
    return nil, "signed envelope payload mismatch"
  end

  local _, wrong_domain_err = signed_envelope.verify(decoded, "wrong-domain")
  if not wrong_domain_err then
    return nil, "expected verification failure on wrong domain"
  end

  local tampered = {
    public_key = decoded.public_key,
    payload_type = decoded.payload_type,
    payload = decoded.payload .. "!",
    signature = decoded.signature,
  }
  local _, tampered_err = signed_envelope.verify(tampered, domain)
  if not tampered_err then
    return nil, "expected verification failure on tampered payload"
  end

  return true
end

return {
  name = "signed envelope sign/verify",
  run = run,
}
