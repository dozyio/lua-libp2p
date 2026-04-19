local ed25519 = require("lua_libp2p.crypto.ed25519")
local peerid = require("lua_libp2p.peerid")
local peer_record = require("lua_libp2p.record.peer_record")

local function run()
  local keypair, key_err = ed25519.generate_keypair()
  if not keypair then
    return nil, key_err
  end

  local pid, pid_err = peerid.from_ed25519_public_key(keypair.public_key)
  if not pid then
    return nil, pid_err
  end

  local rec, rec_err = peer_record.make_record(pid.id, 42, {
    "/ip4/127.0.0.1/tcp/4001",
    "/dns4/example.com/tcp/443/tls/ws",
  })
  if not rec then
    return nil, rec_err
  end

  local payload, payload_err = peer_record.encode(rec)
  if not payload then
    return nil, payload_err
  end

  local decoded, dec_err = peer_record.decode(payload)
  if not decoded then
    return nil, dec_err
  end
  if decoded.seq ~= 42 then
    return nil, "peer record seq mismatch"
  end
  if #decoded.addresses ~= 2 then
    return nil, "peer record address count mismatch"
  end

  local texts, texts_err = peer_record.addresses_as_text(decoded)
  if not texts then
    return nil, texts_err
  end
  if texts[1] ~= "/ip4/127.0.0.1/tcp/4001" then
    return nil, "peer record address decode mismatch"
  end

  local env, env_err = peer_record.sign_ed25519(keypair, rec)
  if not env then
    return nil, env_err
  end

  local verified, verify_err = peer_record.verify_signed_envelope(env, { expected_peer_id = pid.id })
  if not verified then
    return nil, verify_err
  end
  if verified.record.peer_id ~= pid.bytes then
    return nil, "verified peer record id mismatch"
  end

  local _, mismatch_err = peer_record.verify_signed_envelope(env, {
    expected_peer_id = "12D3KooWCryG7Mon9orvQxcS1rYZjotPgpwoJNHHKcLLfE4Hf5mV",
  })
  if not mismatch_err then
    return nil, "expected peer mismatch verification failure"
  end

  return true
end

return {
  name = "peer record encode and envelope verify",
  run = run,
}
