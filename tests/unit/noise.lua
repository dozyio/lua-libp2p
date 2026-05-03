local ed25519 = require("lua_libp2p.crypto.ed25519")
local noise = require("lua_libp2p.connection_encrypter_noise.protocol")

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
  local local_identity = assert(ed25519.generate_keypair())
  local remote_identity = assert(ed25519.generate_keypair())

  local local_static = noise.new_static_keypair()
  local remote_static = noise.new_static_keypair()

  local payload = assert(noise.make_handshake_payload(remote_identity, remote_static.public_key, {
    stream_muxers = { "/yamux/1.0.0" },
  }))
  local encoded = assert(noise.encode_handshake_payload(payload))
  local decoded = assert(noise.decode_handshake_payload(encoded))

  local verified = assert(noise.verify_handshake_payload(decoded, remote_static.public_key))
  if not verified.peer_id or type(verified.peer_id.id) ~= "string" then
    return nil, "noise payload verify did not produce peer id"
  end
  if not verified.extensions or verified.extensions.stream_muxers[1] ~= "/yamux/1.0.0" then
    return nil, "noise extensions mismatch"
  end

  local local_payload = assert(noise.make_handshake_payload(local_identity, local_static.public_key))
  local local_encoded = assert(noise.encode_handshake_payload(local_payload))

  local writer = new_scripted_conn("")
  assert(noise.write_message(writer, local_encoded))

  local reader = new_scripted_conn(writer:writes())
  local read_back = assert(noise.read_message(reader))
  if read_back ~= local_encoded then
    return nil, "noise wire framing mismatch"
  end

  local bad = {
    identity_key = decoded.identity_key,
    identity_sig = decoded.identity_sig,
    extensions = decoded.extensions,
  }
  local _, bad_err = noise.verify_handshake_payload(bad, local_static.public_key)
  if not bad_err then
    return nil, "expected noise signature verification failure with wrong static key"
  end

  return true
end

return {
  name = "noise payload and wire primitives",
  run = run,
}
