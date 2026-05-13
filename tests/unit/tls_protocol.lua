local ed25519 = require("lua_libp2p.crypto.ed25519")
local tls = require("lua_libp2p.connection_encrypter_tls.protocol")

local function run()
  if tls.PROTOCOL_ID ~= "/tls/1.0.0" then
    return nil, "unexpected tls protocol id"
  end

  local identity = assert(ed25519.generate_keypair())
  local _, _, err = tls.handshake_outbound({}, {
    identity_keypair = identity,
  })
  if not err or err.kind ~= "unsupported" then
    return nil, "expected unsupported error for non-cqueues raw connection"
  end

  return true
end

return {
  name = "tls protocol basic behavior",
  run = run,
}
