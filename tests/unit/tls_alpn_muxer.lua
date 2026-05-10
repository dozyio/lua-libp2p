local cqueues = require("cqueues")
local socket = require("cqueues.socket")
local ed25519 = require("lua_libp2p.crypto.ed25519")
local tls = require("lua_libp2p.connection_encrypter_tls.protocol")

local YAMUX = "/yamux/1.0.0"

local function run()
  local a, b = assert(socket.pair())
  local server_identity = assert(ed25519.generate_keypair())
  local client_identity = assert(ed25519.generate_keypair())
  local cq = cqueues.new()
  local server_state, client_state
  local failed

  cq:wrap(function()
    local secure, state, err = tls.handshake_inbound(a, {
      identity_keypair = server_identity,
      muxer_protocols = { YAMUX },
    })
    if not secure then
      failed = "server TLS handshake failed: " .. tostring(err)
      return
    end
    server_state = state
    secure:close()
  end)

  cq:wrap(function()
    local secure, state, err = tls.handshake_outbound(b, {
      identity_keypair = client_identity,
      muxer_protocols = { YAMUX },
    })
    if not secure then
      failed = "client TLS handshake failed: " .. tostring(err)
      return
    end
    client_state = state
    secure:close()
  end)

  local loop_ok, loop_err = cq:loop()
  a:close()
  b:close()
  if not loop_ok then
    return nil, tostring(loop_err)
  end
  if failed then
    return nil, failed
  end
  if not server_state or not client_state then
    return nil, "expected both TLS handshakes to complete"
  end
  if server_state.selected_muxer ~= YAMUX then
    return nil, "server did not select yamux via TLS ALPN"
  end
  if client_state.selected_muxer ~= YAMUX then
    return nil, "client did not select yamux via TLS ALPN"
  end
  if server_state.used_early_muxer_negotiation ~= true or client_state.used_early_muxer_negotiation ~= true then
    return nil, "expected TLS ALPN to mark early muxer negotiation as used"
  end

  return true
end

return {
  name = "tls alpn inlined muxer negotiation",
  run = run,
}
