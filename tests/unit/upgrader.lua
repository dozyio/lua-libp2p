local ed25519 = require("lua_libp2p.crypto.ed25519")
local mss = require("lua_libp2p.protocol.mss")
local plaintext = require("lua_libp2p.protocol.plaintext")
local upgrader = require("lua_libp2p.network.upgrader")
local varint = require("lua_libp2p.multiformats.varint")
local yamux = require("lua_libp2p.muxer.yamux")

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

  function conn:close()
    return true
  end

  return conn
end

local function frame_exchange(exchange)
  local payload = assert(plaintext.encode_exchange(exchange))
  local len = assert(varint.encode_u64(#payload))
  return len .. payload
end

local function run()
  local local_key = assert(ed25519.generate_keypair())
  local remote_key = assert(ed25519.generate_keypair())
  local remote_exchange = assert(plaintext.make_exchange_from_ed25519_public_key(remote_key.public_key))
  local local_exchange = assert(plaintext.make_exchange_from_ed25519_public_key(local_key.public_key))

  local outbound_in = table.concat({
    assert(mss.encode_frame(mss.PROTOCOL_ID)),
    assert(mss.encode_frame(mss.NA)),
    assert(mss.encode_frame(plaintext.PROTOCOL_ID)),
    frame_exchange(remote_exchange),
    assert(mss.encode_frame(mss.PROTOCOL_ID)),
    assert(mss.encode_frame(yamux.PROTOCOL_ID)),
  })

  local outbound_raw = new_scripted_conn(outbound_in)
  local out_conn, out_state, out_err = upgrader.upgrade_outbound(outbound_raw, {
    local_keypair = local_key,
    expected_remote_peer_id = remote_exchange.id,
    security_protocols = { "/noise", plaintext.PROTOCOL_ID },
  })
  if not out_conn then
    return nil, out_err
  end
  if out_state.security ~= plaintext.PROTOCOL_ID or out_state.muxer ~= yamux.PROTOCOL_ID then
    return nil, "unexpected outbound upgrader negotiated stack"
  end
  if out_conn:session() == nil then
    return nil, "expected upgraded outbound connection to have stream session"
  end

  local inbound_in = table.concat({
    assert(mss.encode_frame(mss.PROTOCOL_ID)),
    assert(mss.encode_frame(plaintext.PROTOCOL_ID)),
    frame_exchange(remote_exchange),
    assert(mss.encode_frame(mss.PROTOCOL_ID)),
    assert(mss.encode_frame(yamux.PROTOCOL_ID)),
  })

  local inbound_raw = new_scripted_conn(inbound_in)
  local in_conn, in_state, in_err = upgrader.upgrade_inbound(inbound_raw, {
    local_keypair = local_key,
    security_protocols = { "/noise", plaintext.PROTOCOL_ID },
  })
  if not in_conn then
    return nil, in_err
  end
  if in_state.security ~= plaintext.PROTOCOL_ID or in_state.muxer ~= yamux.PROTOCOL_ID then
    return nil, "unexpected inbound upgrader negotiated stack"
  end
  if in_conn:session() == nil then
    return nil, "expected upgraded inbound connection to have stream session"
  end

  local expected_inbound_writes = table.concat({
    assert(mss.encode_frame(mss.PROTOCOL_ID)),
    assert(mss.encode_frame(plaintext.PROTOCOL_ID)),
  })
  if inbound_raw:writes():sub(1, #expected_inbound_writes) ~= expected_inbound_writes then
    return nil, "unexpected inbound upgrader write prefix"
  end

  local expected_outbound_prefix = table.concat({
    assert(mss.encode_frame(mss.PROTOCOL_ID)),
    assert(mss.encode_frame("/noise")),
    assert(mss.encode_frame(plaintext.PROTOCOL_ID)),
  })
  if outbound_raw:writes():sub(1, #expected_outbound_prefix) ~= expected_outbound_prefix then
    return nil, "unexpected outbound upgrader write prefix"
  end

  -- ensure outbound wrote local plaintext exchange and selected muxer
  if not outbound_raw:writes():find(assert(mss.encode_frame(yamux.PROTOCOL_ID)), 1, true) then
    return nil, "expected outbound upgrader to negotiate yamux"
  end

  -- ensure inbound wrote local plaintext exchange before muxer negotiation
  if not inbound_raw:writes():find(frame_exchange(local_exchange), 1, true) then
    return nil, "expected inbound upgrader to send plaintext exchange"
  end

  return true
end

return {
  name = "connection upgrader plaintext+yamux",
  run = run,
}
