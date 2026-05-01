local ed25519 = require("lua_libp2p.crypto.ed25519")
local mss = require("lua_libp2p.multistream_select.protocol")
local plaintext = require("lua_libp2p.connection_encrypter_plaintext.protocol")
local tcp = require("lua_libp2p.transport_tcp.transport")

local function run()
  local listener, listen_err = tcp.listen({
    host = "127.0.0.1",
    port = 0,
    accept_timeout = 1,
    io_timeout = 1,
  })
  if not listener then
    return nil, listen_err
  end

  local addr, addr_err = listener:multiaddr()
  if not addr then
    listener:close()
    return nil, addr_err
  end

  local client, dial_err = tcp.dial(addr, { timeout = 1 })
  if not client then
    listener:close()
    return nil, dial_err
  end

  local server, accept_err = listener:accept(1)
  if not server then
    client:close()
    listener:close()
    return nil, accept_err
  end

  local c_key, c_key_err = ed25519.generate_keypair()
  if not c_key then
    server:close()
    client:close()
    listener:close()
    return nil, c_key_err
  end
  local s_key, s_key_err = ed25519.generate_keypair()
  if not s_key then
    server:close()
    client:close()
    listener:close()
    return nil, s_key_err
  end

  local c_ex, c_ex_err = plaintext.make_exchange_from_ed25519_public_key(c_key.public_key)
  if not c_ex then
    server:close()
    client:close()
    listener:close()
    return nil, c_ex_err
  end
  local s_ex, s_ex_err = plaintext.make_exchange_from_ed25519_public_key(s_key.public_key)
  if not s_ex then
    server:close()
    client:close()
    listener:close()
    return nil, s_ex_err
  end

  local ok, err = mss.write_frame(client, mss.PROTOCOL_ID)
  if not ok then
    server:close()
    client:close()
    listener:close()
    return nil, err
  end
  local h1, h1_err = mss.read_frame(server)
  if not h1 then
    server:close()
    client:close()
    listener:close()
    return nil, h1_err
  end
  if h1 ~= mss.PROTOCOL_ID then
    return nil, "server did not receive mss header"
  end
  ok, err = mss.write_frame(server, mss.PROTOCOL_ID)
  if not ok then
    server:close()
    client:close()
    listener:close()
    return nil, err
  end
  local h2, h2_err = mss.read_frame(client)
  if not h2 then
    server:close()
    client:close()
    listener:close()
    return nil, h2_err
  end
  if h2 ~= mss.PROTOCOL_ID then
    return nil, "client did not receive mss header"
  end

  ok, err = mss.write_frame(client, plaintext.PROTOCOL_ID)
  if not ok then
    server:close()
    client:close()
    listener:close()
    return nil, err
  end
  local req, req_err = mss.read_frame(server)
  if not req then
    server:close()
    client:close()
    listener:close()
    return nil, req_err
  end
  if req ~= plaintext.PROTOCOL_ID then
    return nil, "server did not receive plaintext protocol id"
  end
  ok, err = mss.write_frame(server, plaintext.PROTOCOL_ID)
  if not ok then
    server:close()
    client:close()
    listener:close()
    return nil, err
  end
  local ack, ack_err = mss.read_frame(client)
  if not ack then
    server:close()
    client:close()
    listener:close()
    return nil, ack_err
  end
  if ack ~= plaintext.PROTOCOL_ID then
    return nil, "client did not receive plaintext protocol ack"
  end

  ok, err = plaintext.write_exchange(client, c_ex)
  if not ok then
    server:close()
    client:close()
    listener:close()
    return nil, err
  end
  ok, err = plaintext.write_exchange(server, s_ex)
  if not ok then
    server:close()
    client:close()
    listener:close()
    return nil, err
  end

  local c_seen, c_seen_err = plaintext.read_exchange(server)
  if not c_seen then
    server:close()
    client:close()
    listener:close()
    return nil, c_seen_err
  end
  local c_verified, c_verify_err = plaintext.verify_exchange(c_seen, c_ex.id)
  if not c_verified then
    server:close()
    client:close()
    listener:close()
    return nil, c_verify_err
  end
  if c_verified.peer_id.bytes ~= c_ex.id then
    return nil, "server verified wrong client peer id"
  end

  local s_seen, s_seen_err = plaintext.read_exchange(client)
  if not s_seen then
    server:close()
    client:close()
    listener:close()
    return nil, s_seen_err
  end
  local s_verified, s_verify_err = plaintext.verify_exchange(s_seen, s_ex.id)
  if not s_verified then
    server:close()
    client:close()
    listener:close()
    return nil, s_verify_err
  end
  if s_verified.peer_id.bytes ~= s_ex.id then
    return nil, "client verified wrong server peer id"
  end

  server:close()
  client:close()
  listener:close()
  return true
end

return {
  name = "plaintext handshake over tcp",
  run = run,
}
