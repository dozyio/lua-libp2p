local ed25519 = require("lua_libp2p.crypto.ed25519")
local identify = require("lua_libp2p.protocol_identify.protocol")
local key_pb = require("lua_libp2p.crypto.key_pb")
local mss = require("lua_libp2p.protocol.mss")
local multiaddr = require("lua_libp2p.multiaddr")
local peer_record = require("lua_libp2p.record.peer_record")
local peerid = require("lua_libp2p.peerid")
local signed_envelope = require("lua_libp2p.record.signed_envelope")
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

  local server_key, key_err = ed25519.generate_keypair()
  if not server_key then
    server:close()
    client:close()
    listener:close()
    return nil, key_err
  end
  local pubkey_proto, pub_err = key_pb.encode_public_key(key_pb.KEY_TYPE.Ed25519, server_key.public_key)
  if not pubkey_proto then
    server:close()
    client:close()
    listener:close()
    return nil, pub_err
  end

  local server_identify = {
    protocolVersion = "/lua-libp2p/0.1.0",
    agentVersion = "lua-libp2p/test",
    publicKey = pubkey_proto,
    listenAddrs = { addr },
    observedAddr = "/ip4/127.0.0.1/tcp/40000",
    protocols = { "/ipfs/id/1.0.0", "/ipfs/ping/1.0.0" },
  }

  local server_pid, pid_err = peerid.from_ed25519_public_key(server_key.public_key)
  if not server_pid then
    return nil, pid_err
  end
  local rec = assert(peer_record.make_record(server_pid.id, 9, { addr }))
  local env = assert(peer_record.sign_ed25519(server_key, rec))
  server_identify.signedPeerRecord = assert(signed_envelope.encode(env))

  local expected_addr_bytes, expected_addr_err = multiaddr.to_bytes(addr)
  if not expected_addr_bytes then
    return nil, expected_addr_err
  end

  local ok, err = mss.write_frame(client, mss.PROTOCOL_ID)
  if not ok then
    return nil, err
  end
  local h1, h1_err = mss.read_frame(server)
  if not h1 then
    return nil, h1_err
  end
  if h1 ~= mss.PROTOCOL_ID then
    return nil, "server did not receive mss header"
  end
  ok, err = mss.write_frame(server, mss.PROTOCOL_ID)
  if not ok then
    return nil, err
  end
  local h2, h2_err = mss.read_frame(client)
  if not h2 then
    return nil, h2_err
  end
  if h2 ~= mss.PROTOCOL_ID then
    return nil, "client did not receive mss header"
  end

  ok, err = mss.write_frame(client, identify.ID)
  if not ok then
    return nil, err
  end
  local req, req_err = mss.read_frame(server)
  if not req then
    return nil, req_err
  end
  if req ~= identify.ID then
    return nil, "server did not receive identify protocol id"
  end
  ok, err = mss.write_frame(server, identify.ID)
  if not ok then
    return nil, err
  end
  local ack, ack_err = mss.read_frame(client)
  if not ack then
    return nil, ack_err
  end
  if ack ~= identify.ID then
    return nil, "client did not receive identify protocol ack"
  end

  ok, err = identify.write(server, server_identify)
  if not ok then
    return nil, err
  end

  local seen, seen_err = identify.read(client)
  if not seen then
    return nil, seen_err
  end
  if seen.agentVersion ~= server_identify.agentVersion then
    return nil, "identify agentVersion mismatch"
  end
  if #seen.listenAddrs ~= 1 or seen.listenAddrs[1] ~= expected_addr_bytes then
    return nil, "identify listen address mismatch"
  end
  local verified, verified_err = identify.verify_signed_peer_record(seen, { expected_peer_id = server_pid.id })
  if not verified then
    return nil, verified_err
  end
  if verified.record.seq ~= 9 then
    return nil, "identify signedPeerRecord seq mismatch"
  end

  local seen_addr, seen_addr_err = multiaddr.from_bytes(seen.listenAddrs[1])
  if not seen_addr then
    return nil, seen_addr_err
  end
  if seen_addr.text ~= addr then
    return nil, "identify listen address decode mismatch"
  end

  server:close()
  client:close()
  listener:close()
  return true
end

return {
  name = "identify response over tcp+mss",
  run = run,
}
