local multiaddr = require("lua_libp2p.multiaddr")

local function run()
  local parsed, parse_err = multiaddr.parse("/dns4/bootstrap.libp2p.io/tcp/443/tls/ws")
  if not parsed then
    return nil, parse_err
  end
  if #parsed.components ~= 4 then
    return nil, "expected four multiaddr components"
  end
  if parsed.components[1].protocol ~= "dns4" or parsed.components[2].protocol ~= "tcp" then
    return nil, "unexpected parsed protocol order"
  end

  local formatted, fmt_err = multiaddr.format(parsed)
  if not formatted then
    return nil, fmt_err
  end
  if formatted ~= "/dns4/bootstrap.libp2p.io/tcp/443/tls/ws" then
    return nil, "multiaddr format roundtrip mismatch"
  end

  local endpoint, endpoint_err = multiaddr.to_tcp_endpoint("/dns4/bootstrap.libp2p.io/tcp/443")
  if not endpoint then
    return nil, endpoint_err
  end
  if endpoint.host ~= "bootstrap.libp2p.io" or endpoint.port ~= 443 then
    return nil, "unexpected tcp endpoint values"
  end

  local peer = "12D3KooWCryG7Mon9orvQxcS1rYZjotPgpwoJNHHKcLLfE4Hf5mV"
  local encapsulated, enc_err = multiaddr.encapsulate("/ip4/127.0.0.1/tcp/4001", "/p2p/" .. peer)
  if not encapsulated then
    return nil, enc_err
  end
  if encapsulated ~= "/ip4/127.0.0.1/tcp/4001/p2p/" .. peer then
    return nil, "encapsulation mismatch"
  end

  local decapsulated, dec_err = multiaddr.decapsulate(encapsulated, "/p2p/" .. peer)
  if not decapsulated then
    return nil, dec_err
  end
  if decapsulated ~= "/ip4/127.0.0.1/tcp/4001" then
    return nil, "decapsulation mismatch"
  end

  local _, invalid_proto_err = multiaddr.parse("/notaproto/value")
  if not invalid_proto_err then
    return nil, "expected unsupported protocol error"
  end

  local _, missing_value_err = multiaddr.parse("/ip4/127.0.0.1/tcp")
  if not missing_value_err then
    return nil, "expected missing protocol value error"
  end

  local noval_text = "/ip4/127.0.0.1/tcp/4001/quic-v1/webtransport/p2p-circuit"
  local noval_addr, noval_err = multiaddr.parse(noval_text)
  if not noval_addr then
    return nil, noval_err
  end
  local noval_roundtrip, noval_fmt_err = multiaddr.format(noval_addr)
  if not noval_roundtrip then
    return nil, noval_fmt_err
  end
  if noval_roundtrip ~= noval_text then
    return nil, "no-value protocol roundtrip mismatch"
  end

  if not multiaddr.is_private_addr("/ip4/10.0.0.1/tcp/4001") then
    return nil, "expected RFC1918 ip4 addr to be private"
  end
  if multiaddr.is_public_addr("/ip4/127.0.0.1/tcp/4001") then
    return nil, "expected loopback ip4 addr to not be public"
  end
  if not multiaddr.is_public_addr("/ip4/8.8.8.8/tcp/4001") then
    return nil, "expected public ip4 addr to be public"
  end
  if not multiaddr.is_public_addr("/dns/bootstrap.libp2p.io/tcp/4001") then
    return nil, "expected dns addr to pass public filter by default"
  end

  return true
end

return {
  name = "multiaddr parse and utilities",
  run = run,
}
