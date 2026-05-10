local multiaddr = require("lua_libp2p.multiaddr")

local function run()
  local text = "/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWCryG7Mon9orvQxcS1rYZjotPgpwoJNHHKcLLfE4Hf5mV"
  local bytes, bytes_err = multiaddr.to_bytes(text)
  if not bytes then
    return nil, bytes_err
  end

  local decoded, decode_err = multiaddr.from_bytes(bytes)
  if not decoded then
    return nil, decode_err
  end
  if decoded.text ~= text then
    return nil, "multiaddr binary roundtrip mismatch"
  end

  local relay_text =
    "/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWCryG7Mon9orvQxcS1rYZjotPgpwoJNHHKcLLfE4Hf5mV/p2p-circuit/p2p/12D3KooWQWZLu9qXWPTDnF9rTRrAiVGZrXCbHAvkqYrsG8cW4UHg"
  local relay_bytes, relay_bytes_err = multiaddr.to_bytes(relay_text)
  if not relay_bytes then
    return nil, relay_bytes_err
  end
  local relay_decoded, relay_decode_err = multiaddr.from_bytes(relay_bytes)
  if not relay_decoded then
    return nil, relay_decode_err
  end
  if relay_decoded.text ~= relay_text then
    return nil, "relay multiaddr binary roundtrip mismatch"
  end

  local ip6_text = "/ip6/2a00:23c7:ad38:6601:1dbf:9fc1:137b:13c4/tcp/4001"
  local ip6_bytes, ip6_bytes_err = multiaddr.to_bytes(ip6_text)
  if not ip6_bytes then
    return nil, ip6_bytes_err
  end
  local ip6_decoded, ip6_decode_err = multiaddr.from_bytes(ip6_bytes)
  if not ip6_decoded then
    return nil, ip6_decode_err
  end
  if ip6_decoded.text ~= ip6_text then
    return nil, "ip6 multiaddr binary roundtrip mismatch: " .. tostring(ip6_decoded.text)
  end

  local loopback_bytes, loopback_err = multiaddr.to_bytes("/ip6/::1/tcp/4001")
  if not loopback_bytes then
    return nil, loopback_err
  end
  local loopback_decoded, loopback_decode_err = multiaddr.from_bytes(loopback_bytes)
  if not loopback_decoded then
    return nil, loopback_decode_err
  end
  if loopback_decoded.text ~= "/ip6/0:0:0:0:0:0:0:1/tcp/4001" then
    return nil, "compressed ip6 multiaddr binary roundtrip mismatch: " .. tostring(loopback_decoded.text)
  end

  return true
end

return {
  name = "multiaddr binary codec subset",
  run = run,
}
