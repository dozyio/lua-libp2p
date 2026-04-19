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

  local _, bad_err = multiaddr.to_bytes("/ip6/::1/tcp/4001")
  if not bad_err then
    return nil, "expected unsupported ip6 binary encoding error"
  end

  return true
end

return {
  name = "multiaddr binary codec subset",
  run = run,
}
