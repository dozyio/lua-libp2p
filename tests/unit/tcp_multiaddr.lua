local tcp = require("lua_libp2p.transport_tcp.transport")

local function run()
  local parsed, parse_err = tcp.parse_multiaddr("/ip4/127.0.0.1/tcp/4001")
  if not parsed then
    return nil, parse_err
  end
  if parsed.host ~= "127.0.0.1" or parsed.port ~= 4001 then
    return nil, "unexpected multiaddr parse result"
  end

  local formatted, format_err = tcp.format_multiaddr(parsed.host, parsed.port)
  if not formatted then
    return nil, format_err
  end
  if formatted ~= "/ip4/127.0.0.1/tcp/4001" then
    return nil, "unexpected multiaddr formatting"
  end

  local dns_parsed, dns_parse_err = tcp.parse_multiaddr("/dns4/example.com/tcp/443")
  if not dns_parsed then
    return nil, dns_parse_err
  end
  if dns_parsed.host ~= "example.com" or dns_parsed.port ~= 443 then
    return nil, "unexpected dns multiaddr parse result"
  end

  local _, strict_err = tcp.parse_multiaddr("/ip4/127.0.0.1/tcp/4001/tls")
  if not strict_err then
    return nil, "expected tcp multiaddr strictness failure"
  end

  local _, bad_parse_err = tcp.parse_multiaddr("/ip4/300.1.1.1/tcp/1")
  if not bad_parse_err then
    return nil, "expected invalid ip4 parse failure"
  end

  local _, bad_format_err = tcp.format_multiaddr("127.0.0.1", 70000)
  if not bad_format_err then
    return nil, "expected invalid port format failure"
  end

  return true
end

return {
  name = "tcp multiaddr parse and format",
  run = run,
}
