local V = {}

V.valid = {
  -- Core go-multiaddr/js-multiaddr style construction cases.
  "/ip4/1.2.3.4",
  "/ip4/0.0.0.0",
  "/ip4/127.0.0.1/tcp/0",
  "/ip4/127.0.0.1/tcp/1234",
  "/ip4/127.0.0.1/udp/1234",
  "/ip4/127.0.0.1/udp/1234/utp",
  "/ip4/127.0.0.1/udp/1234/udt",
  "/ip6/::1/tcp/4001",
  "/ip6/2001:db8::1/udp/4001/quic-v1",
  "/dns4/bootstrap.libp2p.io/tcp/443/tls/ws",
  "/dns6/bootstrap.libp2p.io/tcp/443/tls/ws",
  "/dnsaddr/bootstrap.libp2p.io/tcp/443",
  "/ip4/127.0.0.1/tcp/9090/tls/sni/example.com/http/http-path/foo",
  "/ip4/127.0.0.1/tcp/9090/http/p2p-webrtc-direct",
  "/ip4/127.0.0.1/tcp/127/noise",
  "/ip4/127.0.0.1/tcp/127/wss",
  "/ip4/127.0.0.1/tcp/127/webrtc-direct",
  "/ip4/127.0.0.1/tcp/4001/quic-v1/webtransport/p2p-circuit",
  "/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWCryG7Mon9orvQxcS1rYZjotPgpwoJNHHKcLLfE4Hf5mV",
  "/p2p/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC",
  "/ipfs/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC",
  "/memory/test/p2p/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC",
  "/unix/tmp%2Fp2p.sock",
  "/http-path/tmp%2Fbar",
}

V.invalid = {
  -- A subset adapted from go-multiaddr and js-multiaddr invalid construction tests.
  "/",
  "",
  "/ip4",
  "/ip6",
  "/tcp",
  "/udp",
  "/ip4/300.1.1.1/tcp/1",
  "/ip4/127.0.0.1/tcp/65536",
  "/ip4/127.0.0.1/udp/65536",
  "/ip4/127.0.0.1/tcp/jfodsajfidosajfoidsa",
  "/ip4/127.0.0.1/udp/jfodsajfidosajfoidsa",
  "/ip4/127.0.0.1/tcp",
  "/ip4/127.0.0.1/udp",
  "/ip4/127.0.0.1/p2p",
  "/ip4/127.0.0.1/p2p/tcp",
  "/ip4/127.0.0.1/quic/1234",
  "/ip4/127.0.0.1/quic-v1/1234",
  "/ip4/127.0.0.1/udp/1234/quic-v1/webtransport/certhash",
  "/ip4/127.0.0.1/udp/1234/quic-v1/webtransport/certhash/b2uaraocy6yrdblb4sfptaddgimjmmp",
  "/dns4/-bad.example/tcp/443",
  "/dns4/example..com/tcp/443",
  "/memory",
  "/notaproto/value",
}

V.normalized = {
  {
    input = "/ip4/127.0.0.1/ipfs/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC",
    output = "/ip4/127.0.0.1/p2p/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC",
  },
}

return V
