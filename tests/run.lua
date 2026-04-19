package.path = table.concat({
  "./?.lua",
  "./?/init.lua",
  package.path,
}, ";")

local tests = {
  (require("tests.integration.dummy_loopback")),
  (require("tests.integration.identify_tcp")),
  (require("tests.integration.mss_tcp_framing")),
  (require("tests.integration.ping_tcp")),
  (require("tests.integration.plaintext_tcp")),
  (require("tests.integration.tcp_loopback")),
  (require("tests.integration.yamux_tcp")),
  (require("tests.unit.loopback_errors")),
  (require("tests.unit.multiaddr")),
  (require("tests.unit.multiaddr_bytes")),
  (require("tests.unit.multiaddr_vectors")),
  (require("tests.unit.multiaddr_go_deltas")),
  (require("tests.unit.tcp_multiaddr")),
  (require("tests.unit.key_pb")),
  (require("tests.unit.host")),
  (require("tests.unit.identify")),
  (require("tests.unit.mss")),
  (require("tests.unit.network_connection")),
  (require("tests.unit.signed_envelope")),
  (require("tests.unit.peer_record")),
  (require("tests.unit.ping")),
  (require("tests.unit.plaintext")),
  (require("tests.unit.upgrader")),
  (require("tests.unit.yamux")),
  (require("tests.unit.ed25519")),
  (require("tests.unit.peerid")),
  (require("tests.unit.spec_vectors_ed25519")),
  (require("tests.unit.multiformats")),
}

local failed = 0

for _, t in ipairs(tests) do
  local ok, err = t.run()
  if ok then
    io.stdout:write(string.format("[PASS] %s\n", t.name))
  else
    failed = failed + 1
    io.stderr:write(string.format("[FAIL] %s: %s\n", t.name, tostring(err)))
  end
end

if failed > 0 then
  io.stderr:write(string.format("%d test(s) failed\n", failed))
  os.exit(1)
end

io.stdout:write(string.format("%d test(s) passed\n", #tests))
