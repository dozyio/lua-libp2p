package = "lua-libp2p-dev"
version = "1.0-1"
source = {
  url = "git://github.com/dozyio/lua-libp2p",
}
description = {
  summary = "Minimal libp2p building blocks for Lua",
  detailed = "Development rockspec for lua-libp2p and its runtime dependencies.",
  homepage = "https://github.com/dozyio/lua-libp2p",
  license = "MIT",
}
dependencies = {
  "lua >= 5.4",
  "luasocket >= 3.0",
  "lua-protobuf >= 0.5",
  "luasodium >= 2.4",
}
build = {
  type = "builtin",
  modules = {
    ["lua_libp2p"] = "lua_libp2p/init.lua",
    ["lua_libp2p.error"] = "lua_libp2p/error.lua",
    ["lua_libp2p.log"] = "lua_libp2p/log.lua",
    ["lua_libp2p.multiaddr"] = "lua_libp2p/multiaddr.lua",
    ["lua_libp2p.peerid"] = "lua_libp2p/peerid.lua",
    ["lua_libp2p.record"] = "lua_libp2p/record/init.lua",
    ["lua_libp2p.record.signed_envelope"] = "lua_libp2p/record/signed_envelope.lua",
    ["lua_libp2p.record.peer_record"] = "lua_libp2p/record/peer_record.lua",
    ["lua_libp2p.multiformats"] = "lua_libp2p/multiformats/init.lua",
    ["lua_libp2p.multiformats.varint"] = "lua_libp2p/multiformats/varint.lua",
    ["lua_libp2p.multiformats.multihash"] = "lua_libp2p/multiformats/multihash.lua",
    ["lua_libp2p.multiformats.base58btc"] = "lua_libp2p/multiformats/base58btc.lua",
    ["lua_libp2p.multiformats.base32"] = "lua_libp2p/multiformats/base32.lua",
    ["lua_libp2p.multiformats.cid"] = "lua_libp2p/multiformats/cid.lua",
    ["lua_libp2p.transport"] = "lua_libp2p/transport/init.lua",
    ["lua_libp2p.transport.tcp"] = "lua_libp2p/transport/tcp.lua",
    ["lua_libp2p.security"] = "lua_libp2p/security/init.lua",
    ["lua_libp2p.muxer"] = "lua_libp2p/muxer/init.lua",
    ["lua_libp2p.protocol"] = "lua_libp2p/protocol/init.lua",
    ["lua_libp2p.protocol.dummy"] = "lua_libp2p/protocol/dummy.lua",
    ["lua_libp2p.protocol.identify"] = "lua_libp2p/protocol/identify.lua",
    ["lua_libp2p.protocol.mss"] = "lua_libp2p/protocol/mss.lua",
    ["lua_libp2p.protocol.ping"] = "lua_libp2p/protocol/ping.lua",
    ["lua_libp2p.protocol.plaintext"] = "lua_libp2p/protocol/plaintext.lua",
    ["lua_libp2p.crypto"] = "lua_libp2p/crypto/init.lua",
    ["lua_libp2p.crypto.ed25519"] = "lua_libp2p/crypto/ed25519.lua",
    ["lua_libp2p.crypto.key_pb"] = "lua_libp2p/crypto/key_pb.lua",
    ["lua_libp2p.peerstore"] = "lua_libp2p/peerstore/init.lua",
  },
}
