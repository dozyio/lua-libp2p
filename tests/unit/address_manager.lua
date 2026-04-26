local address_manager = require("lua_libp2p.address_manager")

local function run()
  local am = address_manager.new({
    listen_addrs = { "/ip4/127.0.0.1/tcp/4001" },
    no_announce_addrs = { "/ip4/127.0.0.1/tcp/4001" },
  })

  am:add_observed_addr("/ip4/203.0.113.7/tcp/4001")
  am:add_observed_addr("/ip4/203.0.113.7/tcp/4001")
  am:add_relay_addr("/ip4/198.51.100.1/tcp/4001/p2p/relay/p2p-circuit")

  local advertised = am:get_advertise_addrs()
  if #advertised ~= 1 then
    return nil, "expected only relay addr by default"
  end
  if advertised[1] ~= "/ip4/198.51.100.1/tcp/4001/p2p/relay/p2p-circuit" then
    return nil, "expected relay addr"
  end

  am:set_announce_addrs({ "/dns4/example.com/tcp/4001" })
  advertised = am:get_advertise_addrs()
  if #advertised ~= 2 or advertised[1] ~= "/dns4/example.com/tcp/4001" then
    return nil, "explicit announce addr should replace listen/observed addrs"
  end

  local removed = am:remove_relay_addr("/ip4/198.51.100.1/tcp/4001/p2p/relay/p2p-circuit")
  if not removed then
    return nil, "expected relay addr removal"
  end
  advertised = am:get_advertise_addrs()
  if #advertised ~= 1 then
    return nil, "expected only announce addr after relay removal"
  end

  return true
end

return {
  name = "address manager advertised address selection",
  run = run,
}
