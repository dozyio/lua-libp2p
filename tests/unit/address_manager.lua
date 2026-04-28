local address_manager = require("lua_libp2p.address_manager")

local function run()
  local am = address_manager.new({
    listen_addrs = { "/ip4/127.0.0.1/tcp/4001" },
    no_announce_addrs = { "/ip4/127.0.0.1/tcp/4001" },
  })
  local private_meta = am:get_reachability("/ip4/127.0.0.1/tcp/4001")
  if not private_meta or private_meta.status ~= "private" or private_meta.type ~= "transport" then
    return nil, "private listen addr should be marked private"
  end

  local wildcard = address_manager.new({
    listen_addrs = { "/ip4/0.0.0.0/tcp/4001" },
  })
  local transport_addrs = wildcard:get_transport_addrs()
  if #transport_addrs == 0 then
    return nil, "wildcard listen addr should have at least one transport addr"
  end
  for _, addr in ipairs(transport_addrs) do
    if addr == "/ip4/0.0.0.0/tcp/4001" then
      goto continue_transport
    end
    if addr:find("/ip4/0.0.0.0/", 1, true) then
      return nil, "wildcard transport addr should be expanded to concrete interface addr"
    end
    ::continue_transport::
  end

  am:add_observed_addr("/ip4/203.0.113.7/tcp/4001")
  am:add_observed_addr("/ip4/203.0.113.7/tcp/4001")
  am:add_observed_addr("/ip4/10.0.0.2/tcp/4001")
  private_meta = am:get_reachability("/ip4/10.0.0.2/tcp/4001")
  if not private_meta or private_meta.status ~= "private" or private_meta.type ~= "observed" then
    return nil, "private observed addr should be marked private"
  end
  am:add_relay_addr("/ip4/198.51.100.1/tcp/4001/p2p/relay/p2p-circuit")

  local advertised = am:get_advertise_addrs()
  if #advertised ~= 1 then
    return nil, "expected only relay addr by default"
  end
  if advertised[1] ~= "/ip4/198.51.100.1/tcp/4001/p2p/relay/p2p-circuit" then
    return nil, "expected relay addr"
  end
  local relay_meta = am:get_reachability("/ip4/198.51.100.1/tcp/4001/p2p/relay/p2p-circuit")
  if not relay_meta or relay_meta.type ~= "transport" or relay_meta.verified ~= true or relay_meta.status ~= "public" then
    return nil, "relay addr should be marked as verified transport addr"
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
  if am:get_reachability("/ip4/198.51.100.1/tcp/4001/p2p/relay/p2p-circuit") ~= nil then
    return nil, "relay addr removal should clear reachability metadata"
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
