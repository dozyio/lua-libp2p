local ed25519 = require("lua_libp2p.crypto.ed25519")
local host_mod = require("lua_libp2p.host")

local function ends_with(value, suffix)
  return value:sub(-#suffix) == suffix
end

local function run()
  local h = assert(host_mod.new({
    identity = assert(ed25519.generate_keypair()),
    runtime = "luv",
    listen_addrs = { "/ip4/127.0.0.1/tcp/4001" },
  }))
  local pid = h:peer_id().id

  local raw = h:get_multiaddrs_raw()
  if raw[1] ~= "/ip4/127.0.0.1/tcp/4001" then
    return nil, "raw advertised address should not include peer id"
  end
  local with_peer = h:get_multiaddrs()
  if not with_peer[1] or not ends_with(with_peer[1], "/p2p/" .. pid) then
    return nil, "get_multiaddrs should append peer id"
  end

  h.listen_addrs = { "/ip4/127.0.0.1/tcp/4001/p2p/" .. pid }
  local terminal = h:get_multiaddrs()
  if terminal[1] ~= h.listen_addrs[1] then
    return nil, "get_multiaddrs should not duplicate terminal peer id"
  end

  local sub = assert(h:subscribe("self_peer_update"))
  assert(h:_emit_self_peer_update_if_changed())
  local ev = h:next_event(sub)
  if not ev or ev.name ~= "self_peer_update" then
    return nil, "self peer update should emit first snapshot"
  end
  assert(h:_emit_self_peer_update_if_changed())
  if h:next_event(sub) ~= nil then
    return nil, "unchanged advertised state should not emit"
  end
  h._running = true
  assert(h:handle("/tests/new/1.0.0", function() return true end))
  local changed = h:next_event(sub)
  if not changed or changed.name ~= "self_peer_update" then
    return nil, "protocol changes should emit self peer update"
  end

  return true
end

return {
  name = "host advertised state",
  run = run,
}
