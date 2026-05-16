local ed25519 = require("lua_libp2p.crypto.ed25519")
local host_mod = require("lua_libp2p.host")

local function run()
  local h = assert(host_mod.new({
    identity = assert(ed25519.generate_keypair()),
    runtime = "luv",
    listen_addrs = {},
  }))

  local handled = false
  local ok, err = h:handle("/tests/allowed/1.0.0", function()
    handled = true
    return true
  end, { run_on_limited_connection = true })
  if not ok then
    return nil, err
  end
  assert(h:handle("/tests/blocked/1.0.0", function()
    return true
  end))

  if not h:_protocol_allowed_on_limited_connection("/tests/allowed/1.0.0") then
    return nil, "handler option should allow limited connection"
  end
  local allowed, allow_err = h:_protocols_allowed_on_limited_connection({ "/tests/blocked/1.0.0" })
  if allowed ~= nil or not allow_err or allow_err.kind ~= "permission" then
    return nil, "limited connection policy should reject blocked protocols"
  end
  if
    not h:_protocols_allowed_on_limited_connection({ "/tests/blocked/1.0.0" }, { allow_limited_connection = true })
  then
    return nil, "outbound override should allow limited connection"
  end

  local router, router_err = h:_build_router()
  if not router then
    return nil, router_err
  end
  if not router._handlers["/tests/allowed/1.0.0"] then
    return nil, "router should contain registered protocol"
  end

  h.peerstore:merge("peer-a", { protocols = { "/tests/allowed/1.0.0" } })
  local replayed = false
  local sub, sub_err = h:on_protocol("/tests/allowed/1.0.0", function(peer_id)
    if peer_id == "peer-a" then
      replayed = true
    end
    return true
  end)
  if not sub then
    return nil, sub_err
  end
  if not replayed then
    return nil, "on_protocol should replay matching peerstore entries"
  end

  local emitted = false
  assert(h:on_protocol("/tests/dynamic/1.0.0", function(peer_id)
    if peer_id == "peer-b" then
      emitted = true
    end
    return true
  end))
  assert(h:emit("peer:protocols_updated", {
    peer_id = "peer-b",
    added_protocols = { "/tests/dynamic/1.0.0" },
  }))
  if not emitted then
    return nil, "on_protocol should observe protocol update events"
  end
  if handled then
    return nil, "registering handler should not call it"
  end

  return true
end

return {
  name = "host protocol registry",
  run = run,
}
