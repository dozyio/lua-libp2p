local ed25519 = require("lua_libp2p.crypto.ed25519")
local host_mod = require("lua_libp2p.host")
local tcp = require("lua_libp2p.transport_tcp.transport")

local function new_host()
  local keypair = assert(ed25519.generate_keypair())
  local h, err = host_mod.new({
    runtime = "poll",
    identity = keypair,
    listen_addrs = {},
    blocking = false,
  })
  if not h then
    error(err)
  end
  return h
end

local function make_listener(name)
  local listener = {
    name = name,
    closed = false,
  }
  function listener:close()
    self.closed = true
    return true
  end
  function listener:multiaddr()
    return "/ip4/127.0.0.1/tcp/" .. name
  end
  return listener
end

local function run_inner()
  local h = new_host()

  local first_new = make_listener("4101")
  local listen_calls = 0
  local original_listen = tcp.listen

  tcp.listen = function()
    listen_calls = listen_calls + 1
    if listen_calls == 1 then
      return first_new
    end
    return nil, "forced bind failure"
  end

  h.listen_addrs = {
    "/ip4/127.0.0.1/tcp/4101",
    "/ip4/127.0.0.1/tcp/4102",
  }
  local ok, err = h:start()
  if ok or not err then
    tcp.listen = original_listen
    return nil, "expected bind failure with partial listener rollback"
  end
  if not first_new.closed then
    tcp.listen = original_listen
    return nil, "partially bound listener should be closed on rollback"
  end
  if #h._listeners ~= 0 then
    tcp.listen = original_listen
    return nil, "failed initial bind should not leave listeners"
  end
  if #h.listen_addrs ~= 2 then
    tcp.listen = original_listen
    return nil, "failed initial bind should preserve configured listen_addrs"
  end

  tcp.listen = original_listen

  return true
end

local function run()
  local original_listen = tcp.listen
  local ok, a, b = pcall(run_inner)
  tcp.listen = original_listen
  if not ok then
    return nil, a
  end
  return a, b
end

return {
  name = "host listener bind rollback",
  run = run,
}
