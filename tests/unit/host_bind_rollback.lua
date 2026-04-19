local ed25519 = require("lua_libp2p.crypto.ed25519")
local host_mod = require("lua_libp2p.host")
local tcp = require("lua_libp2p.transport.tcp")

local function new_host()
  local keypair = assert(ed25519.generate_keypair())
  local h, err = host_mod.new({
    identity = keypair,
    listen_addrs = {},
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

  local ok, err = h:start({
    blocking = false,
    listen_addrs = {
      "/ip4/127.0.0.1/tcp/4101",
      "/ip4/127.0.0.1/tcp/4102",
    },
  })
  if ok or not err then
    tcp.listen = original_listen
    return nil, "expected bind failure with partial listener rollback"
  end
  if not first_new.closed then
    tcp.listen = original_listen
    return nil, "partially bound listener should be closed on rollback"
  end
  if #h._listeners ~= 0 or #h.listen_addrs ~= 0 then
    tcp.listen = original_listen
    return nil, "failed initial bind should not leave listeners or addresses"
  end

  local existing = make_listener("4999")
  h._listeners = { existing }
  h.listen_addrs = { "/ip4/127.0.0.1/tcp/4999" }

  local rebind_new = make_listener("5001")
  listen_calls = 0
  tcp.listen = function()
    listen_calls = listen_calls + 1
    if listen_calls == 1 then
      return rebind_new
    end
    return nil, "forced rebind failure"
  end

  ok, err = h:start({
    blocking = false,
    listen_addrs = {
      "/ip4/127.0.0.1/tcp/5001",
      "/ip4/127.0.0.1/tcp/5002",
    },
  })
  tcp.listen = original_listen

  if ok or not err then
    return nil, "expected rebind failure"
  end
  if not rebind_new.closed then
    return nil, "partially rebound listener should be closed on rollback"
  end
  if existing.closed then
    return nil, "existing listener should remain open when rebind fails"
  end
  if #h._listeners ~= 1 or h._listeners[1] ~= existing then
    return nil, "existing listeners should remain unchanged on rebind failure"
  end
  if #h.listen_addrs ~= 1 or h.listen_addrs[1] ~= "/ip4/127.0.0.1/tcp/4999" then
    return nil, "existing listen_addrs should remain unchanged on rebind failure"
  end

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
