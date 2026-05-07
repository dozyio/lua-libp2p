local ed25519 = require("lua_libp2p.crypto.ed25519")
local host_mod = require("lua_libp2p.host")
local function new_host()
  local keypair = assert(ed25519.generate_keypair())
  local h, err = host_mod.new({
    runtime = "luv",
    identity = keypair,
    listen_addrs = {},
    blocking = false,
  })
  if not h then
    error(err)
  end
  return h
end

local function make_listener(name, addr)
  local listener = {
    name = name,
    addr = addr or ("/ip4/127.0.0.1/tcp/" .. name),
    closed = false,
  }
  function listener:close()
    self.closed = true
    return true
  end
  function listener:multiaddr()
    return self.addr
  end
  return listener
end

local function run_inner()
  local h = new_host()

  local first_new = make_listener("4101")
  local listen_calls = 0
  local original_listen = h._tcp_transport.listen

  h._tcp_transport.listen = function()
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
    h._tcp_transport.listen = original_listen
    return nil, "expected bind failure with partial listener rollback"
  end
  if not first_new.closed then
    h._tcp_transport.listen = original_listen
    return nil, "partially bound listener should be closed on rollback"
  end
  if #h._listeners ~= 0 then
    h._tcp_transport.listen = original_listen
    return nil, "failed initial bind should not leave listeners"
  end
  if #h.listen_addrs ~= 2 then
    h._tcp_transport.listen = original_listen
    return nil, "failed initial bind should preserve configured listen_addrs"
  end

  h._tcp_transport.listen = original_listen

  local h2 = new_host()
  local first = make_listener("4201", "/ip4/127.0.0.1/tcp/4201")
  local second = make_listener("4201", "/ip4/127.0.0.1/tcp/4201")
  local calls = 0
  local original_listen2 = h2._tcp_transport.listen
  h2._tcp_transport.listen = function()
    calls = calls + 1
    if calls == 1 then
      return first
    end
    return second
  end
  h2.listen_addrs = {
    "/ip4/0.0.0.0/tcp/4201",
    "/ip6/::/tcp/4201",
  }
  local ok2, err2 = h2:start()
  h2._tcp_transport.listen = original_listen2
  if ok2 then
    h2:stop()
    return nil, "expected bind verification failure when ipv6 listener is missing"
  end
  if not tostring(err2):find("missing ip6 listener", 1, true) then
    return nil, "expected explicit missing ip6 listener verification error"
  end
  if not first.closed or not second.closed then
    return nil, "verification failure should close newly bound listeners"
  end

  local h3 = new_host()
  local v6_listener = make_listener("4301", "/ip6/::/tcp/4301")
  local calls3 = 0
  local original_listen3 = h3._tcp_transport.listen
  h3._tcp_transport.listen = function(opts)
    calls3 = calls3 + 1
    if opts and opts.multiaddr == "/ip6/::/tcp/4301" then
      return v6_listener
    end
    return nil, "EADDRINUSE: address already in use"
  end
  h3.listen_addrs = {
    "/ip4/0.0.0.0/tcp/4301",
    "/ip6/::/tcp/4301",
  }
  local ok3, err3 = h3:start()
  h3._tcp_transport.listen = original_listen3
  if not ok3 then
    return nil, err3
  end
  if #h3._listeners ~= 1 then
    h3:stop()
    return nil, "dual-stack coverage mode should keep single ipv6 wildcard listener"
  end
  if #h3.listen_addrs ~= 1 or h3.listen_addrs[1] ~= "/ip6/::/tcp/4301" then
    h3:stop()
    return nil, "dual-stack coverage mode should resolve to ipv6 wildcard listen address"
  end
  h3:stop()

  return true
end

local function run()
  local ok, a, b = pcall(run_inner)
  if not ok then
    return nil, a
  end
  return a, b
end

return {
  name = "host listener bind rollback",
  run = run,
}
