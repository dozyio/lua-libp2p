local ed25519 = require("lua_libp2p.crypto.ed25519")
local host_mod = require("lua_libp2p.host")
local multiaddr = require("lua_libp2p.multiaddr")

local ok_socket, socket = pcall(require, "socket")

local function close_host(h)
  if h then
    pcall(function()
      h:close()
    end)
  end
end

local function close_sockets(sockets)
  for _, sock in ipairs(sockets or {}) do
    pcall(function()
      sock:close()
    end)
  end
end

local function listen_port(h)
  for _, addr in ipairs(h:get_listen_addrs()) do
    local parsed = multiaddr.parse(addr)
    local endpoint = parsed and multiaddr.to_tcp_endpoint(parsed) or nil
    if endpoint and endpoint.host == "127.0.0.1" and endpoint.port then
      return endpoint.port
    end
  end
  return nil
end

local function listener_totals(h)
  local pending = 0
  local callbacks = 0
  local accepted = 0
  local failed = 0
  for _, listener in ipairs(h._listeners or {}) do
    if listener and type(listener.stats) == "function" then
      local stats = listener:stats()
      pending = pending + (tonumber(stats.pending) or 0)
      callbacks = callbacks + (tonumber(stats.callback_total) or 0)
      accepted = accepted + (tonumber(stats.accepted_total) or 0)
      failed = failed + (tonumber(stats.accept_failed_total) or 0)
    end
  end
  return callbacks, accepted, pending, failed
end

local function run()
  if not ok_socket or type(socket.tcp) ~= "function" then
    return true
  end

  local burst = tonumber(os.getenv("LUA_LIBP2P_TEST_ACCEPT_BURST")) or 128
  local server, server_err = host_mod.new({
    identity = assert(ed25519.generate_keypair()),
    runtime = "luv",
    listen_addrs = { "/ip4/127.0.0.1/tcp/0" },
    max_accepts_per_tick = 64,
    resource_manager_options = {
      transient_connections = math.max(512, burst * 2),
      connections_inbound = math.max(512, burst * 2),
    },
    blocking = false,
    accept_timeout = 0.001,
    poll_interval = 0.001,
  })
  if not server then
    return nil, server_err
  end
  local ok, err = server:start()
  if not ok then
    close_host(server)
    return nil, err
  end

  local port = listen_port(server)
  if not port then
    close_host(server)
    return nil, "expected loopback listener port"
  end

  local counters = rawget(server, "_debug_counters") or {}
  local before = tonumber(counters.inbound_accept) or 0

  local sockets = {}
  for i = 1, burst do
    local sock, sock_err = socket.tcp()
    if not sock then
      close_sockets(sockets)
      close_host(server)
      return nil, sock_err
    end
    sock:settimeout(1)
    local connected, connect_err = sock:connect("127.0.0.1", port)
    if not connected then
      close_sockets(sockets)
      close_host(server)
      return nil, "connect failed at " .. tostring(i) .. ": " .. tostring(connect_err)
    end
    sock:settimeout(0)
    sockets[#sockets + 1] = sock
  end
  local callback_deadline = socket.gettime() + 5
  local listener_callbacks, listener_accepted, listener_pending, listener_failed
  repeat
    ok, err = server:_poll_once(0.001)
    if not ok then
      close_sockets(sockets)
      close_host(server)
      return nil, err
    end
    listener_callbacks, listener_accepted, listener_pending, listener_failed = listener_totals(server)
    if listener_accepted >= burst then
      break
    end
  until socket.gettime() >= callback_deadline

  -- Some OS/libuv combinations can complete client connect() calls before all
  -- accepted sockets are delivered to the Lua callback. This regression is about
  -- host-side draining once libuv has delivered sockets, so assert against the
  -- delivered count rather than treating kernel backlog timing as deterministic.
  local expected = listener_accepted
  if expected < math.min(burst, 64) then
    close_sockets(sockets)
    close_host(server)
    return nil,
      string.format(
        "too few listener accepts delivered: expected at least %d, got %d callbacks=%d pending=%d failed=%d",
        math.min(burst, 64),
        listener_accepted,
        listener_callbacks,
        listener_pending,
        listener_failed
      )
  end

  local deadline = socket.gettime() + 2
  repeat
    ok, err = server:_poll_once(0.001)
    if not ok then
      close_sockets(sockets)
      close_host(server)
      return nil, err
    end
    counters = rawget(server, "_debug_counters") or {}
    if (tonumber(counters.inbound_accept) or 0) - before >= expected then
      break
    end
  until socket.gettime() >= deadline

  local accepted = (tonumber(counters.inbound_accept) or 0) - before
  close_sockets(sockets)

  for _ = 1, 20 do
    ok, err = server:_poll_once(0.001)
    if not ok then
      close_host(server)
      return nil, err
    end
  end

  close_host(server)
  if accepted < expected then
    return nil,
      string.format(
        "expected burst accept drain >= %d, got %d listener_callbacks=%d listener_accepted=%d listener_pending=%d listener_failed=%d",
        expected,
        accepted,
        listener_callbacks,
        listener_accepted,
        listener_pending,
        listener_failed
      )
  end
  return true
end

return {
  name = "host accepts burst connections promptly",
  run = run,
}
