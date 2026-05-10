local host_mod = require("lua_libp2p.host")
local ed25519 = require("lua_libp2p.crypto.ed25519")
local identify_service = require("lua_libp2p.protocol_identify.service")

local M = {
  name = "identify throughput",
}

local ok_socket, socket = pcall(require, "socket")

local function now_seconds()
  if ok_socket and type(socket.gettime) == "function" then
    return socket.gettime()
  end
  return os.clock()
end

local function print_result(name, iterations, elapsed)
  local per_iter_ms = iterations > 0 and (elapsed * 1000 / iterations) or 0
  local per_sec = elapsed > 0 and (iterations / elapsed) or 0
  io.stdout:write(string.format(
    "  %-34s iterations=%d total_ms=%.2f per_iter_ms=%.4f ops_per_sec=%.1f\n",
    name,
    iterations,
    elapsed * 1000,
    per_iter_ms,
    per_sec
  ))
end

local function new_identity()
  local keypair, key_err = ed25519.generate_keypair()
  if not keypair then
    error(tostring(key_err))
  end
  return keypair
end

local function new_host(identity, listen_addrs)
  local h, err = host_mod.new({
    runtime = "luv",
    identity = identity,
    listen_addrs = listen_addrs,
    services = {
      identify = { module = identify_service },
    },
    blocking = false,
    poll_interval = 0.01,
    accept_timeout = 0.05,
  })
  if not h then
    error(tostring(err))
  end
  local started, start_err = h:start()
  if not started then
    error(tostring(start_err))
  end
  return h
end

function M.run()
  local server = new_host(new_identity(), { "/ip4/127.0.0.1/tcp/0" })
  local client = new_host(new_identity(), { "/ip4/127.0.0.1/tcp/0" })

  local target = server:get_multiaddrs()[1]
  if type(target) ~= "string" then
    client:close()
    server:close()
    return nil, "expected server multiaddr"
  end

  local warmup_ok, warmup_err = client.services.identify:identify(target)
  if not warmup_ok then
    client:close()
    server:close()
    return nil, warmup_err
  end

  local iterations = tonumber(os.getenv("LUA_LIBP2P_BENCH_IDENTIFY_ITERS")) or 200
  local started = now_seconds()
  for _ = 1, iterations do
    local result, identify_err = client.services.identify:identify(target)
    if not result then
      client:close()
      server:close()
      return nil, identify_err
    end
  end
  local elapsed = now_seconds() - started
  print_result("identify_serial", iterations, elapsed)

  client:close()
  server:close()
  return true
end

return M
