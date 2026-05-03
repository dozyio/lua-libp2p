package.path = table.concat({
  "/app/?.lua",
  "/app/?/init.lua",
  "./?.lua",
  "./?/init.lua",
  package.path,
}, ";")

local host_mod = require("lua_libp2p.host")
local ping_service = require("lua_libp2p.protocol_ping.service")
local ping = require("lua_libp2p.protocol_ping.protocol")

local ok_socket, socket = pcall(require, "socket")

local function now()
  if ok_socket and type(socket.gettime) == "function" then
    return socket.gettime()
  end
  return os.clock()
end

local function sleep_seconds(seconds)
  if ok_socket and type(socket.sleep) == "function" then
    socket.sleep(seconds)
    return
  end
  os.execute(string.format("sleep %.3f", seconds))
end

local function trim(s)
  return (s:gsub("^%s+", ""):gsub("%s+$", ""))
end

local function shell_quote(s)
  return "'" .. tostring(s):gsub("'", "'\\''") .. "'"
end

local function fatal(msg)
  io.stderr:write(tostring(msg) .. "\n")
  os.exit(1)
end

local function env(name)
  local value = os.getenv(name)
  if not value or value == "" then
    fatal("missing required env var: " .. name)
  end
  return value
end

local function parse_bool(raw, name)
  if raw == "true" then
    return true
  end
  if raw == "false" then
    return false
  end
  fatal("invalid boolean env var for " .. name .. ": " .. tostring(raw))
end

local function parse_runtime(raw)
  local value = raw
  if value == nil or value == "" then
    value = "luv"
  end
  if value ~= "luv" then
    fatal("invalid HOST_RUNTIME env var, expected 'luv': " .. tostring(value))
  end
  return value
end

local function run_command_capture(cmd)
  local pipe = io.popen(cmd)
  if not pipe then
    return nil, "failed to execute command"
  end
  local out = pipe:read("*a") or ""
  local ok = pipe:close()
  if ok == nil then
    return nil, "command failed"
  end
  return out
end

local function parse_redis_addr(addr)
  local host, port = addr:match("^([^:]+):(%d+)$")
  if not host then
    return nil, nil, "invalid REDIS_ADDR, expected host:port"
  end
  return host, tonumber(port), nil
end

local function redis_get(host, port, key)
  local cmd = string.format(
    "redis-cli --raw -h %s -p %d GET %s 2>/dev/null",
    shell_quote(host),
    port,
    shell_quote(key)
  )
  local out, err = run_command_capture(cmd)
  if not out then
    return nil, err
  end
  local value = trim(out)
  if value:match("^%(%s*error%)") or value:find("WRONGTYPE", 1, true) then
    return nil
  end
  if value == "" or value == "(nil)" then
    return nil
  end
  return value
end

local function redis_lindex(host, port, key)
  local cmd = string.format(
    "redis-cli --raw -h %s -p %d LINDEX %s 0 2>/dev/null",
    shell_quote(host),
    port,
    shell_quote(key)
  )
  local out, err = run_command_capture(cmd)
  if not out then
    return nil, err
  end

  local value = trim(out)
  if value:match("^%(%s*error%)") or value:find("WRONGTYPE", 1, true) then
    return nil
  end
  if value ~= "" and value ~= "(nil)" then
    return value
  end
  return nil
end

local function redis_set(host, port, key, value)
  local cmd = string.format(
    "redis-cli --raw -h %s -p %d SET %s %s 1>/dev/null 2>/dev/null",
    shell_quote(host),
    port,
    shell_quote(key),
    shell_quote(value)
  )
  local _, err = run_command_capture(cmd)
  if err then
    return nil, err
  end
  return true
end

local function redis_del(host, port, key)
  local cmd = string.format(
    "redis-cli --raw -h %s -p %d DEL %s 1>/dev/null 2>/dev/null",
    shell_quote(host),
    port,
    shell_quote(key)
  )
  local _, err = run_command_capture(cmd)
  if err then
    return nil, err
  end
  return true
end

local function redis_rpush(host, port, key, value)
  local cmd = string.format(
    "redis-cli --raw -h %s -p %d RPUSH %s %s 1>/dev/null 2>/dev/null",
    shell_quote(host),
    port,
    shell_quote(key),
    shell_quote(value)
  )
  local _, err = run_command_capture(cmd)
  if err then
    return nil, err
  end
  return true
end

local function first_ipv4()
  local out = run_command_capture("hostname -i 2>/dev/null")
  if not out then
    return nil
  end
  for token in tostring(out):gmatch("%S+") do
    if token:find(":", 1, true) == nil and token ~= "127.0.0.1" then
      return token
    end
  end
  return nil
end

local function require_matrix_values(cfg)
  if cfg.transport ~= "tcp" then
    fatal("unsupported TRANSPORT for this image: " .. tostring(cfg.transport))
  end
  if cfg.secure_channel ~= "noise" then
    fatal("unsupported SECURE_CHANNEL for this image: " .. tostring(cfg.secure_channel))
  end
  if cfg.muxer ~= "yamux" then
    fatal("unsupported MUXER for this image: " .. tostring(cfg.muxer))
  end
end

local function security_protocol_id(name)
  if name == "noise" then
    return "/noise"
  end
  fatal("unsupported SECURE_CHANNEL mapping: " .. tostring(name))
end

local function muxer_protocol_id(name)
  if name == "yamux" then
    return "/yamux/1.0.0"
  end
  fatal("unsupported MUXER mapping: " .. tostring(name))
end

local function wait_for_listener_multiaddr(redis_host, redis_port, key)
  local deadline = now() + 60
  while now() < deadline do
    local value, err = redis_get(redis_host, redis_port, key)
    if err then
      fatal("redis get failed: " .. tostring(err))
    end
    if value and value ~= "" then
      return value
    end

    local listed, list_err = redis_lindex(redis_host, redis_port, key)
    if list_err then
      fatal("redis lindex failed: " .. tostring(list_err))
    end
    if listed and listed ~= "" then
      return listed
    end

    sleep_seconds(0.2)
  end
  fatal("timed out waiting for listener multiaddr in redis")
end

local function run_listener(cfg)
  local security_protocol = security_protocol_id(cfg.secure_channel)
  local muxer_protocol = muxer_protocol_id(cfg.muxer)

  local h, host_err = host_mod.new({
    runtime = cfg.runtime,
    listen_addrs = { "/ip4/" .. cfg.listener_ip .. "/tcp/0" },
    security_transports = { security_protocol },
    muxers = { muxer_protocol },
    services = {
      ping = { module = ping_service },
    },
    blocking = false,
    connect_timeout = 5,
    io_timeout = 5,
    accept_timeout = 0.1,
  })
  if not h then
    fatal(host_err)
  end

  local ok, start_err = h:start()
  if not ok then
    fatal(start_err)
  end

  local listen_addrs = h:get_multiaddrs()
  local listen_addr = listen_addrs[1]
  if not listen_addr then
    fatal("listener has no bound multiaddr")
  end

  local port = listen_addr:match("/tcp/(%d+)")
  if not port then
    fatal("failed to parse listener tcp port")
  end

  local advertise_ip = cfg.listener_ip
  if advertise_ip == "0.0.0.0" then
    advertise_ip = first_ipv4() or "127.0.0.1"
  end

  local pid_obj = h:peer_id()
  local peer_id = pid_obj and pid_obj.id
  if not peer_id then
    fatal("failed to resolve listener peer id")
  end

  local advertised_multiaddr = string.format("/ip4/%s/tcp/%s/p2p/%s", advertise_ip, port, peer_id)

  local redis_key = cfg.test_key .. "_listener_multiaddr"
  local del_ok, del_err = redis_del(cfg.redis_host, cfg.redis_port, redis_key)
  if not del_ok then
    fatal("failed to clear listener multiaddr key in redis: " .. tostring(del_err))
  end

  local push_ok, push_err = redis_rpush(
    cfg.redis_host,
    cfg.redis_port,
    redis_key,
    advertised_multiaddr
  )
  if not push_ok then
    fatal("failed to publish listener multiaddr to redis: " .. tostring(push_err))
  end

  local run_task, run_task_err = h:spawn_task("interop.transport.listener", function(ctx)
    while true do
      local slept, sleep_err = ctx:sleep(0.1)
      if slept == nil and sleep_err then
        return nil, sleep_err
      end
    end
  end, { service = "interop" })
  if not run_task then
    fatal(run_task_err)
  end
  local _, run_err = h:run_until_task(run_task, { poll_interval = 0.05 })
  if run_err then
    fatal(run_err)
  end
end

local function run_dialer(cfg)
  local security_protocol = security_protocol_id(cfg.secure_channel)
  local muxer_protocol = muxer_protocol_id(cfg.muxer)

  local h, host_err = host_mod.new({
    runtime = cfg.runtime,
    security_transports = { security_protocol },
    muxers = { muxer_protocol },
    connect_timeout = 5,
    io_timeout = 5,
  })
  if not h then
    fatal(host_err)
  end

  local listener_addr = wait_for_listener_multiaddr(
    cfg.redis_host,
    cfg.redis_port,
    cfg.test_key .. "_listener_multiaddr"
  )

  local started = now()
  local stream, _, conn, stream_err = h:new_stream(listener_addr, { ping.ID }, {
    timeout = 5,
    io_timeout = 5,
  })
  if not stream then
    fatal(stream_err)
  end

  local ping_result, ping_err = ping.ping_once(stream)
  if not ping_result then
    fatal(ping_err)
  end

  local finished = now()

  if type(stream.close) == "function" then
    stream:close()
  end
  if conn and type(conn.close) == "function" then
    conn:close()
  end
  h:close()

  local handshake_plus_one_rtt_ms = (finished - started) * 1000
  local ping_rtt_ms = ping_result.rtt_seconds * 1000

  io.stdout:write(string.format(
    "latency:\n  handshake_plus_one_rtt: %.3f\n  ping_rtt: %.3f\n  unit: ms\n",
    handshake_plus_one_rtt_ms,
    ping_rtt_ms
  ))
end

local function main()
  local cfg = {
    debug = parse_bool(os.getenv("DEBUG") or "false", "DEBUG"),
    is_dialer = parse_bool(env("IS_DIALER"), "IS_DIALER"),
    runtime = parse_runtime(os.getenv("HOST_RUNTIME")),
    redis_addr = env("REDIS_ADDR"),
    test_key = env("TEST_KEY"),
    transport = env("TRANSPORT"),
    secure_channel = env("SECURE_CHANNEL"),
    muxer = env("MUXER"),
    listener_ip = os.getenv("LISTENER_IP") or "0.0.0.0",
  }

  local redis_host, redis_port, redis_err = parse_redis_addr(cfg.redis_addr)
  if not redis_host then
    fatal(redis_err)
  end
  cfg.redis_host = redis_host
  cfg.redis_port = redis_port

  require_matrix_values(cfg)

  if cfg.is_dialer then
    run_dialer(cfg)
  else
    run_listener(cfg)
  end
end

main()
