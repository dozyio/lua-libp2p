package.path = table.concat({
  "/app/?.lua",
  "/app/?/init.lua",
  "./?.lua",
  "./?/init.lua",
  package.path,
}, ";")

local host_mod = require("lua_libp2p.host")

local ok_socket, socket = pcall(require, "socket")

local PERF_PROTOCOL_ID = "/perf/1.0.0"
local BLOCK_SIZE = 64 * 1024
local SEND_CHUNK = string.rep(string.char(0xAB), BLOCK_SIZE)

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

local function parse_int(raw, default_value)
  if not raw or raw == "" or raw == "null" then
    return default_value
  end
  local parsed = tonumber(raw)
  if not parsed then
    return default_value
  end
  return math.floor(parsed)
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
  if value == "" or value == "(nil)" then
    return nil
  end
  return value
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

local function write_bytes(stream, count)
  local remaining = count
  while remaining > 0 do
    local chunk_size = math.min(remaining, BLOCK_SIZE)
    local payload = SEND_CHUNK
    if chunk_size < BLOCK_SIZE then
      payload = payload:sub(1, chunk_size)
    end
    local ok, err = stream:write(payload)
    if not ok then
      return nil, err
    end
    remaining = remaining - chunk_size
  end
  return true
end

local function drain_bytes(stream, count)
  local remaining = count
  while remaining > 0 do
    local to_read = math.min(remaining, BLOCK_SIZE)
    local data, err = stream:read(to_read)
    if not data then
      return nil, err
    end
    remaining = remaining - #data
  end
  return true
end

local function percentile(sorted_values, p)
  local n = #sorted_values
  if n == 0 then
    return 0
  end
  local index = (p / 100) * (n - 1)
  local lower = math.floor(index) + 1
  local upper = math.ceil(index) + 1
  if lower == upper then
    return sorted_values[lower]
  end
  local weight = index - math.floor(index)
  return sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight
end

local function calculate_stats(samples)
  table.sort(samples)
  local q1 = percentile(samples, 25)
  local median = percentile(samples, 50)
  local q3 = percentile(samples, 75)
  local iqr = q3 - q1
  local lower_fence = q1 - 1.5 * iqr
  local upper_fence = q3 + 1.5 * iqr

  local outliers = {}
  local non_outliers = {}
  for _, value in ipairs(samples) do
    if value < lower_fence or value > upper_fence then
      outliers[#outliers + 1] = value
    else
      non_outliers[#non_outliers + 1] = value
    end
  end

  local min_value
  local max_value
  if #non_outliers > 0 then
    min_value = non_outliers[1]
    max_value = non_outliers[#non_outliers]
  else
    min_value = samples[1]
    max_value = samples[#samples]
  end

  return {
    min = min_value,
    q1 = q1,
    median = median,
    q3 = q3,
    max = max_value,
    outliers = outliers,
    samples = samples,
  }
end

local function format_array(values, fmt)
  if #values == 0 then
    return "[]"
  end
  local out = {}
  for i, value in ipairs(values) do
    out[i] = string.format(fmt, value)
  end
  return "[" .. table.concat(out, ", ") .. "]"
end

local function print_measurement(name, iterations, stats, unit, fmt)
  io.stdout:write(name .. ":\n")
  io.stdout:write(string.format("  iterations: %d\n", iterations))
  io.stdout:write(string.format("  min: " .. fmt .. "\n", stats.min))
  io.stdout:write(string.format("  q1: " .. fmt .. "\n", stats.q1))
  io.stdout:write(string.format("  median: " .. fmt .. "\n", stats.median))
  io.stdout:write(string.format("  q3: " .. fmt .. "\n", stats.q3))
  io.stdout:write(string.format("  max: " .. fmt .. "\n", stats.max))
  io.stdout:write(string.format("  outliers: %s\n", format_array(stats.outliers, fmt)))
  io.stdout:write(string.format("  samples: %s\n", format_array(stats.samples, fmt)))
  io.stdout:write(string.format("  unit: %s\n", unit))
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
    sleep_seconds(0.2)
  end
  fatal("timed out waiting for listener multiaddr in redis")
end

local function handle_perf_stream(stream)
  local header, header_err = stream:read(16)
  if not header then
    return nil, header_err
  end

  local upload_bytes = string.unpack(">I8", header, 1)
  local download_bytes = string.unpack(">I8", header, 9)

  local drained_ok, drained_err = drain_bytes(stream, upload_bytes)
  if not drained_ok then
    return nil, drained_err
  end

  local sent_ok, sent_err = write_bytes(stream, download_bytes)
  if not sent_ok then
    return nil, sent_err
  end

  if type(stream.close_write) == "function" then
    stream:close_write()
  end
  return true
end

local function run_listener(cfg)
  local h, host_err = host_mod.new({
    listen_addrs = { "/ip4/" .. cfg.listener_ip .. "/tcp/0" },
    security_transports = { "/noise" },
    muxers = { "/yamux/1.0.0" },
    connect_timeout = 5,
    io_timeout = 5,
    accept_timeout = 0.1,
  })
  if not h then
    fatal(host_err)
  end

  local handle_ok, handle_err = h:handle(PERF_PROTOCOL_ID, function(stream)
    local ok, err = handle_perf_stream(stream)
    if type(stream.close) == "function" then
      stream:close()
    end
    if not ok then
      io.stderr:write("perf handler failed: " .. tostring(err) .. "\n")
    end
    return true
  end)
  if not handle_ok then
    fatal(handle_err)
  end

  local start_ok, start_err = h:start({ blocking = false })
  if not start_ok then
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

  local set_ok, set_err = redis_set(
    cfg.redis_host,
    cfg.redis_port,
    cfg.test_key .. "_listener_multiaddr",
    advertised_multiaddr
  )
  if not set_ok then
    fatal("failed to write listener multiaddr to redis: " .. tostring(set_err))
  end

  while true do
    local polled, poll_err = h:poll_once(0.1)
    if not polled then
      fatal(poll_err)
    end
    sleep_seconds(0.01)
  end
end

local function run_one_iteration(h, listener_addr, upload_bytes, download_bytes)
  local started = now()
  local stream, _, conn, _, stream_err = h:new_stream(listener_addr, { PERF_PROTOCOL_ID }, {
    timeout = 10,
    io_timeout = 10,
  })
  if not stream then
    return nil, stream_err
  end

  local header = string.pack(">I8I8", upload_bytes, download_bytes)
  local write_ok, write_err = stream:write(header)
  if not write_ok then
    return nil, write_err
  end

  local send_ok, send_err = write_bytes(stream, upload_bytes)
  if not send_ok then
    return nil, send_err
  end

  local recv_ok, recv_err = drain_bytes(stream, download_bytes)
  if not recv_ok then
    return nil, recv_err
  end

  local elapsed = now() - started

  if type(stream.close) == "function" then
    stream:close()
  end
  if conn and type(conn.close) == "function" then
    conn:close()
  end

  return elapsed
end

local function run_measurement(h, listener_addr, upload_bytes, download_bytes, iterations, is_latency)
  local samples = {}
  for i = 1, iterations do
    local elapsed, err = run_one_iteration(h, listener_addr, upload_bytes, download_bytes)
    if not elapsed then
      return nil, err
    end

    if is_latency then
      samples[#samples + 1] = elapsed * 1000
    else
      local bytes = math.max(upload_bytes, download_bytes)
      samples[#samples + 1] = (bytes * 8) / elapsed / 1000000000
    end
  end
  return calculate_stats(samples)
end

local function run_dialer(cfg)
  local h, host_err = host_mod.new({
    security_transports = { "/noise" },
    muxers = { "/yamux/1.0.0" },
    connect_timeout = 10,
    io_timeout = 10,
  })
  if not h then
    fatal(host_err)
  end

  local listener_addr = wait_for_listener_multiaddr(
    cfg.redis_host,
    cfg.redis_port,
    cfg.test_key .. "_listener_multiaddr"
  )

  local upload_stats, upload_err = run_measurement(
    h,
    listener_addr,
    cfg.upload_bytes,
    0,
    cfg.upload_iterations,
    false
  )
  if not upload_stats then
    fatal(upload_err)
  end

  local download_stats, download_err = run_measurement(
    h,
    listener_addr,
    0,
    cfg.download_bytes,
    cfg.download_iterations,
    false
  )
  if not download_stats then
    fatal(download_err)
  end

  local latency_stats, latency_err = run_measurement(
    h,
    listener_addr,
    1,
    1,
    cfg.latency_iterations,
    true
  )
  if not latency_stats then
    fatal(latency_err)
  end

  h:close()

  print_measurement("upload", cfg.upload_iterations, upload_stats, "Gbps", "%.2f")
  print_measurement("download", cfg.download_iterations, download_stats, "Gbps", "%.2f")
  print_measurement("latency", cfg.latency_iterations, latency_stats, "ms", "%.3f")
end

local function main()
  local cfg = {
    is_dialer = parse_bool(env("IS_DIALER"), "IS_DIALER"),
    redis_addr = env("REDIS_ADDR"),
    test_key = env("TEST_KEY"),
    transport = env("TRANSPORT"),
    secure_channel = env("SECURE_CHANNEL"),
    muxer = env("MUXER"),
    listener_ip = os.getenv("LISTENER_IP") or "0.0.0.0",
    upload_bytes = parse_int(os.getenv("UPLOAD_BYTES"), 1073741824),
    download_bytes = parse_int(os.getenv("DOWNLOAD_BYTES"), 1073741824),
    upload_iterations = parse_int(os.getenv("UPLOAD_ITERATIONS"), 10),
    download_iterations = parse_int(os.getenv("DOWNLOAD_ITERATIONS"), 10),
    latency_iterations = parse_int(os.getenv("LATENCY_ITERATIONS"), 100),
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
