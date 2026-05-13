package.path = table.concat({
  "./?.lua",
  "./?/init.lua",
  package.path,
}, ";")

local host_mod = require("lua_libp2p.host")
local perf_service = require("lua_libp2p.protocol_perf.service")
local subprocess = require("tests.support.subprocess")

local ok_luv, uv = pcall(require, "luv")
if not ok_luv then
  error("luv is required for perf security benchmark")
end

local iterations = tonumber(arg[1]) or tonumber(os.getenv("N")) or 5
-- Keep defaults below yamux's current 256 KiB initial stream window. Larger
-- upload-before-download perf runs exercise yamux flow-control behavior in
-- addition to security transport overhead.
local upload_bytes = tonumber(os.getenv("UPLOAD_BYTES")) or 128 * 1024
local download_bytes = tonumber(os.getenv("DOWNLOAD_BYTES")) or 128 * 1024
local write_block_size = tonumber(os.getenv("WRITE_BLOCK_SIZE")) or 16 * 1024

local function security_map(protocol_id)
  if protocol_id == "/noise" then
    return { noise = true }
  end
  if protocol_id == "/tls/1.0.0" then
    return { tls = true }
  end
  error("unsupported benchmark security protocol: " .. tostring(protocol_id))
end

local function percentile(values, p)
  local idx = math.max(1, math.min(#values, math.ceil(#values * p)))
  return values[idx]
end

local function child_source()
  return [[
package.path = "./?.lua;./?/init.lua;" .. package.path
local host_mod = require("lua_libp2p.host")
local perf = require("lua_libp2p.protocol_perf.protocol")

local addr = arg[1]
local security = arg[2]
local upload_bytes = tonumber(arg[3])
local download_bytes = tonumber(arg[4])
local write_block_size = tonumber(arg[5])
local out_path = arg[6]

local function security_map(protocol_id)
  if protocol_id == "/noise" then
    return { noise = true }
  end
  if protocol_id == "/tls/1.0.0" then
    return { tls = true }
  end
  error("unsupported benchmark security protocol: " .. tostring(protocol_id))
end

local function write_out(text)
  local f = assert(io.open(out_path, "wb"))
  f:write(text)
  f:close()
end

local function err_text(err)
  if type(err) == "table" then
    local parts = {}
    if err.kind ~= nil then parts[#parts + 1] = "kind=" .. tostring(err.kind) end
    if err.message ~= nil then parts[#parts + 1] = "message=" .. tostring(err.message) end
    if err.context ~= nil then
      for k, v in pairs(err.context) do
        parts[#parts + 1] = tostring(k) .. "=" .. tostring(v)
      end
    end
    if #parts > 0 then return table.concat(parts, ";") end
  end
  return tostring(err)
end

local h, h_err = host_mod.new({
  runtime = "luv",
  blocking = false,
  security_transports = security_map(security),
})
  if not h then
  write_out("init_error:" .. err_text(h_err))
  os.exit(1)
end

local task, task_err = h:spawn_task("bench.perf_client", function(ctx)
  local stream, _, _, stream_err = h:new_stream(addr, { perf.ID }, {
    timeout = 10,
    io_timeout = 10,
    ctx = ctx,
  })
  if not stream then
    return nil, "stream_error:" .. err_text(stream_err)
  end

  local result, perf_err = perf.measure_once(stream, upload_bytes, download_bytes, {
    write_block_size = write_block_size,
  })
  if type(stream.close) == "function" then
    stream:close()
  end
  if not result then
    return nil, "perf_error:" .. err_text(perf_err)
  end
  write_out(string.format("ok %.9f %d %d", result.time_seconds, result.upload_bytes, result.download_bytes))
  return true
end, { service = "bench" })
if not task then
  write_out("task_error:" .. err_text(task_err))
  os.exit(1)
end

local ok, run_err = h:run_until_task(task, { timeout = 30, poll_interval = 0 })
h:close()
if not ok then
  write_out("run_error:" .. err_text(run_err))
  os.exit(1)
end
]]
end

local function run_once(security)
  local server, server_err = host_mod.new({
    runtime = "luv",
    blocking = false,
    listen_addrs = { "/ip4/127.0.0.1/tcp/0" },
    security_transports = security_map(security),
    services = {
      perf = { module = perf_service, config = { write_block_size = write_block_size } },
    },
    accept_timeout = 0.05,
  })
  if not server then
    return nil, server_err
  end
  local started, start_err = server:start()
  if not started then
    server:close()
    return nil, start_err
  end
  local addrs = server:get_multiaddrs()
  if #addrs == 0 then
    server:close()
    return nil, "server did not expose a dialable address"
  end

  local ok, result_or_err = subprocess.run_luv_child_file_case({
    uv = uv,
    host = server,
    child_source = child_source(),
    child_args = { addrs[1], security, tostring(upload_bytes), tostring(download_bytes), tostring(write_block_size) },
    timeout_ms = 35000,
    spawn_error = "failed to spawn perf client",
    timeout_error = "timed out waiting for perf client",
    incomplete_error = "perf client did not complete",
  })
  if not ok then
    return nil, result_or_err
  end

  local status, seconds, uploaded, downloaded = tostring(result_or_err):match("^(%S+)%s+(%S+)%s+(%S+)%s+(%S+)")
  if status ~= "ok" then
    return nil, result_or_err
  end
  return {
    seconds = tonumber(seconds),
    upload_bytes = tonumber(uploaded),
    download_bytes = tonumber(downloaded),
  }
end

local function bench(label, security)
  local values = {}
  local total = 0
  for i = 1, iterations do
    local result, err = run_once(security)
    if not result then
      error(string.format("%s iteration %d failed: %s", label, i, tostring(err)))
    end
    local ms = result.seconds * 1000
    values[#values + 1] = ms
    total = total + ms
  end
  table.sort(values)
  return {
    name = label,
    avg = total / #values,
    min = values[1],
    p50 = percentile(values, 0.50),
    p95 = percentile(values, 0.95),
    max = values[#values],
  }
end

local function print_result(r)
  print(string.format(
    "%s: avg=%.3fms min=%.3fms p50=%.3fms p95=%.3fms max=%.3fms",
    r.name,
    r.avg,
    r.min,
    r.p50,
    r.p95,
    r.max
  ))
end

print(string.format(
  "perf security benchmark n=%d upload=%dB download=%dB block=%dB",
  iterations,
  upload_bytes,
  download_bytes,
  write_block_size
))
print_result(bench("noise", "/noise"))
print_result(bench("tls", "/tls/1.0.0"))
