--- Perf protocol helpers and measurement utilities.
-- @module lua_libp2p.protocol_perf.protocol
local ok_socket, socket = pcall(require, "socket")
local error_mod = require("lua_libp2p.error")

local M = {}

M.ID = "/perf/1.0.0"
M.DEFAULT_WRITE_BLOCK_SIZE = 16384
M.MAX_WRITE_BLOCK_SIZE = 65535
M.MAX_SAFE_U64 = 9007199254740991

local function now()
  if ok_socket and type(socket.gettime) == "function" then
    return socket.gettime()
  end
  return os.clock()
end

local function maybe_yield()
  local co, is_main = coroutine.running()
  if co and not is_main then
    coroutine.yield("perf")
  end
end

local function is_closed_error(err)
  if err == nil then
    return false
  end
  if error_mod.is_error(err) then
    return err.kind == "closed"
  end
  if type(err) == "string" then
    return err == "closed" or err:find("closed", 1, true) ~= nil
  end
  return false
end

local function closed_partial(err)
  if not error_mod.is_error(err) then
    return nil
  end
  if type(err.context) ~= "table" then
    return nil
  end
  if type(err.context.partial) == "string" then
    return err.context.partial
  end
  return nil
end

local function u32be(n)
  local b1 = math.floor(n / 16777216) % 256
  local b2 = math.floor(n / 65536) % 256
  local b3 = math.floor(n / 256) % 256
  local b4 = n % 256
  return string.char(b1, b2, b3, b4)
end

local function parse_u32be(bytes, offset)
  local a, b, c, d = string.byte(bytes, offset, offset + 3)
  return (((a * 256 + b) * 256 + c) * 256 + d)
end

function M.encode_u64be(value)
  if type(value) ~= "number" then
    return nil, error_mod.new("input", "perf u64 value must be a number")
  end
  if value < 0 or value > M.MAX_SAFE_U64 or value % 1 ~= 0 then
    return nil, error_mod.new("input", "perf u64 value out of range", { value = value })
  end

  local hi = math.floor(value / 4294967296)
  local lo = value % 4294967296
  return u32be(hi) .. u32be(lo)
end

function M.decode_u64be(bytes)
  if type(bytes) ~= "string" or #bytes ~= 8 then
    return nil, error_mod.new("input", "perf u64 input must be exactly 8 bytes")
  end
  local hi = parse_u32be(bytes, 1)
  local lo = parse_u32be(bytes, 5)
  return hi * 4294967296 + lo
end

local function write_repeated(conn, total_bytes, block_size, opts)
  local options = opts or {}
  local yield_every = options.yield_every_bytes
  local since_yield = 0
  local zero_block = string.rep("\0", block_size)
  local remaining = total_bytes
  while remaining > 0 do
    local to_write = block_size
    if to_write > remaining then
      to_write = remaining
    end
    local payload = zero_block
    if to_write ~= block_size then
      payload = zero_block:sub(1, to_write)
    end
    local ok, err = conn:write(payload)
    if not ok then
      return nil, err
    end
    remaining = remaining - to_write
    if type(yield_every) == "number" and yield_every > 0 then
      since_yield = since_yield + to_write
      if since_yield >= yield_every then
        since_yield = 0
        maybe_yield()
      end
    end
  end
  return true
end

local function read_upload_until_eof(conn, opts)
  local options = opts or {}
  local yield_every = options.yield_every_bytes
  local since_yield = 0
  local total = 0

  while true do
    if type(conn.read_now) == "function" then
      local immediate = conn:read_now()
      if immediate and #immediate > 0 then
        total = total + #immediate
        if type(yield_every) == "number" and yield_every > 0 then
          since_yield = since_yield + #immediate
          if since_yield >= yield_every then
            since_yield = 0
            maybe_yield()
          end
        end
      else
        local chunk, err = conn:read(1)
        if chunk then
          total = total + #chunk
          if type(yield_every) == "number" and yield_every > 0 then
            since_yield = since_yield + #chunk
            if since_yield >= yield_every then
              since_yield = 0
              maybe_yield()
            end
          end
        elseif is_closed_error(err) then
          local partial = closed_partial(err)
          if partial and #partial > 0 then
            total = total + #partial
          end
          return total
        else
          return nil, err
        end
      end
    else
      local chunk, err = conn:read(1)
      if chunk then
        total = total + #chunk
        if type(yield_every) == "number" and yield_every > 0 then
          since_yield = since_yield + #chunk
          if since_yield >= yield_every then
            since_yield = 0
            maybe_yield()
          end
        end
      elseif is_closed_error(err) then
        local partial = closed_partial(err)
        if partial and #partial > 0 then
          total = total + #partial
        end
        return total
      else
        return nil, err
      end
    end
  end
end

--- Handle one perf protocol session.
-- `opts.write_block_size` (`number`, default `DEFAULT_WRITE_BLOCK_SIZE`) controls send chunks.
-- `opts.yield_every_bytes` (`number`) sets periodic cooperative yield cadence.
-- @tparam table conn
-- @tparam[opt] table opts
-- @treturn table|nil stats
-- @treturn[opt] table err
function M.handle(conn, opts)
  local options = opts or {}
  local write_block_size = options.write_block_size or M.DEFAULT_WRITE_BLOCK_SIZE
  local yield_every = options.yield_every_bytes or (write_block_size * 64)
  if type(write_block_size) ~= "number" or write_block_size <= 0 then
    return nil, error_mod.new("input", "perf write_block_size must be positive")
  end
  if write_block_size > M.MAX_WRITE_BLOCK_SIZE then
    return nil, error_mod.new("input", "perf write_block_size too large", {
      max = M.MAX_WRITE_BLOCK_SIZE,
      got = write_block_size,
    })
  end

  local requested_bytes_be, read_err = conn:read(8)
  if not requested_bytes_be then
    return nil, read_err
  end

  local bytes_to_send, decode_err = M.decode_u64be(requested_bytes_be)
  if not bytes_to_send then
    return nil, decode_err
  end

  local uploaded_bytes, upload_err = read_upload_until_eof(conn, {
    yield_every_bytes = yield_every,
  })
  if uploaded_bytes == nil then
    return nil, upload_err
  end

  local ok, write_err = write_repeated(conn, bytes_to_send, write_block_size, {
    yield_every_bytes = yield_every,
  })
  if not ok then
    return nil, write_err
  end

  if type(conn.close_write) == "function" then
    local closed_ok, close_err = conn:close_write()
    if closed_ok == nil then
      return nil, close_err
    end
  end

  return {
    uploaded_bytes = uploaded_bytes,
    download_bytes = bytes_to_send,
  }
end

--- Run one perf upload/download measurement exchange.
-- `opts.write_block_size` (`number`, default `DEFAULT_WRITE_BLOCK_SIZE`) controls send chunks.
-- @tparam table conn
-- @tparam number send_bytes
-- @tparam number recv_bytes
-- @tparam[opt] table opts
-- @treturn table|nil report
-- @treturn[opt] table err
function M.measure_once(conn, send_bytes, recv_bytes, opts)
  local options = opts or {}
  local write_block_size = options.write_block_size or M.DEFAULT_WRITE_BLOCK_SIZE
  if type(write_block_size) ~= "number" or write_block_size <= 0 then
    return nil, error_mod.new("input", "perf write_block_size must be positive")
  end
  if write_block_size > M.MAX_WRITE_BLOCK_SIZE then
    return nil, error_mod.new("input", "perf write_block_size too large", {
      max = M.MAX_WRITE_BLOCK_SIZE,
      got = write_block_size,
    })
  end
  if type(send_bytes) ~= "number" or send_bytes < 0 or send_bytes % 1 ~= 0 then
    return nil, error_mod.new("input", "perf send_bytes must be a non-negative integer")
  end
  if type(recv_bytes) ~= "number" or recv_bytes < 0 or recv_bytes % 1 ~= 0 then
    return nil, error_mod.new("input", "perf recv_bytes must be a non-negative integer")
  end

  local recv_be, encode_err = M.encode_u64be(recv_bytes)
  if not recv_be then
    return nil, encode_err
  end

  local started = now()

  local ok, write_err = conn:write(recv_be)
  if not ok then
    return nil, write_err
  end

  ok, write_err = write_repeated(conn, send_bytes, write_block_size)
  if not ok then
    return nil, write_err
  end

  if type(conn.close_write) == "function" then
    local closed_ok, close_err = conn:close_write()
    if closed_ok == nil then
      return nil, close_err
    end
  end

  local downloaded = 0
  local remaining = recv_bytes
  while remaining > 0 do
    local want = write_block_size
    if want > remaining then
      want = remaining
    end
    local chunk, read_err = conn:read(want)
    if not chunk then
      return nil, read_err
    end
    downloaded = downloaded + #chunk
    remaining = remaining - #chunk
  end

  local finished = now()
  return {
    time_seconds = finished - started,
    upload_bytes = send_bytes,
    download_bytes = downloaded,
  }
end

return M
