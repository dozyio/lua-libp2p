--- Ping protocol primitives.
-- @module lua_libp2p.protocol_ping.protocol
local ok_socket, socket = pcall(require, "socket")
local ok_sodium, sodium = pcall(require, "luasodium")
local error_mod = require("lua_libp2p.error")

local M = {}

M.ID = "/ipfs/ping/1.0.0"
M.PAYLOAD_SIZE = 32

local function now()
  if ok_socket and type(socket.gettime) == "function" then
    return socket.gettime()
  end
  return os.clock()
end

function M.new_payload()
  if ok_sodium and type(sodium.randombytes_buf) == "function" then
    return sodium.randombytes_buf(M.PAYLOAD_SIZE)
  end

  local out = {}
  for i = 1, M.PAYLOAD_SIZE do
    out[i] = string.char(math.random(0, 255))
  end
  return table.concat(out)
end

function M.handle_once(conn)
  local payload, read_err = conn:read(M.PAYLOAD_SIZE)
  if not payload then
    return nil, read_err
  end
  if #payload ~= M.PAYLOAD_SIZE then
    return nil, error_mod.new("protocol", "invalid ping payload size", { size = #payload })
  end

  local ok, write_err = conn:write(payload)
  if not ok then
    return nil, write_err
  end
  return true
end

--- Serve ping echo loop.
-- `opts.max_messages` (`number`) stops after N messages; otherwise runs until closed.
-- @tparam table conn
-- @tparam[opt] table opts
-- @treturn true|nil ok
-- @treturn[opt] table err
function M.handle(conn, opts)
  local options = opts or {}
  local max_messages = options.max_messages
  local count = 0

  while true do
    if max_messages and count >= max_messages then
      return true
    end

    local ok, err = M.handle_once(conn)
    if ok then
      count = count + 1
    else
      if error_mod.is_error(err) and (err.kind == "closed") then
        return true
      end
      return nil, err
    end
  end
end

function M.ping_once(conn, payload)
  local request = payload or M.new_payload()
  if type(request) ~= "string" or #request ~= M.PAYLOAD_SIZE then
    return nil, error_mod.new("input", "ping payload must be exactly 32 bytes")
  end

  local started_at = now()
  local ok, write_err = conn:write(request)
  if not ok then
    return nil, write_err
  end

  local response, read_err = conn:read(M.PAYLOAD_SIZE)
  if not response then
    return nil, read_err
  end

  local finished_at = now()
  if response ~= request then
    return nil, error_mod.new("protocol", "ping echo payload mismatch")
  end

  return {
    rtt_seconds = finished_at - started_at,
    payload = request,
  }
end

return M
