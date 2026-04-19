local error_mod = require("lua_libp2p.error")
local varint = require("lua_libp2p.multiformats.varint")

local M = {}

M.PROTOCOL_ID = "/multistream/1.0.0"
M.NA = "na"
M.MAX_PROTOCOL_LENGTH = 1024

local function ensure_newline(value)
  if value:sub(-1) == "\n" then
    return value
  end
  return value .. "\n"
end

local function strip_newline(value)
  if value:sub(-1) == "\n" then
    return value:sub(1, -2)
  end
  return value
end

local function read_exact(conn, n)
  if n == 0 then
    return ""
  end

  local parts = {}
  local remaining = n
  while remaining > 0 do
    local chunk, err = conn:read(remaining)
    if not chunk then
      return nil, err or error_mod.new("io", "unexpected EOF")
    end
    if #chunk == 0 then
      return nil, error_mod.new("io", "unexpected empty read")
    end

    if #chunk > remaining then
      parts[#parts + 1] = chunk:sub(1, remaining)
      remaining = 0
    else
      parts[#parts + 1] = chunk
      remaining = remaining - #chunk
    end
  end

  return table.concat(parts)
end

local function read_varint(conn)
  local bytes = {}
  while true do
    local b, err = read_exact(conn, 1)
    if not b then
      return nil, err
    end
    bytes[#bytes + 1] = b
    if b:byte(1) < 128 then
      break
    end
    if #bytes > 10 then
      return nil, error_mod.new("decode", "varint too long")
    end
  end

  local value, next_i_or_err = varint.decode_u64(table.concat(bytes), 1)
  if not value then
    return nil, next_i_or_err
  end
  return value
end

function M.encode_frame(message)
  if type(message) ~= "string" then
    return nil, error_mod.new("input", "frame message must be a string")
  end
  local payload = ensure_newline(message)
  local len, len_err = varint.encode_u64(#payload)
  if not len then
    return nil, len_err
  end
  return len .. payload
end

function M.write_frame(conn, message)
  local frame, frame_err = M.encode_frame(message)
  if not frame then
    return nil, frame_err
  end
  local ok, err = conn:write(frame)
  if not ok then
    return nil, err
  end
  return true
end

function M.read_frame(conn)
  local length, len_err = read_varint(conn)
  if not length then
    return nil, len_err
  end
  if length > M.MAX_PROTOCOL_LENGTH then
    return nil, error_mod.new("decode", "multistream frame too large", {
      max = M.MAX_PROTOCOL_LENGTH,
      got = length,
    })
  end

  local payload, payload_err = read_exact(conn, length)
  if not payload then
    return nil, payload_err
  end

  if payload:sub(-1) ~= "\n" then
    if payload:find("\n", 1, true) then
      return nil, error_mod.new("decode", "frame missing newline terminator")
    end
    return payload
  end

  return strip_newline(payload)
end

function M.select(conn, protocol_ids)
  local protocols = protocol_ids
  if type(protocols) == "string" then
    protocols = { protocols }
  end
  if type(protocols) ~= "table" or #protocols == 0 then
    return nil, error_mod.new("input", "protocol_ids must be non-empty")
  end

  local ok, err = M.write_frame(conn, M.PROTOCOL_ID)
  if not ok then
    return nil, err
  end

  local remote_header, header_err = M.read_frame(conn)
  if not remote_header then
    return nil, header_err
  end
  if remote_header ~= M.PROTOCOL_ID then
    return nil, error_mod.new("protocol", "unexpected multistream header", { received = remote_header })
  end

  for _, protocol_id in ipairs(protocols) do
    ok, err = M.write_frame(conn, protocol_id)
    if not ok then
      return nil, err
    end

    local response, response_err = M.read_frame(conn)
    if not response then
      return nil, response_err
    end

    if response == protocol_id then
      return protocol_id
    end
    if response ~= M.NA then
      return nil, error_mod.new("protocol", "unexpected multistream response", {
        protocol = protocol_id,
        response = response,
      })
    end
  end

  return nil, error_mod.new("unsupported", "no common protocol found")
end

local Router = {}
Router.__index = Router

function Router:new()
  return setmetatable({
    _handlers = {},
  }, self)
end

function Router:register(protocol_id, handler)
  if type(protocol_id) ~= "string" or protocol_id == "" then
    return nil, error_mod.new("input", "protocol id must be a non-empty string")
  end
  if type(handler) ~= "function" then
    return nil, error_mod.new("input", "handler must be a function")
  end
  self._handlers[protocol_id] = handler
  return true
end

function Router:negotiate(conn)
  local first, first_err = M.read_frame(conn)
  if not first then
    return nil, nil, first_err
  end

  local requested
  if first == M.PROTOCOL_ID then
    local ok, err = M.write_frame(conn, M.PROTOCOL_ID)
    if not ok then
      return nil, nil, err
    end
  else
    requested = first
  end

  while true do
    if requested == nil then
      local request_err
      requested, request_err = M.read_frame(conn)
      if not requested then
        return nil, nil, request_err
      end
    end

    local handler = self._handlers[requested]
    local ok, err
    if handler then
      ok, err = M.write_frame(conn, requested)
      if not ok then
        return nil, nil, err
      end
      return requested, handler
    end

    ok, err = M.write_frame(conn, M.NA)
    if not ok then
      return nil, nil, err
    end

    requested = nil
  end
end

function M.new_router()
  return Router:new()
end

return M
