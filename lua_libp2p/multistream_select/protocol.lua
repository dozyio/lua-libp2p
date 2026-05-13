--- Multistream-select negotiation protocol.
local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log").subsystem("multistream")
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
    return nil,
      error_mod.new("decode", "multistream frame too large", {
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

  log.debug("multistream outbound select started", {
    protocols = table.concat(protocols, ","),
  })

  local ok, err = M.write_frame(conn, M.PROTOCOL_ID)
  if not ok then
    log.debug("multistream outbound write header failed", {
      cause = tostring(err),
    })
    return nil, err
  end

  local remote_header, header_err = M.read_frame(conn)
  if not remote_header then
    log.debug("multistream outbound read header failed", {
      cause = tostring(header_err),
    })
    return nil, header_err
  end
  log.debug("multistream outbound header received", {
    protocol = remote_header,
  })
  if remote_header ~= M.PROTOCOL_ID then
    local unexpected_err = error_mod.new("protocol", "unexpected multistream header", { received = remote_header })
    log.debug("multistream outbound unexpected header", {
      received = remote_header,
      cause = tostring(unexpected_err),
    })
    return nil, unexpected_err
  end

  for _, protocol_id in ipairs(protocols) do
    ok, err = M.write_frame(conn, protocol_id)
    if not ok then
      log.debug("multistream outbound write protocol failed", {
        protocol = protocol_id,
        cause = tostring(err),
      })
      return nil, err
    end
    log.debug("multistream outbound protocol proposed", {
      protocol = protocol_id,
    })

    local response, response_err = M.read_frame(conn)
    if not response then
      log.debug("multistream outbound read response failed", {
        protocol = protocol_id,
        cause = tostring(response_err),
      })
      return nil, response_err
    end
    log.debug("multistream outbound response received", {
      protocol = protocol_id,
      response = response,
    })

    if response == protocol_id then
      log.debug("multistream outbound protocol selected", {
        protocol = protocol_id,
      })
      return protocol_id
    end
    if response ~= M.NA then
      local unexpected_err = error_mod.new("protocol", "unexpected multistream response", {
        protocol = protocol_id,
        response = response,
      })
      log.debug("multistream outbound unexpected response", {
        protocol = protocol_id,
        response = response,
        cause = tostring(unexpected_err),
      })
      return nil, unexpected_err
    end
  end

  local unsupported_err = error_mod.new("unsupported", "no common protocol found")
  log.debug("multistream outbound select failed", {
    protocols = table.concat(protocols, ","),
    cause = tostring(unsupported_err),
  })
  return nil, unsupported_err
end

local Router = {}
Router.__index = Router

function Router:new()
  return setmetatable({
    _handlers = {},
  }, self)
end

--- Register protocol handler on router.
-- `opts` are returned from `negotiate` as handler options.
-- `opts.run_on_limited_connection` can be used by higher layers.
--- protocol_id string
--- handler function
--- opts? table
--- true|nil ok
--- table|nil err
function Router:register(protocol_id, handler, opts)
  if type(protocol_id) ~= "string" or protocol_id == "" then
    return nil, error_mod.new("input", "protocol id must be a non-empty string")
  end
  if type(handler) ~= "function" then
    return nil, error_mod.new("input", "handler must be a function")
  end
  self._handlers[protocol_id] = {
    handler = handler,
    options = opts or {},
  }
  return true
end

function Router:negotiate(conn)
  local first, first_err = M.read_frame(conn)
  if not first then
    log.debug("multistream inbound read first frame failed", {
      cause = tostring(first_err),
    })
    return nil, nil, nil, first_err
  end
  log.debug("multistream inbound first frame received", {
    frame = first,
  })

  local requested
  if first == M.PROTOCOL_ID then
    local ok, err = M.write_frame(conn, M.PROTOCOL_ID)
    if not ok then
      log.debug("multistream inbound write header failed", {
        cause = tostring(err),
      })
      return nil, nil, nil, err
    end
    log.debug("multistream inbound header acknowledged")
  else
    requested = first
  end

  while true do
    if requested == nil then
      local request_err
      requested, request_err = M.read_frame(conn)
      if not requested then
        log.debug("multistream inbound read protocol failed", {
          cause = tostring(request_err),
        })
        return nil, nil, nil, request_err
      end
    end
    log.debug("multistream inbound protocol requested", {
      protocol = requested,
    })

    local record = self._handlers[requested]
    local handler = record and record.handler or nil
    local ok, err
    if handler then
      ok, err = M.write_frame(conn, requested)
      if not ok then
        log.debug("multistream inbound write selected protocol failed", {
          protocol = requested,
          cause = tostring(err),
        })
        return nil, nil, nil, err
      end
      log.debug("multistream inbound protocol selected", {
        protocol = requested,
      })
      return requested, handler, record.options
    end

    ok, err = M.write_frame(conn, M.NA)
    if not ok then
      log.debug("multistream inbound write na failed", {
        protocol = requested,
        cause = tostring(err),
      })
      return nil, nil, nil, err
    end
    log.debug("multistream inbound protocol unsupported", {
      protocol = requested,
    })

    requested = nil
  end
end

function M.new_router()
  return Router:new()
end

return M
