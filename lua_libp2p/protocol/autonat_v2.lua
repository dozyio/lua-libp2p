--- AutoNAT v2 wire protocol codec.
-- @module lua_libp2p.protocol.autonat_v2
local error_mod = require("lua_libp2p.error")
local varint = require("lua_libp2p.multiformats.varint")

local M = {}

M.DIAL_REQUEST_ID = "/libp2p/autonat/2/dial-request"
M.DIAL_BACK_ID = "/libp2p/autonat/2/dial-back"
M.MAX_MESSAGE_SIZE = 128 * 1024
M.DIAL_DATA_CHUNK_SIZE = 4096

M.RESPONSE_STATUS = {
  E_INTERNAL_ERROR = 0,
  E_REQUEST_REJECTED = 100,
  E_DIAL_REFUSED = 101,
  OK = 200,
}

M.DIAL_STATUS = {
  UNUSED = 0,
  E_DIAL_ERROR = 100,
  E_DIAL_BACK_ERROR = 101,
  OK = 200,
}

M.DIAL_BACK_STATUS = {
  OK = 0,
}

local function read_exact(conn, n)
  if n == 0 then
    return ""
  end
  local out = {}
  local remaining = n
  while remaining > 0 do
    local chunk, err = conn:read(remaining)
    if not chunk then
      return nil, err or error_mod.new("io", "unexpected EOF")
    end
    if #chunk == 0 then
      return nil, error_mod.new("io", "unexpected empty read")
    end
    out[#out + 1] = chunk
    remaining = remaining - #chunk
  end
  return table.concat(out)
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
  local value, next_or_err = varint.decode_u64(table.concat(bytes), 1)
  if not value then
    return nil, next_or_err
  end
  return value
end

local function encode_varint_field(field_no, value)
  local tag, tag_err = varint.encode_u64(field_no * 8)
  if not tag then
    return nil, tag_err
  end
  local encoded, value_err = varint.encode_u64(value)
  if not encoded then
    return nil, value_err
  end
  return tag .. encoded
end

local function encode_len_field(field_no, value)
  local tag, tag_err = varint.encode_u64(field_no * 8 + 2)
  if not tag then
    return nil, tag_err
  end
  local len, len_err = varint.encode_u64(#value)
  if not len then
    return nil, len_err
  end
  return tag .. len .. value
end

local function encode_fixed64_field(field_no, value)
  if type(value) ~= "number" or value < 0 then
    return nil, error_mod.new("input", "fixed64 field must be a non-negative number", { field = field_no })
  end
  local tag, tag_err = varint.encode_u64(field_no * 8 + 1)
  if not tag then
    return nil, tag_err
  end
  local bytes = {}
  local n = math.floor(value)
  for i = 1, 8 do
    bytes[i] = string.char(n % 256)
    n = math.floor(n / 256)
  end
  return tag .. table.concat(bytes)
end

local function decode_fixed64(bytes)
  if #bytes ~= 8 then
    return nil, error_mod.new("decode", "fixed64 must be 8 bytes")
  end
  local value = 0
  local mul = 1
  for i = 1, 8 do
    value = value + bytes:byte(i) * mul
    mul = mul * 256
  end
  return value
end

local function append_varint(parts, field_no, value)
  if value == nil then
    return true
  end
  local encoded, err = encode_varint_field(field_no, value)
  if not encoded then
    return nil, err
  end
  parts[#parts + 1] = encoded
  return true
end

local function append_len(parts, field_no, value)
  if value == nil then
    return true
  end
  if type(value) ~= "string" then
    return nil, error_mod.new("input", "bytes field must be a string", { field = field_no })
  end
  local encoded, err = encode_len_field(field_no, value)
  if not encoded then
    return nil, err
  end
  parts[#parts + 1] = encoded
  return true
end

local function append_fixed64(parts, field_no, value)
  if value == nil then
    return true
  end
  local encoded, err = encode_fixed64_field(field_no, value)
  if not encoded then
    return nil, err
  end
  parts[#parts + 1] = encoded
  return true
end

local function skip_unknown(payload, index, wire)
  if wire == 0 then
    local _, next_i_or_err = varint.decode_u64(payload, index)
    return next_i_or_err
  end
  if wire == 1 then
    if index + 7 > #payload then
      return nil, error_mod.new("decode", "truncated 64-bit protobuf field")
    end
    return index + 8
  end
  if wire == 2 then
    local length, after_len_or_err = varint.decode_u64(payload, index)
    if not length then
      return nil, after_len_or_err
    end
    if after_len_or_err + length - 1 > #payload then
      return nil, error_mod.new("decode", "truncated length-delimited protobuf field")
    end
    return after_len_or_err + length
  end
  if wire == 5 then
    if index + 3 > #payload then
      return nil, error_mod.new("decode", "truncated 32-bit protobuf field")
    end
    return index + 4
  end
  return nil, error_mod.new("decode", "unsupported protobuf wire type", { wire = wire })
end

function M.encode_dial_request(message)
  local parts = {}
  for _, addr in ipairs(message.addrs or {}) do
    local ok, err = append_len(parts, 1, addr)
    if not ok then
      return nil, err
    end
  end
  local ok, err = append_fixed64(parts, 2, message.nonce)
  if not ok then
    return nil, err
  end
  return table.concat(parts)
end

function M.decode_dial_request(payload)
  local out = { addrs = {} }
  local i = 1
  while i <= #payload do
    local tag, next_i_or_err = varint.decode_u64(payload, i)
    if not tag then
      return nil, next_i_or_err
    end
    i = next_i_or_err
    local field_no = math.floor(tag / 8)
    local wire = tag % 8
    if field_no == 1 and wire == 2 then
      local length, after_len_or_err = varint.decode_u64(payload, i)
      if not length then
        return nil, after_len_or_err
      end
      i = after_len_or_err
      out.addrs[#out.addrs + 1] = payload:sub(i, i + length - 1)
      i = i + length
    elseif field_no == 2 and wire == 1 then
      local decoded, err = decode_fixed64(payload:sub(i, i + 7))
      if not decoded then
        return nil, err
      end
      out.nonce = decoded
      i = i + 8
    else
      local next_i, err = skip_unknown(payload, i, wire)
      if not next_i then
        return nil, err
      end
      i = next_i
    end
  end
  return out
end

function M.encode_dial_data_request(message)
  local parts = {}
  local ok, err = append_varint(parts, 1, message.addrIdx)
  if not ok then
    return nil, err
  end
  ok, err = append_varint(parts, 2, message.numBytes)
  if not ok then
    return nil, err
  end
  return table.concat(parts)
end

function M.decode_dial_data_request(payload)
  local out = {}
  local i = 1
  while i <= #payload do
    local tag, next_i_or_err = varint.decode_u64(payload, i)
    if not tag then
      return nil, next_i_or_err
    end
    i = next_i_or_err
    local field_no = math.floor(tag / 8)
    local wire = tag % 8
    if (field_no == 1 or field_no == 2) and wire == 0 then
      local value, after_or_err = varint.decode_u64(payload, i)
      if not value then
        return nil, after_or_err
      end
      if field_no == 1 then
        out.addrIdx = value
      else
        out.numBytes = value
      end
      i = after_or_err
    else
      local next_i, err = skip_unknown(payload, i, wire)
      if not next_i then
        return nil, err
      end
      i = next_i
    end
  end
  return out
end

function M.encode_dial_response(message)
  local parts = {}
  local ok, err = append_varint(parts, 1, message.status)
  if not ok then
    return nil, err
  end
  ok, err = append_varint(parts, 2, message.addrIdx)
  if not ok then
    return nil, err
  end
  ok, err = append_varint(parts, 3, message.dialStatus)
  if not ok then
    return nil, err
  end
  return table.concat(parts)
end

function M.decode_dial_response(payload)
  local out = {}
  local i = 1
  while i <= #payload do
    local tag, next_i_or_err = varint.decode_u64(payload, i)
    if not tag then
      return nil, next_i_or_err
    end
    i = next_i_or_err
    local field_no = math.floor(tag / 8)
    local wire = tag % 8
    if (field_no == 1 or field_no == 2 or field_no == 3) and wire == 0 then
      local value, after_or_err = varint.decode_u64(payload, i)
      if not value then
        return nil, after_or_err
      end
      if field_no == 1 then
        out.status = value
      elseif field_no == 2 then
        out.addrIdx = value
      else
        out.dialStatus = value
      end
      i = after_or_err
    else
      local next_i, err = skip_unknown(payload, i, wire)
      if not next_i then
        return nil, err
      end
      i = next_i
    end
  end
  return out
end

function M.encode_dial_data_response(message)
  local parts = {}
  local ok, err = append_len(parts, 1, message.data or "")
  if not ok then
    return nil, err
  end
  return table.concat(parts)
end

function M.decode_dial_data_response(payload)
  local out = {}
  local i = 1
  while i <= #payload do
    local tag, next_i_or_err = varint.decode_u64(payload, i)
    if not tag then
      return nil, next_i_or_err
    end
    i = next_i_or_err
    local field_no = math.floor(tag / 8)
    local wire = tag % 8
    if field_no == 1 and wire == 2 then
      local length, after_len_or_err = varint.decode_u64(payload, i)
      if not length then
        return nil, after_len_or_err
      end
      i = after_len_or_err
      out.data = payload:sub(i, i + length - 1)
      i = i + length
    else
      local next_i, err = skip_unknown(payload, i, wire)
      if not next_i then
        return nil, err
      end
      i = next_i
    end
  end
  return out
end

function M.encode_dial_back(message)
  local parts = {}
  local ok, err = append_fixed64(parts, 1, message.nonce)
  if not ok then
    return nil, err
  end
  return table.concat(parts)
end

function M.decode_dial_back(payload)
  local out = {}
  local i = 1
  while i <= #payload do
    local tag, next_i_or_err = varint.decode_u64(payload, i)
    if not tag then
      return nil, next_i_or_err
    end
    i = next_i_or_err
    local field_no = math.floor(tag / 8)
    local wire = tag % 8
    if field_no == 1 and wire == 1 then
      local decoded, decode_err = decode_fixed64(payload:sub(i, i + 7))
      if not decoded then
        return nil, decode_err
      end
      out.nonce = decoded
      i = i + 8
    else
      local next_i, skip_err = skip_unknown(payload, i, wire)
      if not next_i then
        return nil, skip_err
      end
      i = next_i
    end
  end
  return out
end

function M.encode_dial_back_response(message)
  local parts = {}
  local ok, err = append_varint(parts, 1, message.status or M.DIAL_BACK_STATUS.OK)
  if not ok then
    return nil, err
  end
  return table.concat(parts)
end

function M.decode_dial_back_response(payload)
  local out = {}
  local i = 1
  while i <= #payload do
    local tag, next_i_or_err = varint.decode_u64(payload, i)
    if not tag then
      return nil, next_i_or_err
    end
    i = next_i_or_err
    local field_no = math.floor(tag / 8)
    local wire = tag % 8
    if field_no == 1 and wire == 0 then
      local value, after_or_err = varint.decode_u64(payload, i)
      if not value then
        return nil, after_or_err
      end
      out.status = value
      i = after_or_err
    else
      local next_i, err = skip_unknown(payload, i, wire)
      if not next_i then
        return nil, err
      end
      i = next_i
    end
  end
  return out
end

local MESSAGE_FIELDS = {
  dialRequest = { field = 1, encode = M.encode_dial_request, decode = M.decode_dial_request },
  dialResponse = { field = 2, encode = M.encode_dial_response, decode = M.decode_dial_response },
  dialDataRequest = { field = 3, encode = M.encode_dial_data_request, decode = M.decode_dial_data_request },
  dialDataResponse = { field = 4, encode = M.encode_dial_data_response, decode = M.decode_dial_data_response },
}

function M.encode_message(message)
  for name, spec in pairs(MESSAGE_FIELDS) do
    if message[name] ~= nil then
      local payload, err = spec.encode(message[name])
      if not payload then
        return nil, err
      end
      return encode_len_field(spec.field, payload)
    end
  end
  return nil, error_mod.new("input", "autonat message must set one message field")
end

function M.decode_message(payload)
  local i = 1
  while i <= #payload do
    local tag, next_i_or_err = varint.decode_u64(payload, i)
    if not tag then
      return nil, next_i_or_err
    end
    i = next_i_or_err
    local field_no = math.floor(tag / 8)
    local wire = tag % 8
    if wire == 2 then
      local length, after_len_or_err = varint.decode_u64(payload, i)
      if not length then
        return nil, after_len_or_err
      end
      i = after_len_or_err
      local value = payload:sub(i, i + length - 1)
      for name, spec in pairs(MESSAGE_FIELDS) do
        if spec.field == field_no then
          local decoded, err = spec.decode(value)
          if not decoded then
            return nil, err
          end
          return { [name] = decoded }
        end
      end
      i = i + length
    else
      local next_i, err = skip_unknown(payload, i, wire)
      if not next_i then
        return nil, err
      end
      i = next_i
    end
  end
  return nil, error_mod.new("decode", "autonat message missing known field")
end

function M.write_message(stream, message)
  local payload, payload_err = M.encode_message(message)
  if not payload then
    return nil, payload_err
  end
  local len, len_err = varint.encode_u64(#payload)
  if not len then
    return nil, len_err
  end
  return stream:write(len .. payload)
end

--- Read and decode AutoNAT message.
-- `opts.max_message_size` (`number`, default `MAX_MESSAGE_SIZE`) bounds frame size.
function M.read_message(stream, opts)
  local options = opts or {}
  local max = options.max_message_size or M.MAX_MESSAGE_SIZE
  local length, len_err = read_varint(stream)
  if not length then
    return nil, len_err
  end
  if length > max then
    return nil, error_mod.new("decode", "autonat message too large", { max = max, got = length })
  end
  local payload, payload_err = read_exact(stream, length)
  if not payload then
    return nil, payload_err
  end
  return M.decode_message(payload)
end

function M.write_dial_back(stream, message)
  local payload, payload_err = M.encode_dial_back(message)
  if not payload then
    return nil, payload_err
  end
  local len, len_err = varint.encode_u64(#payload)
  if not len then
    return nil, len_err
  end
  return stream:write(len .. payload)
end

--- Read and decode AutoNAT dial-back message.
-- `opts.max_message_size` (`number`, default `MAX_MESSAGE_SIZE`) bounds frame size.
function M.read_dial_back(stream, opts)
  local options = opts or {}
  local length, len_err = read_varint(stream)
  if not length then
    return nil, len_err
  end
  if length > (options.max_message_size or M.MAX_MESSAGE_SIZE) then
    return nil, error_mod.new("decode", "autonat dial-back message too large")
  end
  local payload, payload_err = read_exact(stream, length)
  if not payload then
    return nil, payload_err
  end
  return M.decode_dial_back(payload)
end

function M.write_dial_back_response(stream, message)
  local payload, payload_err = M.encode_dial_back_response(message or { status = M.DIAL_BACK_STATUS.OK })
  if not payload then
    return nil, payload_err
  end
  local len, len_err = varint.encode_u64(#payload)
  if not len then
    return nil, len_err
  end
  return stream:write(len .. payload)
end

--- Read and decode AutoNAT dial-back response.
-- `opts.max_message_size` (`number`, default `MAX_MESSAGE_SIZE`) bounds frame size.
function M.read_dial_back_response(stream, opts)
  local options = opts or {}
  local length, len_err = read_varint(stream)
  if not length then
    return nil, len_err
  end
  if length > (options.max_message_size or M.MAX_MESSAGE_SIZE) then
    return nil, error_mod.new("decode", "autonat dial-back response too large")
  end
  local payload, payload_err = read_exact(stream, length)
  if not payload then
    return nil, payload_err
  end
  return M.decode_dial_back_response(payload)
end

return M
