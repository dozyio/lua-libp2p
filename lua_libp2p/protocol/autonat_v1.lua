--- AutoNAT v1 wire protocol codec.
-- @module lua_libp2p.protocol.autonat_v1
local error_mod = require("lua_libp2p.error")
local varint = require("lua_libp2p.multiformats.varint")

local M = {}

M.ID = "/libp2p/autonat/1.0.0"
M.MAX_MESSAGE_SIZE = 128 * 1024

M.MESSAGE_TYPE = {
  DIAL = 0,
  DIAL_RESPONSE = 1,
}

M.RESPONSE_STATUS = {
  OK = 0,
  E_DIAL_ERROR = 100,
  E_DIAL_REFUSED = 101,
  E_BAD_REQUEST = 200,
  E_INTERNAL_ERROR = 300,
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

local function encode_peer_info(peer)
  local parts = {}
  local ok, err = append_len(parts, 1, peer.id)
  if not ok then
    return nil, err
  end
  for _, addr in ipairs(peer.addrs or {}) do
    ok, err = append_len(parts, 2, addr)
    if not ok then
      return nil, err
    end
  end
  return table.concat(parts)
end

local function decode_peer_info(payload)
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
      out.id = payload:sub(i, i + length - 1)
      i = i + length
    elseif field_no == 2 and wire == 2 then
      local length, after_len_or_err = varint.decode_u64(payload, i)
      if not length then
        return nil, after_len_or_err
      end
      i = after_len_or_err
      out.addrs[#out.addrs + 1] = payload:sub(i, i + length - 1)
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

local function encode_dial(message)
  local parts = {}
  if message.peer then
    local peer_payload, peer_err = encode_peer_info(message.peer)
    if not peer_payload then
      return nil, peer_err
    end
    local ok, err = append_len(parts, 1, peer_payload)
    if not ok then
      return nil, err
    end
  end
  return table.concat(parts)
end

local function decode_dial(payload)
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
      local decoded, err = decode_peer_info(payload:sub(i, i + length - 1))
      if not decoded then
        return nil, err
      end
      out.peer = decoded
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

local function encode_dial_response(message)
  local parts = {}
  local ok, err = append_varint(parts, 1, message.status)
  if not ok then
    return nil, err
  end
  ok, err = append_len(parts, 2, message.statusText)
  if not ok then
    return nil, err
  end
  ok, err = append_len(parts, 3, message.addr)
  if not ok then
    return nil, err
  end
  return table.concat(parts)
end

local function decode_dial_response(payload)
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
      local value, next_i_or_err2 = varint.decode_u64(payload, i)
      if not value then
        return nil, next_i_or_err2
      end
      out.status = value
      i = next_i_or_err2
    elseif field_no == 2 and wire == 2 then
      local length, after_len_or_err = varint.decode_u64(payload, i)
      if not length then
        return nil, after_len_or_err
      end
      i = after_len_or_err
      out.statusText = payload:sub(i, i + length - 1)
      i = i + length
    elseif field_no == 3 and wire == 2 then
      local length, after_len_or_err = varint.decode_u64(payload, i)
      if not length then
        return nil, after_len_or_err
      end
      i = after_len_or_err
      out.addr = payload:sub(i, i + length - 1)
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

function M.encode_message(message)
  local out = {}
  local ok, err = append_varint(out, 1, message.type)
  if not ok then
    return nil, err
  end
  if message.dial ~= nil then
    local payload, payload_err = encode_dial(message.dial)
    if not payload then
      return nil, payload_err
    end
    ok, err = append_len(out, 2, payload)
    if not ok then
      return nil, err
    end
  end
  if message.dialResponse ~= nil then
    local payload, payload_err = encode_dial_response(message.dialResponse)
    if not payload then
      return nil, payload_err
    end
    ok, err = append_len(out, 3, payload)
    if not ok then
      return nil, err
    end
  end
  return table.concat(out)
end

function M.decode_message(payload)
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
      local value, next_i_or_err2 = varint.decode_u64(payload, i)
      if not value then
        return nil, next_i_or_err2
      end
      out.type = value
      i = next_i_or_err2
    elseif field_no == 2 and wire == 2 then
      local length, after_len_or_err = varint.decode_u64(payload, i)
      if not length then
        return nil, after_len_or_err
      end
      i = after_len_or_err
      local decoded, err = decode_dial(payload:sub(i, i + length - 1))
      if not decoded then
        return nil, err
      end
      out.dial = decoded
      i = i + length
    elseif field_no == 3 and wire == 2 then
      local length, after_len_or_err = varint.decode_u64(payload, i)
      if not length then
        return nil, after_len_or_err
      end
      i = after_len_or_err
      local decoded, err = decode_dial_response(payload:sub(i, i + length - 1))
      if not decoded then
        return nil, err
      end
      out.dialResponse = decoded
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

function M.read_message(stream, opts)
  local options = opts or {}
  local max = options.max_message_size or M.MAX_MESSAGE_SIZE
  local length, len_err = read_varint(stream)
  if not length then
    return nil, len_err
  end
  if length > max then
    return nil, error_mod.new("decode", "autonat v1 message too large", { max = max, got = length })
  end
  local payload, payload_err = read_exact(stream, length)
  if not payload then
    return nil, payload_err
  end
  return M.decode_message(payload)
end

return M
