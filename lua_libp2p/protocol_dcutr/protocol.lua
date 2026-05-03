local error_mod = require("lua_libp2p.error")
local multiaddr = require("lua_libp2p.multiaddr")
local varint = require("lua_libp2p.multiformats.varint")

local M = {}

M.ID = "/libp2p/dcutr"
M.MAX_MESSAGE_SIZE = 4 * 1024

M.TYPE = {
  CONNECT = 100,
  SYNC = 300,
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

function M.encode_message(message)
  if type(message) ~= "table" then
    return nil, error_mod.new("input", "dcutr message must be a table")
  end
  if type(message.type) ~= "number" then
    return nil, error_mod.new("input", "dcutr message type is required")
  end
  local parts = {}
  local encoded_type, type_err = encode_varint_field(1, message.type)
  if not encoded_type then
    return nil, type_err
  end
  parts[#parts + 1] = encoded_type
  for _, addr in ipairs(message.obs_addrs or {}) do
    local addr_bytes = addr
    if type(addr_bytes) == "string" and addr_bytes:sub(1, 1) == "/" then
      addr_bytes = multiaddr.to_bytes(addr_bytes)
    end
    if type(addr_bytes) ~= "string" then
      return nil, error_mod.new("input", "observed addresses must be bytes or text multiaddrs")
    end
    local encoded_addr, addr_err = encode_len_field(2, addr_bytes)
    if not encoded_addr then
      return nil, addr_err
    end
    parts[#parts + 1] = encoded_addr
  end
  return table.concat(parts)
end

function M.decode_message(payload)
  local out = { obs_addrs = {} }
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
      local value, value_or_err = varint.decode_u64(payload, i)
      if not value then
        return nil, value_or_err
      end
      out.type = value
      i = value_or_err
    elseif field_no == 2 and wire == 2 then
      local length, after_len_or_err = varint.decode_u64(payload, i)
      if not length then
        return nil, after_len_or_err
      end
      i = after_len_or_err
      out.obs_addrs[#out.obs_addrs + 1] = payload:sub(i, i + length - 1)
      i = i + length
    else
      local next_i, skip_err = skip_unknown(payload, i, wire)
      if not next_i then
        return nil, skip_err
      end
      i = next_i
    end
  end
  if out.type == nil then
    return nil, error_mod.new("decode", "dcutr message type is missing")
  end
  return out
end

function M.read_message(conn, opts)
  local options = opts or {}
  local max_message_size = options.max_message_size or M.MAX_MESSAGE_SIZE
  local length, len_err = read_varint(conn)
  if not length then
    return nil, len_err
  end
  if length > max_message_size then
    return nil, error_mod.new("decode", "dcutr message too large", {
      size = length,
      max_size = max_message_size,
    })
  end
  local payload, payload_err = read_exact(conn, length)
  if not payload then
    return nil, payload_err
  end
  return M.decode_message(payload)
end

function M.write_message(conn, message, opts)
  local options = opts or {}
  local max_message_size = options.max_message_size or M.MAX_MESSAGE_SIZE
  local payload, payload_err = M.encode_message(message)
  if not payload then
    return nil, payload_err
  end
  if #payload > max_message_size then
    return nil, error_mod.new("input", "dcutr message too large", {
      size = #payload,
      max_size = max_message_size,
    })
  end
  local len, len_err = varint.encode_u64(#payload)
  if not len then
    return nil, len_err
  end
  local ok, write_err = conn:write(len .. payload)
  if not ok then
    return nil, write_err
  end
  return true
end

return M
