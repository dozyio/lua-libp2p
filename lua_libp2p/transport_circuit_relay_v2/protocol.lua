--- Circuit Relay v2 protocol codec and helpers.
---@class Libp2pRelayPeer
---@field id? string
---@field peer_id? string
---@field addrs? string[]

---@class Libp2pRelayReservation
---@field expire? integer
---@field addrs? string[]
---@field voucher? string

---@class Libp2pRelayLimit
---@field duration? integer
---@field data? integer

---@class Libp2pRelayMessage
---@field type integer
---@field peer? Libp2pRelayPeer
---@field reservation? Libp2pRelayReservation
---@field limit? Libp2pRelayLimit
---@field status? integer

local error_mod = require("lua_libp2p.error")
local multiaddr = require("lua_libp2p.multiformats.multiaddr")
local peerid = require("lua_libp2p.peerid")
local varint = require("lua_libp2p.multiformats.varint")

local M = {}

M.HOP_ID = "/libp2p/circuit/relay/0.2.0/hop"
M.STOP_ID = "/libp2p/circuit/relay/0.2.0/stop"
M.MAX_MESSAGE_SIZE = 64 * 1024

M.HOP_TYPE = {
  RESERVE = 0,
  CONNECT = 1,
  STATUS = 2,
}

M.STOP_TYPE = {
  CONNECT = 0,
  STATUS = 1,
}

M.TYPE = M.HOP_TYPE

M.STATUS = {
  OK = 100,
  RESERVATION_REFUSED = 200,
  RESOURCE_LIMIT_EXCEEDED = 201,
  PERMISSION_DENIED = 202,
  CONNECTION_FAILED = 203,
  NO_RESERVATION = 204,
  MALFORMED_MESSAGE = 400,
  UNEXPECTED_MESSAGE = 401,
}

local STATUS_NAME = {}
for name, code in pairs(M.STATUS) do
  STATUS_NAME[code] = name
end

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

local function append_varint_field(parts, field_no, value)
  if value == nil then
    return true
  end
  if type(value) ~= "number" then
    return nil, error_mod.new("input", "protobuf varint field must be a number", { field = field_no })
  end
  local encoded, err = encode_varint_field(field_no, value)
  if not encoded then
    return nil, err
  end
  parts[#parts + 1] = encoded
  return true
end

local function append_len_field(parts, field_no, value)
  if value == nil then
    return true
  end
  if type(value) ~= "string" then
    return nil, error_mod.new("input", "protobuf bytes field must be a string", { field = field_no })
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
    if not next_i_or_err then
      return nil, next_i_or_err
    end
    return next_i_or_err
  end
  if wire == 1 then
    local finish = index + 7
    if finish > #payload then
      return nil, error_mod.new("decode", "truncated 64-bit protobuf field")
    end
    return finish + 1
  end
  if wire == 2 then
    local length, after_len_or_err = varint.decode_u64(payload, index)
    if not length then
      return nil, after_len_or_err
    end
    local finish = after_len_or_err + length - 1
    if finish > #payload then
      return nil, error_mod.new("decode", "truncated length-delimited protobuf field")
    end
    return finish + 1
  end
  if wire == 5 then
    local finish = index + 3
    if finish > #payload then
      return nil, error_mod.new("decode", "truncated 32-bit protobuf field")
    end
    return finish + 1
  end
  return nil, error_mod.new("decode", "unsupported protobuf wire type", { wire = wire })
end

local function peer_bytes(value)
  if type(value) ~= "string" or value == "" then
    return nil, error_mod.new("input", "peer id must be non-empty")
  end
  local parsed = peerid.parse(value)
  if parsed then
    return parsed.bytes
  end
  return value
end

local function normalize_addr_bytes(addr)
  if type(addr) ~= "string" then
    return nil, error_mod.new("input", "multiaddr must be bytes or text")
  end
  if addr:sub(1, 1) == "/" then
    return multiaddr.to_bytes(addr)
  end
  return addr
end

function M.status_name(code)
  return STATUS_NAME[code] or tostring(code)
end

function M.classify_limit(limit)
  if type(limit) ~= "table" then
    return "unlimited"
  end
  local duration = tonumber(limit.duration or 0) or 0
  local data = tonumber(limit.data or 0) or 0
  if duration > 0 or data > 0 then
    return "limited"
  end
  return "unlimited"
end

--- peer Libp2pRelayPeer
--- string|nil payload
--- table|nil err
function M.encode_peer(peer)
  if type(peer) ~= "table" then
    return nil, error_mod.new("input", "relay peer must be a table")
  end
  local id, id_err = peer_bytes(peer.id or peer.peer_id)
  if not id then
    return nil, id_err
  end
  local parts = {}
  local ok, err = append_len_field(parts, 1, id)
  if not ok then
    return nil, err
  end
  for _, addr in ipairs(peer.addrs or {}) do
    local addr_bytes, addr_err = normalize_addr_bytes(addr)
    if not addr_bytes then
      return nil, addr_err
    end
    ok, err = append_len_field(parts, 2, addr_bytes)
    if not ok then
      return nil, err
    end
  end
  return table.concat(parts)
end

--- payload string
--- Libp2pRelayPeer|nil peer
--- table|nil err
function M.decode_peer(payload)
  local out = { addrs = {} }
  local i = 1
  while i <= #payload do
    local tag, next_i_or_err = varint.decode_u64(payload, i)
    if not tag then
      return nil, next_i_or_err
    end
    local field_no = math.floor(tag / 8)
    local wire = tag % 8
    i = next_i_or_err
    if wire ~= 2 then
      local next_i, skip_err = skip_unknown(payload, i, wire)
      if not next_i then
        return nil, skip_err
      end
      i = next_i
      goto continue
    end
    local length, after_len_or_err = varint.decode_u64(payload, i)
    if not length then
      return nil, after_len_or_err
    end
    local finish = after_len_or_err + length - 1
    if finish > #payload then
      return nil, error_mod.new("decode", "truncated relay peer field")
    end
    local value = payload:sub(after_len_or_err, finish)
    i = finish + 1
    if field_no == 1 then
      out.id = value
    elseif field_no == 2 then
      out.addrs[#out.addrs + 1] = value
    end
    ::continue::
  end
  if type(out.id) ~= "string" then
    return nil, error_mod.new("decode", "relay peer missing id")
  end
  return out
end

--- reservation Libp2pRelayReservation
--- string|nil payload
--- table|nil err
function M.encode_reservation(reservation)
  if type(reservation) ~= "table" then
    return nil, error_mod.new("input", "reservation must be a table")
  end
  local parts = {}
  local ok, err = append_varint_field(parts, 1, reservation.expire)
  if not ok then
    return nil, err
  end
  for _, addr in ipairs(reservation.addrs or {}) do
    local addr_bytes, addr_err = normalize_addr_bytes(addr)
    if not addr_bytes then
      return nil, addr_err
    end
    ok, err = append_len_field(parts, 2, addr_bytes)
    if not ok then
      return nil, err
    end
  end
  ok, err = append_len_field(parts, 3, reservation.voucher)
  if not ok then
    return nil, err
  end
  return table.concat(parts)
end

--- payload string
--- Libp2pRelayReservation|nil reservation
--- table|nil err
function M.decode_reservation(payload)
  local out = { addrs = {} }
  local i = 1
  while i <= #payload do
    local tag, next_i_or_err = varint.decode_u64(payload, i)
    if not tag then
      return nil, next_i_or_err
    end
    local field_no = math.floor(tag / 8)
    local wire = tag % 8
    i = next_i_or_err
    if field_no == 1 and wire == 0 then
      local value, after_value_or_err = varint.decode_u64(payload, i)
      if not value then
        return nil, after_value_or_err
      end
      out.expire = value
      i = after_value_or_err
      goto continue
    end
    if wire ~= 2 then
      local next_i, skip_err = skip_unknown(payload, i, wire)
      if not next_i then
        return nil, skip_err
      end
      i = next_i
      goto continue
    end
    local length, after_len_or_err = varint.decode_u64(payload, i)
    if not length then
      return nil, after_len_or_err
    end
    local finish = after_len_or_err + length - 1
    if finish > #payload then
      return nil, error_mod.new("decode", "truncated reservation field")
    end
    local value = payload:sub(after_len_or_err, finish)
    i = finish + 1
    if field_no == 2 then
      out.addrs[#out.addrs + 1] = value
    elseif field_no == 3 then
      out.voucher = value
    end
    ::continue::
  end
  return out
end

--- limit Libp2pRelayLimit
--- string|nil payload
--- table|nil err
function M.encode_limit(limit)
  if type(limit) ~= "table" then
    return nil, error_mod.new("input", "limit must be a table")
  end
  local parts = {}
  local ok, err = append_varint_field(parts, 1, limit.duration)
  if not ok then
    return nil, err
  end
  ok, err = append_varint_field(parts, 2, limit.data)
  if not ok then
    return nil, err
  end
  return table.concat(parts)
end

--- payload string
--- Libp2pRelayLimit|nil limit
--- table|nil err
function M.decode_limit(payload)
  local out = {}
  local i = 1
  while i <= #payload do
    local tag, next_i_or_err = varint.decode_u64(payload, i)
    if not tag then
      return nil, next_i_or_err
    end
    local field_no = math.floor(tag / 8)
    local wire = tag % 8
    i = next_i_or_err
    if wire ~= 0 then
      local next_i, skip_err = skip_unknown(payload, i, wire)
      if not next_i then
        return nil, skip_err
      end
      i = next_i
      goto continue
    end
    local value, after_value_or_err = varint.decode_u64(payload, i)
    if not value then
      return nil, after_value_or_err
    end
    i = after_value_or_err
    if field_no == 1 then
      out.duration = value
    elseif field_no == 2 then
      out.data = value
    end
    ::continue::
  end
  return out
end

local HOP_FIELDS = { reservation = 3, limit = 4, status = 5 }
local STOP_FIELDS = { limit = 3, status = 4 }

local function encode_message(message, fields)
  if type(message) ~= "table" then
    return nil, error_mod.new("input", "relay message must be a table")
  end
  local field_map = fields or HOP_FIELDS
  local parts = {}
  local ok, err = append_varint_field(parts, 1, message.type)
  if not ok then
    return nil, err
  end
  if message.peer then
    local encoded, peer_err = M.encode_peer(message.peer)
    if not encoded then
      return nil, peer_err
    end
    ok, err = append_len_field(parts, 2, encoded)
    if not ok then
      return nil, err
    end
  end
  if message.reservation then
    if not field_map.reservation then
      return nil, error_mod.new("input", "relay message type does not support reservations")
    end
    local encoded, reservation_err = M.encode_reservation(message.reservation)
    if not encoded then
      return nil, reservation_err
    end
    ok, err = append_len_field(parts, field_map.reservation, encoded)
    if not ok then
      return nil, err
    end
  end
  if message.limit then
    local encoded, limit_err = M.encode_limit(message.limit)
    if not encoded then
      return nil, limit_err
    end
    ok, err = append_len_field(parts, field_map.limit, encoded)
    if not ok then
      return nil, err
    end
  end
  ok, err = append_varint_field(parts, field_map.status, message.status)
  if not ok then
    return nil, err
  end
  return table.concat(parts)
end

local function decode_message(payload, fields)
  if type(payload) ~= "string" then
    return nil, error_mod.new("input", "relay payload must be bytes")
  end
  local field_map = fields or HOP_FIELDS
  local out = {}
  local i = 1
  while i <= #payload do
    local tag, next_i_or_err = varint.decode_u64(payload, i)
    if not tag then
      return nil, next_i_or_err
    end
    local field_no = math.floor(tag / 8)
    local wire = tag % 8
    i = next_i_or_err
    if (field_no == 1 or field_no == field_map.status) and wire == 0 then
      local value, after_value_or_err = varint.decode_u64(payload, i)
      if not value then
        return nil, after_value_or_err
      end
      if field_no == 1 then
        out.type = value
      else
        out.status = value
      end
      i = after_value_or_err
      goto continue
    end
    if wire ~= 2 then
      local next_i, skip_err = skip_unknown(payload, i, wire)
      if not next_i then
        return nil, skip_err
      end
      i = next_i
      goto continue
    end
    local length, after_len_or_err = varint.decode_u64(payload, i)
    if not length then
      return nil, after_len_or_err
    end
    local finish = after_len_or_err + length - 1
    if finish > #payload then
      return nil, error_mod.new("decode", "truncated relay message field")
    end
    local value = payload:sub(after_len_or_err, finish)
    i = finish + 1
    if field_no == 2 then
      local peer, peer_err = M.decode_peer(value)
      if not peer then
        return nil, peer_err
      end
      out.peer = peer
    elseif field_map.reservation and field_no == field_map.reservation then
      local reservation, reservation_err = M.decode_reservation(value)
      if not reservation then
        return nil, reservation_err
      end
      out.reservation = reservation
    elseif field_no == field_map.limit then
      local limit, limit_err = M.decode_limit(value)
      if not limit then
        return nil, limit_err
      end
      out.limit = limit
    end
    ::continue::
  end
  return out
end

--- message Libp2pRelayMessage
--- string|nil payload
--- table|nil err
function M.encode_hop_message(message)
  return encode_message(message, HOP_FIELDS)
end

--- payload string
--- Libp2pRelayMessage|nil message
--- table|nil err
function M.decode_hop_message(payload)
  return decode_message(payload, HOP_FIELDS)
end

--- message Libp2pRelayMessage
--- string|nil payload
--- table|nil err
function M.encode_stop_message(message)
  return encode_message(message, STOP_FIELDS)
end

--- payload string
--- Libp2pRelayMessage|nil message
--- table|nil err
function M.decode_stop_message(payload)
  return decode_message(payload, STOP_FIELDS)
end

local function write_message(stream, message, fields)
  local payload, payload_err = encode_message(message, fields)
  if not payload then
    return nil, payload_err
  end
  local len, len_err = varint.encode_u64(#payload)
  if not len then
    return nil, len_err
  end
  return stream:write(len .. payload)
end

local function read_message(stream, opts, fields)
  local options = opts or {}
  local length, len_err = read_varint(stream)
  if not length then
    return nil, len_err
  end
  local max = options.max_message_size or M.MAX_MESSAGE_SIZE
  if length > max then
    return nil, error_mod.new("decode", "relay message too large", { max = max, got = length })
  end
  local payload, payload_err = read_exact(stream, length)
  if not payload then
    return nil, payload_err
  end
  return decode_message(payload, fields)
end

function M.write_message(stream, message)
  return write_message(stream, message, HOP_FIELDS)
end

--- Read generic HOP message.
-- `opts.max_message_size` (`number`, default `MAX_MESSAGE_SIZE`) bounds frame size.
function M.read_message(stream, opts)
  return read_message(stream, opts, HOP_FIELDS)
end

function M.write_hop_message(stream, message)
  return write_message(stream, message, HOP_FIELDS)
end

--- Read HOP message.
-- `opts.max_message_size` (`number`, default `MAX_MESSAGE_SIZE`) bounds frame size.
function M.read_hop_message(stream, opts)
  return read_message(stream, opts, HOP_FIELDS)
end

function M.write_stop_message(stream, message)
  return write_message(stream, message, STOP_FIELDS)
end

--- Read STOP message.
-- `opts.max_message_size` (`number`, default `MAX_MESSAGE_SIZE`) bounds frame size.
function M.read_stop_message(stream, opts)
  return read_message(stream, opts, STOP_FIELDS)
end

--- Perform a relay RESERVE request/response exchange.
-- `opts.max_message_size` (`number`, default `MAX_MESSAGE_SIZE`) bounds response size.
--- stream table
--- opts? table
--- table|nil reservation
--- table|nil err_or_response
function M.reserve(stream, opts)
  local options = opts or {}
  local wrote, write_err = M.write_hop_message(stream, { type = M.HOP_TYPE.RESERVE })
  if not wrote then
    return nil, write_err
  end
  local response, read_err = M.read_hop_message(stream, options)
  if not response then
    return nil, read_err
  end
  if response.type ~= M.HOP_TYPE.STATUS then
    return nil, error_mod.new("protocol", "unexpected relay reserve response type", { got = response.type })
  end
  if response.status ~= M.STATUS.OK then
    return nil,
      error_mod.new("protocol", "relay reservation failed", {
        status = response.status,
        status_name = M.status_name(response.status),
      })
  end
  return response.reservation or {}, response
end

--- Perform a relay CONNECT request/response exchange.
-- `opts.max_message_size` (`number`, default `MAX_MESSAGE_SIZE`) bounds response size.
--- stream table
--- destination_peer_id string
--- opts? table
--- true|nil ok
--- table|nil err_or_response
function M.connect(stream, destination_peer_id, opts)
  local options = opts or {}
  local wrote, write_err = M.write_hop_message(stream, {
    type = M.HOP_TYPE.CONNECT,
    peer = { peer_id = destination_peer_id },
  })
  if not wrote then
    return nil, write_err
  end
  local response, read_err = M.read_hop_message(stream, options)
  if not response then
    return nil, read_err
  end
  if response.type ~= M.HOP_TYPE.STATUS then
    return nil, error_mod.new("protocol", "unexpected relay connect response type", { got = response.type })
  end
  if response.status ~= M.STATUS.OK then
    return nil,
      error_mod.new("protocol", "relay connect failed", {
        status = response.status,
        status_name = M.status_name(response.status),
      })
  end
  return true, response
end

--- Accept and parse a STOP-side relay CONNECT request.
-- `opts.max_message_size` (`number`, default `MAX_MESSAGE_SIZE`) bounds request size.
--- stream table
--- opts? table
--- table|nil info
--- table|nil err
function M.accept_stop(stream, opts)
  local options = opts or {}
  local request, read_err = M.read_stop_message(stream, options)
  if not request then
    return nil, read_err
  end
  if request.type ~= M.STOP_TYPE.CONNECT then
    M.write_stop_message(stream, {
      type = M.STOP_TYPE.STATUS,
      status = M.STATUS.UNEXPECTED_MESSAGE,
    })
    return nil, error_mod.new("protocol", "unexpected relay stop message type", { got = request.type })
  end
  if not request.peer or type(request.peer.id) ~= "string" then
    M.write_stop_message(stream, {
      type = M.STOP_TYPE.STATUS,
      status = M.STATUS.MALFORMED_MESSAGE,
    })
    return nil, error_mod.new("protocol", "relay stop connect missing initiator peer")
  end
  local wrote, write_err = M.write_stop_message(stream, {
    type = M.STOP_TYPE.STATUS,
    status = M.STATUS.OK,
  })
  if not wrote then
    return nil, write_err
  end
  return {
    initiator_peer_id_bytes = request.peer.id,
    initiator_addrs = request.peer.addrs or {},
    limit = request.limit,
    limit_kind = M.classify_limit(request.limit),
    request = request,
  }
end

return M
