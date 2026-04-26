local error_mod = require("lua_libp2p.error")
local network = require("lua_libp2p.network")
local peerid = require("lua_libp2p.peerid")
local varint = require("lua_libp2p.multiformats.varint")

local M = {}

M.MAX_MESSAGE_SIZE = network.MESSAGE_SIZE_MAX

M.MESSAGE_TYPE = {
  PUT_VALUE = 0,
  GET_VALUE = 1,
  ADD_PROVIDER = 2,
  GET_PROVIDERS = 3,
  FIND_NODE = 4,
  PING = 5,
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

local function parse_peer_id_bytes(value)
  if type(value) ~= "string" then
    return nil, error_mod.new("input", "peer id must be bytes or string")
  end
  local parsed = peerid.parse(value)
  if parsed then
    return parsed.bytes
  end
  return value
end

local function normalize_peer(peer)
  if type(peer) ~= "table" then
    return nil, error_mod.new("input", "peer must be a table")
  end

  local peer_id = peer.id or peer.peer_id
  local id_bytes, id_err = parse_peer_id_bytes(peer_id)
  if not id_bytes then
    return nil, id_err
  end

  local addrs = {}
  if peer.addrs ~= nil then
    if type(peer.addrs) ~= "table" then
      return nil, error_mod.new("input", "peer addrs must be a table")
    end
    for _, addr in ipairs(peer.addrs) do
      if type(addr) ~= "string" then
        return nil, error_mod.new("input", "peer addr must be bytes/string")
      end
      addrs[#addrs + 1] = addr
    end
  end

  return {
    id = id_bytes,
    addrs = addrs,
    connection = peer.connection,
  }
end

function M.encode_peer(peer)
  local normalized, norm_err = normalize_peer(peer)
  if not normalized then
    return nil, norm_err
  end

  local parts = {}
  local ok, err = append_len_field(parts, 1, normalized.id)
  if not ok then
    return nil, err
  end

  for _, addr in ipairs(normalized.addrs) do
    ok, err = append_len_field(parts, 2, addr)
    if not ok then
      return nil, err
    end
  end

  ok, err = append_varint_field(parts, 3, normalized.connection)
  if not ok then
    return nil, err
  end

  return table.concat(parts)
end

function M.decode_peer(payload)
  if type(payload) ~= "string" then
    return nil, error_mod.new("input", "peer payload must be bytes")
  end

  local out = {
    addrs = {},
  }

  local i = 1
  while i <= #payload do
    local tag, next_i_or_err = varint.decode_u64(payload, i)
    if not tag then
      return nil, next_i_or_err
    end
    local field_no = math.floor(tag / 8)
    local wire = tag % 8
    i = next_i_or_err

    if field_no == 3 and wire == 0 then
      local value, after_or_err = varint.decode_u64(payload, i)
      if not value then
        return nil, after_or_err
      end
      out.connection = value
      i = after_or_err
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
      return nil, error_mod.new("decode", "truncated kad peer field")
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

  return out
end

function M.encode_record(record)
  if type(record) ~= "table" then
    return nil, error_mod.new("input", "record must be a table")
  end
  local parts = {}
  local ok, err = append_len_field(parts, 1, record.key)
  if not ok then
    return nil, err
  end
  ok, err = append_len_field(parts, 2, record.value)
  if not ok then
    return nil, err
  end
  ok, err = append_len_field(parts, 5, record.time_received or record.timeReceived)
  if not ok then
    return nil, err
  end
  return table.concat(parts)
end

function M.decode_record(payload)
  if type(payload) ~= "string" then
    return nil, error_mod.new("input", "record payload must be bytes")
  end
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
      return nil, error_mod.new("decode", "truncated kad record field")
    end
    local value = payload:sub(after_len_or_err, finish)
    i = finish + 1
    if field_no == 1 then
      out.key = value
    elseif field_no == 2 then
      out.value = value
    elseif field_no == 5 then
      out.time_received = value
      out.timeReceived = value
    end
    ::continue::
  end
  return out
end

function M.encode_message(message)
  if type(message) ~= "table" then
    return nil, error_mod.new("input", "kad message must be a table")
  end

  local parts = {}
  local ok, err = append_varint_field(parts, 1, message.type)
  if not ok then
    return nil, err
  end

  ok, err = append_len_field(parts, 2, message.key)
  if not ok then
    return nil, err
  end

  if message.record ~= nil then
    local encoded_record, rec_err = M.encode_record(message.record)
    if not encoded_record then
      return nil, rec_err
    end
    ok, err = append_len_field(parts, 3, encoded_record)
    if not ok then
      return nil, err
    end
  end

  local closer = message.closer_peers or message.closerPeers
  if closer ~= nil then
    if type(closer) ~= "table" then
      return nil, error_mod.new("input", "closer peers must be a table")
    end
    for _, peer in ipairs(closer) do
      local encoded_peer, enc_err = M.encode_peer(peer)
      if not encoded_peer then
        return nil, enc_err
      end
      ok, err = append_len_field(parts, 8, encoded_peer)
      if not ok then
        return nil, err
      end
    end
  end

  local providers = message.provider_peers or message.providerPeers
  if providers ~= nil then
    if type(providers) ~= "table" then
      return nil, error_mod.new("input", "provider peers must be a table")
    end
    for _, peer in ipairs(providers) do
      local encoded_peer, enc_err = M.encode_peer(peer)
      if not encoded_peer then
        return nil, enc_err
      end
      ok, err = append_len_field(parts, 9, encoded_peer)
      if not ok then
        return nil, err
      end
    end
  end

  return table.concat(parts)
end

function M.decode_message(payload)
  if type(payload) ~= "string" then
    return nil, error_mod.new("input", "kad payload must be bytes")
  end

  local out = {
    closer_peers = {},
    provider_peers = {},
  }

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
      local value, after_or_err = varint.decode_u64(payload, i)
      if not value then
        return nil, after_or_err
      end
      out.type = value
      i = after_or_err
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
      return nil, error_mod.new("decode", "truncated kad message field")
    end
    local value = payload:sub(after_len_or_err, finish)
    i = finish + 1

    if field_no == 2 then
      out.key = value
    elseif field_no == 3 then
      local record, record_err = M.decode_record(value)
      if not record then
        return nil, record_err
      end
      out.record = record
    elseif field_no == 8 then
      local peer, peer_err = M.decode_peer(value)
      if not peer then
        return nil, peer_err
      end
      out.closer_peers[#out.closer_peers + 1] = peer
    elseif field_no == 9 then
      local peer, peer_err = M.decode_peer(value)
      if not peer then
        return nil, peer_err
      end
      out.provider_peers[#out.provider_peers + 1] = peer
    end

    ::continue::
  end

  return out
end

local function max_message_size(opts)
  local max = opts and opts.max_message_size or M.MAX_MESSAGE_SIZE
  if type(max) ~= "number" or max < 1 then
    return nil, error_mod.new("input", "max_message_size must be a positive number")
  end
  return max
end

function M.read(conn, opts)
  local max, max_err = max_message_size(opts)
  if not max then
    return nil, max_err
  end

  local length, len_err = read_varint(conn)
  if not length then
    return nil, len_err
  end
  if length > max then
    return nil, error_mod.new("decode", "kad-dht message too large", {
      max = max,
      got = length,
    })
  end

  local payload, payload_err = read_exact(conn, length)
  if not payload then
    return nil, payload_err
  end

  return M.decode_message(payload)
end

function M.write(conn, message, opts)
  local max, max_err = max_message_size(opts)
  if not max then
    return nil, max_err
  end

  local payload, payload_err = M.encode_message(message)
  if not payload then
    return nil, payload_err
  end
  if #payload > max then
    return nil, error_mod.new("input", "kad-dht payload too large", {
      max = max,
      got = #payload,
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

function M.peer_bytes(peer_text_or_bytes)
  return parse_peer_id_bytes(peer_text_or_bytes)
end

return M
