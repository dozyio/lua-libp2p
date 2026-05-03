--- Peerstore record codec.
-- Schema-specific protobuf-style encoding for datastore-backed peerstore records.
-- @module lua_libp2p.peerstore.codec
local error_mod = require("lua_libp2p.error")
local multiaddr = require("lua_libp2p.multiaddr")
local varint = require("lua_libp2p.multiformats.varint")

local M = {}

local function sorted_keys(tbl)
  local keys = {}
  for k in pairs(tbl or {}) do
    if type(k) == "string" then
      keys[#keys + 1] = k
    end
  end
  table.sort(keys)
  return keys
end

local function encode_tag(field_no, wire)
  return varint.encode_u64(field_no * 8 + wire)
end

local function encode_varint_field(field_no, value)
  if type(value) ~= "number" or value < 0 then
    return nil, error_mod.new("input", "peerstore varint field must be a non-negative number", { field = field_no })
  end
  local tag, tag_err = encode_tag(field_no, 0)
  if not tag then
    return nil, tag_err
  end
  local encoded, encoded_err = varint.encode_u64(value)
  if not encoded then
    return nil, encoded_err
  end
  return tag .. encoded
end

local function encode_bool_field(field_no, value)
  return encode_varint_field(field_no, value and 1 or 0)
end

local function encode_double_field(field_no, value)
  if type(value) ~= "number" then
    return nil, error_mod.new("input", "peerstore double field must be a number", { field = field_no })
  end
  local tag, tag_err = encode_tag(field_no, 1)
  if not tag then
    return nil, tag_err
  end
  return tag .. string.pack("<d", value)
end

local function encode_bytes_field(field_no, value)
  if type(value) ~= "string" then
    return nil, error_mod.new("input", "peerstore bytes field must be a string", { field = field_no })
  end
  local tag, tag_err = encode_tag(field_no, 2)
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
    if not next_i_or_err then
      return nil, next_i_or_err
    end
    return next_i_or_err
  end
  if wire == 1 then
    local finish = index + 7
    if finish > #payload then
      return nil, error_mod.new("decode", "truncated 64-bit peerstore field")
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
      return nil, error_mod.new("decode", "truncated peerstore bytes field")
    end
    return finish + 1
  end
  if wire == 5 then
    local finish = index + 3
    if finish > #payload then
      return nil, error_mod.new("decode", "truncated 32-bit peerstore field")
    end
    return finish + 1
  end
  return nil, error_mod.new("decode", "unsupported peerstore wire type", { wire = wire })
end

local function decode_fields(payload, handlers)
  local i = 1
  while i <= #payload do
    local tag, after_tag_or_err = varint.decode_u64(payload, i)
    if not tag then
      return nil, after_tag_or_err
    end
    local field_no = math.floor(tag / 8)
    local wire = tag % 8
    i = after_tag_or_err
    local handler = handlers[field_no]
    if handler then
      local next_i, err = handler(payload, i, wire)
      if not next_i then
        return nil, err
      end
      i = next_i
    else
      local next_i, err = skip_unknown(payload, i, wire)
      if not next_i then
        return nil, err
      end
      i = next_i
    end
  end
  return true
end

local function read_varint(payload, index, wire, name)
  if wire ~= 0 then
    return nil, nil, error_mod.new("decode", name .. " must be varint")
  end
  local value, next_i_or_err = varint.decode_u64(payload, index)
  if not value then
    return nil, nil, next_i_or_err
  end
  return value, next_i_or_err
end

local function read_bool(payload, index, wire, name)
  local value, next_i, err = read_varint(payload, index, wire, name)
  if not next_i then
    return nil, nil, err
  end
  return value ~= 0, next_i
end

local function read_double(payload, index, wire, name)
  if wire ~= 1 then
    return nil, nil, error_mod.new("decode", name .. " must be 64-bit")
  end
  if index + 7 > #payload then
    return nil, nil, error_mod.new("decode", "truncated " .. name)
  end
  return string.unpack("<d", payload, index)
end

local function read_bytes(payload, index, wire, name)
  if wire ~= 2 then
    return nil, nil, error_mod.new("decode", name .. " must be bytes")
  end
  local length, after_len_or_err = varint.decode_u64(payload, index)
  if not length then
    return nil, nil, after_len_or_err
  end
  local finish = after_len_or_err + length - 1
  if finish > #payload then
    return nil, nil, error_mod.new("decode", "truncated " .. name)
  end
  return payload:sub(after_len_or_err, finish), finish + 1
end

local function encode_typed_value(value)
  local value_type = type(value)
  local parts = {}
  local field, err
  if value == nil then
    field, err = encode_varint_field(1, 0)
  elseif value_type == "boolean" then
    field, err = encode_bool_field(2, value)
  elseif value_type == "number" then
    field, err = encode_double_field(3, value)
  elseif value_type == "string" then
    field, err = encode_bytes_field(4, value)
  else
    return nil, error_mod.new("input", "unsupported peerstore metadata value type", { value_type = value_type })
  end
  if not field then
    return nil, err
  end
  parts[#parts + 1] = field
  return table.concat(parts)
end

local function decode_typed_value(payload)
  local out = { set = false, value = nil }
  local ok, err = decode_fields(payload, {
    [1] = function(p, i, wire)
      local _, next_i, read_err = read_varint(p, i, wire, "nil metadata value")
      if not next_i then
        return nil, read_err
      end
      out.set = true
      out.value = nil
      return next_i
    end,
    [2] = function(p, i, wire)
      local value, next_i, read_err = read_bool(p, i, wire, "boolean metadata value")
      if not next_i then
        return nil, read_err
      end
      out.set = true
      out.value = value
      return next_i
    end,
    [3] = function(p, i, wire)
      local value, next_i, read_err = read_double(p, i, wire, "number metadata value")
      if not next_i then
        return nil, read_err
      end
      out.set = true
      out.value = value
      return next_i
    end,
    [4] = function(p, i, wire)
      local value, next_i, read_err = read_bytes(p, i, wire, "string metadata value")
      if not next_i then
        return nil, read_err
      end
      out.set = true
      out.value = value
      return next_i
    end,
  })
  if not ok then
    return nil, err
  end
  if not out.set then
    return nil, error_mod.new("decode", "typed metadata value missing value")
  end
  return out.value
end

local function encode_addr_entry(entry)
  local parts = {}
  local addr_bytes, addr_err = multiaddr.to_bytes(entry.addr)
  if not addr_bytes then
    return nil, addr_err
  end
  local field, err = encode_bytes_field(1, addr_bytes)
  if not field then
    return nil, err
  end
  parts[#parts + 1] = field
  if entry.expires_at ~= nil then
    field, err = encode_varint_field(2, entry.expires_at)
    if not field then
      return nil, err
    end
    parts[#parts + 1] = field
  end
  field, err = encode_bool_field(3, entry.certified == true)
  if not field then
    return nil, err
  end
  parts[#parts + 1] = field
  if entry.last_seen ~= nil then
    field, err = encode_varint_field(4, entry.last_seen)
    if not field then
      return nil, err
    end
    parts[#parts + 1] = field
  end
  return table.concat(parts)
end

local function decode_addr_entry(payload)
  local entry = {}
  local ok, err = decode_fields(payload, {
    [1] = function(p, i, wire)
      local value, next_i, read_err = read_bytes(p, i, wire, "addr")
      local parsed = multiaddr.from_bytes(value)
      if not parsed or type(parsed.text) ~= "string" then
        return nil, error_mod.new("decode", "invalid peerstore multiaddr bytes")
      end
      entry.addr = parsed.text
      return next_i, read_err
    end,
    [2] = function(p, i, wire)
      local value, next_i, read_err = read_varint(p, i, wire, "addr expires_at")
      entry.expires_at = value
      return next_i, read_err
    end,
    [3] = function(p, i, wire)
      local value, next_i, read_err = read_bool(p, i, wire, "addr certified")
      entry.certified = value
      return next_i, read_err
    end,
    [4] = function(p, i, wire)
      local value, next_i, read_err = read_varint(p, i, wire, "addr last_seen")
      entry.last_seen = value
      return next_i, read_err
    end,
  })
  if not ok then
    return nil, err
  end
  if type(entry.addr) ~= "string" or entry.addr == "" then
    return nil, error_mod.new("decode", "address entry missing addr")
  end
  entry.certified = entry.certified == true
  return entry
end

local function encode_tag_entry(tag)
  local parts = {}
  local field, err = encode_bytes_field(1, tag.name)
  if not field then
    return nil, err
  end
  parts[#parts + 1] = field
  field, err = encode_double_field(2, tonumber(tag.value) or 0)
  if not field then
    return nil, err
  end
  parts[#parts + 1] = field
  if tag.expires_at ~= nil then
    field, err = encode_varint_field(3, tag.expires_at)
    if not field then
      return nil, err
    end
    parts[#parts + 1] = field
  end
  return table.concat(parts)
end

local function decode_tag_entry(payload)
  local tag = {}
  local ok, err = decode_fields(payload, {
    [1] = function(p, i, wire)
      local value, next_i, read_err = read_bytes(p, i, wire, "tag name")
      tag.name = value
      return next_i, read_err
    end,
    [2] = function(p, i, wire)
      local value, next_i, read_err = read_double(p, i, wire, "tag value")
      tag.value = value
      return next_i, read_err
    end,
    [3] = function(p, i, wire)
      local value, next_i, read_err = read_varint(p, i, wire, "tag expires_at")
      tag.expires_at = value
      return next_i, read_err
    end,
  })
  if not ok then
    return nil, err
  end
  if type(tag.name) ~= "string" or tag.name == "" then
    return nil, error_mod.new("decode", "tag entry missing name")
  end
  tag.value = tag.value or 0
  return tag
end

local function encode_metadata_entry(key, value)
  local encoded_value, value_err = encode_typed_value(value)
  if not encoded_value then
    return nil, value_err
  end
  local key_field, key_err = encode_bytes_field(1, key)
  if not key_field then
    return nil, key_err
  end
  local value_field, field_err = encode_bytes_field(2, encoded_value)
  if not value_field then
    return nil, field_err
  end
  return key_field .. value_field
end

local function decode_metadata_entry(payload)
  local key, encoded_value
  local ok, err = decode_fields(payload, {
    [1] = function(p, i, wire)
      local value, next_i, read_err = read_bytes(p, i, wire, "metadata key")
      key = value
      return next_i, read_err
    end,
    [2] = function(p, i, wire)
      local value, next_i, read_err = read_bytes(p, i, wire, "metadata value")
      encoded_value = value
      return next_i, read_err
    end,
  })
  if not ok then
    return nil, nil, err
  end
  if type(key) ~= "string" or key == "" then
    return nil, nil, error_mod.new("decode", "metadata entry missing key")
  end
  local value, value_err = decode_typed_value(encoded_value or "")
  if value_err then
    return nil, nil, value_err
  end
  return key, value
end

function M.encode(record)
  if type(record) ~= "table" then
    return nil, error_mod.new("input", "peerstore record must be a table")
  end
  if type(record.peer_id) ~= "string" or record.peer_id == "" then
    return nil, error_mod.new("input", "peerstore record peer_id must be non-empty")
  end

  local parts = {}
  local field, err = encode_bytes_field(1, record.peer_id)
  if not field then
    return nil, err
  end
  parts[#parts + 1] = field

  for _, addr in ipairs(sorted_keys(record.addrs)) do
    local encoded_addr, addr_err = encode_addr_entry(record.addrs[addr])
    if not encoded_addr then
      return nil, addr_err
    end
    field, err = encode_bytes_field(2, encoded_addr)
    if not field then
      return nil, err
    end
    parts[#parts + 1] = field
  end

  for _, protocol in ipairs(sorted_keys(record.protocols)) do
    field, err = encode_bytes_field(3, protocol)
    if not field then
      return nil, err
    end
    parts[#parts + 1] = field
  end

  for _, name in ipairs(sorted_keys(record.tags)) do
    local encoded_tag, tag_err = encode_tag_entry(record.tags[name])
    if not encoded_tag then
      return nil, tag_err
    end
    field, err = encode_bytes_field(4, encoded_tag)
    if not field then
      return nil, err
    end
    parts[#parts + 1] = field
  end

  for _, key in ipairs(sorted_keys(record.metadata)) do
    local encoded_metadata, metadata_err = encode_metadata_entry(key, record.metadata[key])
    if not encoded_metadata then
      return nil, metadata_err
    end
    field, err = encode_bytes_field(5, encoded_metadata)
    if not field then
      return nil, err
    end
    parts[#parts + 1] = field
  end

  if record.public_key ~= nil then
    field, err = encode_bytes_field(6, record.public_key)
    if not field then
      return nil, err
    end
    parts[#parts + 1] = field
  end
  if record.peer_record_envelope ~= nil then
    field, err = encode_bytes_field(7, record.peer_record_envelope)
    if not field then
      return nil, err
    end
    parts[#parts + 1] = field
  end
  if record.updated_at ~= nil then
    field, err = encode_varint_field(8, record.updated_at)
    if not field then
      return nil, err
    end
    parts[#parts + 1] = field
  end

  return table.concat(parts)
end

function M.decode(bytes)
  if type(bytes) ~= "string" then
    return nil, error_mod.new("decode", "peerstore record bytes must be a string")
  end
  local record = {
    addrs = {},
    protocols = {},
    tags = {},
    metadata = {},
  }
  local ok, err = decode_fields(bytes, {
    [1] = function(p, i, wire)
      local value, next_i, read_err = read_bytes(p, i, wire, "peer id")
      record.peer_id = value
      return next_i, read_err
    end,
    [2] = function(p, i, wire)
      local value, next_i, read_err = read_bytes(p, i, wire, "address entry")
      if not next_i then
        return nil, read_err
      end
      local entry, entry_err = decode_addr_entry(value)
      if not entry then
        return nil, entry_err
      end
      record.addrs[entry.addr] = entry
      return next_i
    end,
    [3] = function(p, i, wire)
      local value, next_i, read_err = read_bytes(p, i, wire, "protocol")
      if value and value ~= "" then
        record.protocols[value] = true
      end
      return next_i, read_err
    end,
    [4] = function(p, i, wire)
      local value, next_i, read_err = read_bytes(p, i, wire, "tag entry")
      if not next_i then
        return nil, read_err
      end
      local tag, tag_err = decode_tag_entry(value)
      if not tag then
        return nil, tag_err
      end
      record.tags[tag.name] = tag
      return next_i
    end,
    [5] = function(p, i, wire)
      local value, next_i, read_err = read_bytes(p, i, wire, "metadata entry")
      if not next_i then
        return nil, read_err
      end
      local key, metadata_value, metadata_err = decode_metadata_entry(value)
      if not key then
        return nil, metadata_err
      end
      record.metadata[key] = metadata_value
      return next_i
    end,
    [6] = function(p, i, wire)
      local value, next_i, read_err = read_bytes(p, i, wire, "public key")
      record.public_key = value
      return next_i, read_err
    end,
    [7] = function(p, i, wire)
      local value, next_i, read_err = read_bytes(p, i, wire, "peer record envelope")
      record.peer_record_envelope = value
      return next_i, read_err
    end,
    [8] = function(p, i, wire)
      local value, next_i, read_err = read_varint(p, i, wire, "updated_at")
      record.updated_at = value
      return next_i, read_err
    end,
  })
  if not ok then
    return nil, err
  end
  if type(record.peer_id) ~= "string" or record.peer_id == "" then
    return nil, error_mod.new("decode", "peerstore record missing peer_id")
  end
  return record
end

return M
