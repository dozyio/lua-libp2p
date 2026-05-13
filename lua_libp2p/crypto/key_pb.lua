--- Libp2p key protobuf codec.
local error_mod = require("lua_libp2p.error")
local varint = require("lua_libp2p.multiformats.varint")

local M = {}

M.KEY_TYPE = {
  RSA = 0,
  Ed25519 = 1,
  Secp256k1 = 2,
  ECDSA = 3,
}

local TYPE_NAME = {
  [M.KEY_TYPE.RSA] = "rsa",
  [M.KEY_TYPE.Ed25519] = "ed25519",
  [M.KEY_TYPE.Secp256k1] = "secp256k1",
  [M.KEY_TYPE.ECDSA] = "ecdsa",
}

local NAME_TO_TYPE = {
  rsa = M.KEY_TYPE.RSA,
  ed25519 = M.KEY_TYPE.Ed25519,
  secp256k1 = M.KEY_TYPE.Secp256k1,
  ecdsa = M.KEY_TYPE.ECDSA,
}

local function encode_field_tag(field_number, wire_type)
  return varint.encode_u64(field_number * 8 + wire_type)
end

local function normalize_key_type(key_type)
  if type(key_type) == "number" then
    return key_type
  end
  if type(key_type) == "string" then
    return NAME_TO_TYPE[key_type]
  end
  return nil
end

local function encode_key_message(key_type, data)
  local normalized = normalize_key_type(key_type)
  if normalized == nil then
    return nil, error_mod.new("input", "invalid key type")
  end
  if type(data) ~= "string" then
    return nil, error_mod.new("input", "key data must be bytes")
  end

  local type_tag, type_tag_err = encode_field_tag(1, 0)
  if not type_tag then
    return nil, type_tag_err
  end
  local type_val, type_val_err = varint.encode_u64(normalized)
  if not type_val then
    return nil, type_val_err
  end

  local data_tag, data_tag_err = encode_field_tag(2, 2)
  if not data_tag then
    return nil, data_tag_err
  end
  local data_len, data_len_err = varint.encode_u64(#data)
  if not data_len then
    return nil, data_len_err
  end

  return type_tag .. type_val .. data_tag .. data_len .. data
end

local function decode_key_message(bytes)
  if type(bytes) ~= "string" or bytes == "" then
    return nil, error_mod.new("input", "key message bytes must be non-empty")
  end

  local i = 1
  local key_type
  local data

  while i <= #bytes do
    local tag, after_tag_or_err = varint.decode_u64(bytes, i)
    if not tag then
      return nil, after_tag_or_err
    end

    local field_number = math.floor(tag / 8)
    local wire_type = tag % 8
    i = after_tag_or_err

    if field_number == 1 then
      if wire_type ~= 0 then
        return nil, error_mod.new("decode", "key type field must be varint")
      end
      if key_type ~= nil then
        return nil, error_mod.new("decode", "duplicate key type field")
      end
      local parsed_type, after_type_or_err = varint.decode_u64(bytes, i)
      if not parsed_type then
        return nil, after_type_or_err
      end
      key_type = parsed_type
      i = after_type_or_err
    elseif field_number == 2 then
      if wire_type ~= 2 then
        return nil, error_mod.new("decode", "key data field must be bytes")
      end
      if data ~= nil then
        return nil, error_mod.new("decode", "duplicate key data field")
      end
      local length, after_len_or_err = varint.decode_u64(bytes, i)
      if not length then
        return nil, after_len_or_err
      end
      local finish = after_len_or_err + length - 1
      if finish > #bytes then
        return nil, error_mod.new("decode", "truncated key data field")
      end
      data = bytes:sub(after_len_or_err, finish)
      i = finish + 1
    else
      return nil, error_mod.new("decode", "unknown protobuf field in key message", { field = field_number })
    end
  end

  if key_type == nil or data == nil then
    return nil, error_mod.new("decode", "missing required key fields")
  end

  return {
    type = key_type,
    type_name = TYPE_NAME[key_type],
    data = data,
  }
end

function M.type_name(key_type)
  return TYPE_NAME[key_type]
end

function M.type_code(name)
  return NAME_TO_TYPE[name]
end

function M.encode_public_key(key_type, data)
  return encode_key_message(key_type, data)
end

function M.decode_public_key(bytes)
  return decode_key_message(bytes)
end

function M.encode_private_key(key_type, data)
  return encode_key_message(key_type, data)
end

function M.decode_private_key(bytes)
  return decode_key_message(bytes)
end

return M
