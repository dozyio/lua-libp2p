local error_mod = require("lua_libp2p.error")
local varint = require("lua_libp2p.multiformats.varint")
local key_pb = require("lua_libp2p.crypto.key_pb")
local peerid = require("lua_libp2p.peerid")

local M = {}

M.PROTOCOL_ID = "/plaintext/2.0.0"

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

  local value, next_or_err = varint.decode_u64(table.concat(bytes), 1)
  if not value then
    return nil, next_or_err
  end
  return value
end

local function encode_bytes_field(field_no, value)
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

local function normalize_expected_peer_id(expected)
  if expected == nil then
    return nil
  end
  if type(expected) ~= "string" then
    return nil, error_mod.new("input", "expected peer id must be bytes or string")
  end

  local parsed = peerid.parse(expected)
  if parsed then
    return parsed.bytes
  end
  return expected
end

function M.make_exchange_from_ed25519_public_key(raw_public_key)
  local pubkey_proto, pubkey_err = key_pb.encode_public_key(key_pb.KEY_TYPE.Ed25519, raw_public_key)
  if not pubkey_proto then
    return nil, pubkey_err
  end

  local pid, pid_err = peerid.from_public_key_proto(pubkey_proto, "ed25519")
  if not pid then
    return nil, pid_err
  end

  return {
    id = pid.bytes,
    pubkey = pubkey_proto,
  }
end

function M.encode_exchange(exchange)
  if type(exchange) ~= "table" then
    return nil, error_mod.new("input", "exchange must be a table")
  end
  if type(exchange.id) ~= "string" or exchange.id == "" then
    return nil, error_mod.new("input", "exchange.id must be non-empty bytes")
  end
  if type(exchange.pubkey) ~= "string" or exchange.pubkey == "" then
    return nil, error_mod.new("input", "exchange.pubkey must be non-empty bytes")
  end

  local id_field, id_err = encode_bytes_field(1, exchange.id)
  if not id_field then
    return nil, id_err
  end
  local pub_field, pub_err = encode_bytes_field(2, exchange.pubkey)
  if not pub_field then
    return nil, pub_err
  end

  return id_field .. pub_field
end

function M.decode_exchange(payload)
  if type(payload) ~= "string" or payload == "" then
    return nil, error_mod.new("input", "exchange payload must be non-empty bytes")
  end

  local i = 1
  local out = {}
  while i <= #payload do
    local tag, next_i_or_err = varint.decode_u64(payload, i)
    if not tag then
      return nil, next_i_or_err
    end
    local field_no = math.floor(tag / 8)
    local wire = tag % 8
    i = next_i_or_err

    if wire ~= 2 then
      return nil, error_mod.new("decode", "exchange fields must be bytes")
    end

    local length, after_len_or_err = varint.decode_u64(payload, i)
    if not length then
      return nil, after_len_or_err
    end
    local finish = after_len_or_err + length - 1
    if finish > #payload then
      return nil, error_mod.new("decode", "truncated exchange field")
    end

    local value = payload:sub(after_len_or_err, finish)
    if field_no == 1 then
      out.id = value
    elseif field_no == 2 then
      out.pubkey = value
    else
      return nil, error_mod.new("decode", "unknown exchange field", { field = field_no })
    end

    i = finish + 1
  end

  if type(out.id) ~= "string" or type(out.pubkey) ~= "string" then
    return nil, error_mod.new("decode", "exchange missing required fields")
  end

  return out
end

function M.write_exchange(conn, exchange)
  local payload, payload_err = M.encode_exchange(exchange)
  if not payload then
    return nil, payload_err
  end
  local len, len_err = varint.encode_u64(#payload)
  if not len then
    return nil, len_err
  end
  local ok, err = conn:write(len .. payload)
  if not ok then
    return nil, err
  end
  return true
end

function M.read_exchange(conn)
  local length, len_err = read_varint(conn)
  if not length then
    return nil, len_err
  end

  local payload, payload_err = read_exact(conn, length)
  if not payload then
    return nil, payload_err
  end

  return M.decode_exchange(payload)
end

function M.verify_exchange(exchange, expected_remote_peer_id)
  local decoded_key, decode_err = key_pb.decode_public_key(exchange.pubkey)
  if not decoded_key then
    return nil, decode_err
  end

  local derived, derive_err = peerid.from_public_key_proto(exchange.pubkey, decoded_key.type_name)
  if not derived then
    return nil, derive_err
  end
  if derived.bytes ~= exchange.id then
    return nil, error_mod.new("verify", "peer id does not match public key")
  end

  local expected_bytes, expected_err = normalize_expected_peer_id(expected_remote_peer_id)
  if expected_err then
    return nil, expected_err
  end
  if expected_bytes and expected_bytes ~= exchange.id then
    return nil, error_mod.new("verify", "remote peer id did not match expected", {
      expected = peerid.to_base58(expected_bytes),
      received = derived.id,
    })
  end

  return {
    peer_id = derived,
    key_type = decoded_key.type_name,
    public_key_data = decoded_key.data,
    exchange = exchange,
  }
end

function M.handshake(conn, local_exchange, expected_remote_peer_id)
  local ok, write_err = M.write_exchange(conn, local_exchange)
  if not ok then
    return nil, write_err
  end

  local remote_exchange, read_err = M.read_exchange(conn)
  if not remote_exchange then
    return nil, read_err
  end

  return M.verify_exchange(remote_exchange, expected_remote_peer_id)
end

return M
