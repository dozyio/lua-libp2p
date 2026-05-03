--- Signed envelope primitives.
-- @module lua_libp2p.record.signed_envelope
local error_mod = require("lua_libp2p.error")
local keys = require("lua_libp2p.crypto.keys")
local key_pb = require("lua_libp2p.crypto.key_pb")
local peerid = require("lua_libp2p.peerid")
local varint = require("lua_libp2p.multiformats.varint")

local M = {}

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

local function append_field(parts, field_no, value)
  if value == nil then
    return true
  end
  if type(value) ~= "string" then
    return nil, error_mod.new("input", "signed envelope field must be bytes", { field = field_no })
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

--- Build canonical bytes for envelope signatures.
function M.signature_input(domain, payload_type, payload)
  if type(domain) ~= "string" then
    return nil, error_mod.new("input", "domain must be a string")
  end
  if type(payload_type) ~= "string" then
    return nil, error_mod.new("input", "payload_type must be bytes")
  end
  if type(payload) ~= "string" then
    return nil, error_mod.new("input", "payload must be bytes")
  end

  local dlen, dlen_err = varint.encode_u64(#domain)
  if not dlen then
    return nil, dlen_err
  end
  local tlen, tlen_err = varint.encode_u64(#payload_type)
  if not tlen then
    return nil, tlen_err
  end
  local plen, plen_err = varint.encode_u64(#payload)
  if not plen then
    return nil, plen_err
  end

  return dlen .. domain .. tlen .. payload_type .. plen .. payload
end

--- Encode a signed envelope protobuf payload.
function M.encode(envelope)
  if type(envelope) ~= "table" then
    return nil, error_mod.new("input", "envelope must be a table")
  end

  local parts = {}
  local ok, err = append_field(parts, 1, envelope.public_key)
  if not ok then
    return nil, err
  end
  ok, err = append_field(parts, 2, envelope.payload_type)
  if not ok then
    return nil, err
  end
  ok, err = append_field(parts, 3, envelope.payload)
  if not ok then
    return nil, err
  end
  ok, err = append_field(parts, 5, envelope.signature)
  if not ok then
    return nil, err
  end

  return table.concat(parts)
end

--- Decode a signed envelope protobuf payload.
function M.decode(payload)
  if type(payload) ~= "string" then
    return nil, error_mod.new("input", "envelope payload must be bytes")
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
      return nil, error_mod.new("decode", "truncated signed envelope field")
    end
    local value = payload:sub(after_len_or_err, finish)
    i = finish + 1

    if field_no == 1 then
      out.public_key = value
    elseif field_no == 2 then
      out.payload_type = value
    elseif field_no == 3 then
      out.payload = value
    elseif field_no == 5 then
      out.signature = value
    end

    ::continue::
  end

  if type(out.public_key) ~= "string" then
    return nil, error_mod.new("decode", "missing envelope public_key")
  end
  if type(out.payload) ~= "string" then
    return nil, error_mod.new("decode", "missing envelope payload")
  end
  if type(out.signature) ~= "string" then
    return nil, error_mod.new("decode", "missing envelope signature")
  end

  if type(out.payload_type) ~= "string" then
    out.payload_type = ""
  end

  return out
end

function M.sign_ed25519(keypair, domain, payload_type, payload)
  return M.sign(keypair, domain, payload_type, payload)
end

--- Sign payload with keypair under a domain.
function M.sign(keypair, domain, payload_type, payload)
  if not keypair or type(keypair.public_key) ~= "string" or type(keypair.private_key) ~= "string" then
    return nil, error_mod.new("input", "keypair must contain public/private key bytes")
  end

  local signing_input, input_err = M.signature_input(domain, payload_type or "", payload)
  if not signing_input then
    return nil, input_err
  end

  local public_key_pb, pub_err = keys.public_key_proto(keypair)
  if not public_key_pb then
    return nil, pub_err
  end

  local signature, sig_err = keys.sign(keypair, signing_input)
  if not signature then
    return nil, sig_err
  end

  return {
    public_key = public_key_pb,
    payload_type = payload_type or "",
    payload = payload,
    signature = signature,
  }
end

--- Verify envelope signature and optional expected peer.
function M.verify(envelope, domain, expected_peer_id)
  if type(envelope) ~= "table" then
    return nil, error_mod.new("input", "envelope must be a table")
  end

  local decoded_key, decode_err = key_pb.decode_public_key(envelope.public_key)
  if not decoded_key then
    return nil, decode_err
  end
  local pid, pid_err = peerid.from_public_key_proto(envelope.public_key, decoded_key.type_name)
  if not pid then
    return nil, pid_err
  end

  local expected_bytes, expected_err = normalize_expected_peer_id(expected_peer_id)
  if expected_err then
    return nil, expected_err
  end
  if expected_bytes and expected_bytes ~= pid.bytes then
    return nil, error_mod.new("verify", "envelope signer did not match expected peer", {
      expected = peerid.to_base58(expected_bytes),
      received = pid.id,
    })
  end

  local signing_input, input_err = M.signature_input(domain, envelope.payload_type or "", envelope.payload)
  if not signing_input then
    return nil, input_err
  end

  local ok, verify_err = keys.verify_signature({ data = decoded_key.data, type = decoded_key.type }, signing_input, envelope.signature, decoded_key.type)
  if verify_err then
    return nil, verify_err
  end
  if not ok then
    return nil, error_mod.new("verify", "signed envelope signature verification failed")
  end

  return {
    peer_id = pid,
    public_key = decoded_key,
    payload_type = envelope.payload_type or "",
    payload = envelope.payload,
  }
end

return M
