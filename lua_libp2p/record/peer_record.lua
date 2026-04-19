local error_mod = require("lua_libp2p.error")
local multiaddr = require("lua_libp2p.multiaddr")
local peerid = require("lua_libp2p.peerid")
local signed_envelope = require("lua_libp2p.record.signed_envelope")
local varint = require("lua_libp2p.multiformats.varint")

local M = {}

M.DOMAIN = "libp2p-peer-record"
M.LEGACY_DOMAIN = "libp2p-routing-state"
M.PAYLOAD_TYPE = "\x81\x06" -- multicodec: libp2p-peer-record (0x0301)
M.LEGACY_PAYLOAD_TYPE = "/libp2p/routing-state-record"

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

local function encode_u64_field(field_no, value)
  local tag, tag_err = varint.encode_u64(field_no * 8)
  if not tag then
    return nil, tag_err
  end
  local num, num_err = varint.encode_u64(value)
  if not num then
    return nil, num_err
  end
  return tag .. num
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

local function normalize_addr_bytes(addr)
  if type(addr) ~= "string" then
    return nil, error_mod.new("input", "address must be bytes or multiaddr text")
  end
  if addr:sub(1, 1) == "/" then
    return multiaddr.to_bytes(addr)
  end
  return addr
end

local function decode_address_info(payload)
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
      return nil, error_mod.new("decode", "truncated address info field")
    end
    local value = payload:sub(after_len_or_err, finish)
    i = finish + 1

    if field_no == 1 then
      out.multiaddr = value
    end

    ::continue::
  end

  if type(out.multiaddr) ~= "string" then
    return nil, error_mod.new("decode", "address info missing multiaddr")
  end

  return out
end

function M.encode(record)
  if type(record) ~= "table" then
    return nil, error_mod.new("input", "peer record must be a table")
  end
  if type(record.peer_id) ~= "string" or record.peer_id == "" then
    return nil, error_mod.new("input", "peer_id must be non-empty bytes")
  end
  if type(record.seq) ~= "number" or record.seq < 0 then
    return nil, error_mod.new("input", "seq must be a non-negative number")
  end

  local parts = {}
  local peer_field, peer_err = encode_len_field(1, record.peer_id)
  if not peer_field then
    return nil, peer_err
  end
  parts[#parts + 1] = peer_field

  local seq_field, seq_err = encode_u64_field(2, record.seq)
  if not seq_field then
    return nil, seq_err
  end
  parts[#parts + 1] = seq_field

  if record.addresses ~= nil then
    if type(record.addresses) ~= "table" then
      return nil, error_mod.new("input", "addresses must be a table")
    end
    for _, addr in ipairs(record.addresses) do
      local addr_bytes, addr_err = normalize_addr_bytes(addr)
      if not addr_bytes then
        return nil, addr_err
      end
      local ai, ai_err = encode_len_field(1, addr_bytes)
      if not ai then
        return nil, ai_err
      end
      local wrapped, wrapped_err = encode_len_field(3, ai)
      if not wrapped then
        return nil, wrapped_err
      end
      parts[#parts + 1] = wrapped
    end
  end

  return table.concat(parts)
end

function M.decode(payload)
  if type(payload) ~= "string" then
    return nil, error_mod.new("input", "peer record payload must be bytes")
  end

  local out = {
    addresses = {},
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

    if field_no == 2 and wire == 0 then
      local seq, after_seq_or_err = varint.decode_u64(payload, i)
      if not seq then
        return nil, after_seq_or_err
      end
      out.seq = seq
      i = after_seq_or_err
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
      return nil, error_mod.new("decode", "truncated peer record field")
    end
    local value = payload:sub(after_len_or_err, finish)
    i = finish + 1

    if field_no == 1 then
      out.peer_id = value
    elseif field_no == 3 then
      local info, info_err = decode_address_info(value)
      if not info then
        return nil, info_err
      end
      out.addresses[#out.addresses + 1] = info.multiaddr
    end

    ::continue::
  end

  if type(out.peer_id) ~= "string" then
    return nil, error_mod.new("decode", "peer record missing peer_id")
  end
  if type(out.seq) ~= "number" then
    return nil, error_mod.new("decode", "peer record missing seq")
  end

  return out
end

function M.sign_ed25519(keypair, record, opts)
  local options = opts or {}
  local payload, payload_err = M.encode(record)
  if not payload then
    return nil, payload_err
  end

  local domain = options.domain or M.DOMAIN
  local payload_type = options.payload_type or M.PAYLOAD_TYPE
  return signed_envelope.sign_ed25519(keypair, domain, payload_type, payload)
end

function M.verify_signed_envelope(envelope_or_bytes, opts)
  local options = opts or {}
  local envelope = envelope_or_bytes
  if type(envelope_or_bytes) == "string" then
    local decoded, decode_err = signed_envelope.decode(envelope_or_bytes)
    if not decoded then
      return nil, decode_err
    end
    envelope = decoded
  end

  local domains = options.domains or { M.DOMAIN, M.LEGACY_DOMAIN }
  local payload_types = options.payload_types or { M.PAYLOAD_TYPE, M.LEGACY_PAYLOAD_TYPE }

  local verify_result
  local verify_error
  for _, domain in ipairs(domains) do
    verify_result, verify_error = signed_envelope.verify(envelope, domain, options.expected_peer_id)
    if verify_result then
      local payload_ok = false
      for _, pt in ipairs(payload_types) do
        if verify_result.payload_type == pt then
          payload_ok = true
          break
        end
      end
      if payload_ok then
        break
      end
      verify_result = nil
      verify_error = error_mod.new("verify", "unsupported peer record payload_type")
    end
  end
  if not verify_result then
    return nil, verify_error
  end

  local record, record_err = M.decode(verify_result.payload)
  if not record then
    return nil, record_err
  end
  if record.peer_id ~= verify_result.peer_id.bytes then
    return nil, error_mod.new("verify", "peer record peer_id mismatch with signer")
  end

  return {
    peer_id = verify_result.peer_id,
    record = record,
    envelope = envelope,
  }
end

function M.addresses_as_text(record)
  if type(record) ~= "table" or type(record.addresses) ~= "table" then
    return nil, error_mod.new("input", "record must contain addresses")
  end

  local out = {}
  for _, addr_bytes in ipairs(record.addresses) do
    local addr, err = multiaddr.from_bytes(addr_bytes)
    if not addr then
      return nil, err
    end
    out[#out + 1] = addr.text
  end
  return out
end

function M.make_record(peer_id_value, seq, addrs)
  if type(peer_id_value) ~= "string" then
    return nil, error_mod.new("input", "peer_id must be bytes or string")
  end
  local pid = peerid.parse(peer_id_value)
  local peer_bytes = pid and pid.bytes or peer_id_value

  return {
    peer_id = peer_bytes,
    seq = seq,
    addresses = addrs or {},
  }
end

return M
