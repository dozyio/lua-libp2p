local error_mod = require("lua_libp2p.error")
local multiaddr = require("lua_libp2p.multiaddr")
local peer_record = require("lua_libp2p.record.peer_record")
local varint = require("lua_libp2p.multiformats.varint")

local M = {}

M.ID = "/ipfs/id/1.0.0"
M.PUSH_ID = "/ipfs/id/push/1.0.0"
M.MAX_MESSAGE_SIZE = 8 * 1024

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
    return nil, error_mod.new("input", "identify field must be a string", { field = field_no })
  end
  local encoded, err = encode_len_field(field_no, value)
  if not encoded then
    return nil, err
  end
  parts[#parts + 1] = encoded
  return true
end

local function normalize_multiaddr_bytes(value)
  if type(value) ~= "string" then
    return nil, error_mod.new("input", "identify multiaddr field must be bytes/string")
  end
  if value:sub(1, 1) == "/" then
    return multiaddr.to_bytes(value)
  end
  return value
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

function M.encode(message)
  if type(message) ~= "table" then
    return nil, error_mod.new("input", "identify message must be a table")
  end

  local parts = {}

  local ok, err = append_field(parts, 1, message.publicKey)
  if not ok then
    return nil, err
  end

  if message.listenAddrs ~= nil then
    if type(message.listenAddrs) ~= "table" then
      return nil, error_mod.new("input", "listenAddrs must be a table")
    end
    for _, addr in ipairs(message.listenAddrs) do
      local addr_bytes, addr_err = normalize_multiaddr_bytes(addr)
      if not addr_bytes then
        return nil, addr_err
      end
      ok, err = append_field(parts, 2, addr_bytes)
      if not ok then
        return nil, err
      end
    end
  end

  if message.protocols ~= nil then
    if type(message.protocols) ~= "table" then
      return nil, error_mod.new("input", "protocols must be a table")
    end
    for _, protocol in ipairs(message.protocols) do
      ok, err = append_field(parts, 3, protocol)
      if not ok then
        return nil, err
      end
    end
  end

  local observed_addr = message.observedAddr
  if observed_addr ~= nil then
    observed_addr, err = normalize_multiaddr_bytes(observed_addr)
    if not observed_addr then
      return nil, err
    end
  end
  ok, err = append_field(parts, 4, observed_addr)
  if not ok then
    return nil, err
  end

  ok, err = append_field(parts, 5, message.protocolVersion)
  if not ok then
    return nil, err
  end

  ok, err = append_field(parts, 6, message.agentVersion)
  if not ok then
    return nil, err
  end

  ok, err = append_field(parts, 8, message.signedPeerRecord)
  if not ok then
    return nil, err
  end

  return table.concat(parts)
end

function M.decode(payload)
  if type(payload) ~= "string" then
    return nil, error_mod.new("input", "identify payload must be bytes")
  end

  local out = {
    listenAddrs = {},
    protocols = {},
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
      return nil, error_mod.new("decode", "truncated identify field")
    end
    local value = payload:sub(after_len_or_err, finish)
    i = finish + 1

    if field_no == 1 then
      out.publicKey = value
    elseif field_no == 2 then
      out.listenAddrs[#out.listenAddrs + 1] = value
    elseif field_no == 3 then
      out.protocols[#out.protocols + 1] = value
    elseif field_no == 4 then
      out.observedAddr = value
    elseif field_no == 5 then
      out.protocolVersion = value
    elseif field_no == 6 then
      out.agentVersion = value
    elseif field_no == 8 then
      out.signedPeerRecord = value
    end

    ::continue::
  end

  return out
end

function M.merge(messages)
  if type(messages) ~= "table" then
    return nil, error_mod.new("input", "messages must be a table")
  end

  local out = {
    listenAddrs = {},
    protocols = {},
  }
  local seen_addrs = {}
  local seen_protocols = {}

  for _, msg in ipairs(messages) do
    if type(msg) ~= "table" then
      return nil, error_mod.new("input", "identify message must be a table")
    end

    if msg.protocolVersion ~= nil then
      out.protocolVersion = msg.protocolVersion
    end
    if msg.agentVersion ~= nil then
      out.agentVersion = msg.agentVersion
    end
    if msg.publicKey ~= nil then
      out.publicKey = msg.publicKey
    end
    if msg.observedAddr ~= nil then
      out.observedAddr = msg.observedAddr
    end
    if msg.signedPeerRecord ~= nil then
      out.signedPeerRecord = msg.signedPeerRecord
    end

    if type(msg.listenAddrs) == "table" then
      for _, addr in ipairs(msg.listenAddrs) do
        if not seen_addrs[addr] then
          seen_addrs[addr] = true
          out.listenAddrs[#out.listenAddrs + 1] = addr
        end
      end
    end

    if type(msg.protocols) == "table" then
      for _, protocol in ipairs(msg.protocols) do
        if not seen_protocols[protocol] then
          seen_protocols[protocol] = true
          out.protocols[#out.protocols + 1] = protocol
        end
      end
    end
  end

  return out
end

function M.write(conn, message)
  local payload, payload_err = M.encode(message)
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

function M.read(conn)
  local length, len_err = read_varint(conn)
  if not length then
    return nil, len_err
  end
  if length > M.MAX_MESSAGE_SIZE then
    return nil, error_mod.new("decode", "identify message too large", {
      max = M.MAX_MESSAGE_SIZE,
      got = length,
    })
  end

  local payload, payload_err = read_exact(conn, length)
  if not payload then
    return nil, payload_err
  end

  return M.decode(payload)
end

function M.send_push(conn, message)
  return M.write(conn, message)
end

function M.read_push(conn)
  return M.read(conn)
end

function M.handle_push(conn, on_message)
  local msg, err = M.read_push(conn)
  if not msg then
    return nil, err
  end
  if type(on_message) == "function" then
    return on_message(msg)
  end
  return msg
end

function M.verify_signed_peer_record(message, opts)
  if type(message) ~= "table" then
    return nil, error_mod.new("input", "identify message must be a table")
  end
  if type(message.signedPeerRecord) ~= "string" or message.signedPeerRecord == "" then
    return nil, error_mod.new("input", "identify message does not include signedPeerRecord")
  end
  return peer_record.verify_signed_envelope(message.signedPeerRecord, opts)
end

function M.enable_run_on_connection_open(host, opts)
  local options = opts or {}
  if options.run_on_connection_open == false then
    return true
  end
  if type(host) ~= "table" then
    return nil, error_mod.new("input", "identify run-on-connect requires host table")
  end
  if host._identify_on_connect_handler ~= nil then
    return true
  end
  if type(host.on) ~= "function" then
    return nil, error_mod.new("input", "identify run-on-connect requires host:on")
  end
  if type(host._schedule_identify_for_peer) ~= "function" then
    return nil, error_mod.new("input", "identify run-on-connect requires host scheduler")
  end

  host._identify_on_connect_handler = function(payload)
    local peer_id = payload and payload.peer_id
    return host:_schedule_identify_for_peer(peer_id)
  end

  return host:on("peer_connected", host._identify_on_connect_handler)
end

return M
