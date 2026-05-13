--- Minimal DNS packet codec for libp2p mDNS discovery.
local error_mod = require("lua_libp2p.error")

local M = {}

M.TYPE_A = 1
M.TYPE_PTR = 12
M.TYPE_TXT = 16
M.TYPE_AAAA = 28
M.TYPE_SRV = 33
M.CLASS_IN = 1

local function u16be(n)
  return string.char((n >> 8) & 0xff, n & 0xff)
end

local function u32be(n)
  return string.char((n >> 24) & 0xff, (n >> 16) & 0xff, (n >> 8) & 0xff, n & 0xff)
end

local function read_u16(bytes, offset)
  local a, b = bytes:byte(offset, offset + 1)
  if not a or not b then
    return nil
  end
  return (a << 8) | b
end

local function read_u32(bytes, offset)
  local a, b, c, d = bytes:byte(offset, offset + 3)
  if not a or not b or not c or not d then
    return nil
  end
  return (a << 24) | (b << 16) | (c << 8) | d
end

local function encode_name(name)
  if type(name) ~= "string" or name == "" then
    return nil, error_mod.new("input", "dns name must be non-empty")
  end
  local out = {}
  local text = name:gsub("%.$", "")
  for label in text:gmatch("[^%.]+") do
    if #label == 0 or #label > 63 then
      return nil, error_mod.new("input", "dns label must be 1-63 bytes", { name = name })
    end
    out[#out + 1] = string.char(#label)
    out[#out + 1] = label
  end
  out[#out + 1] = "\0"
  return table.concat(out)
end

local function decode_name(packet, offset, depth)
  if type(packet) ~= "string" or type(offset) ~= "number" then
    return nil, nil, error_mod.new("input", "dns packet and offset are required")
  end
  if (depth or 0) > 16 then
    return nil, nil, error_mod.new("decode", "dns name compression pointer loop")
  end

  local labels = {}
  local i = offset
  local consumed = nil
  while true do
    local len = packet:byte(i)
    if not len then
      return nil, nil, error_mod.new("decode", "truncated dns name")
    end
    if len == 0 then
      i = i + 1
      consumed = consumed or i
      break
    end
    if (len & 0xc0) == 0xc0 then
      local next_byte = packet:byte(i + 1)
      if not next_byte then
        return nil, nil, error_mod.new("decode", "truncated dns compression pointer")
      end
      local pointer = ((len & 0x3f) << 8) | next_byte
      local pointed_name, _, pointed_err = decode_name(packet, pointer + 1, (depth or 0) + 1)
      if not pointed_name then
        return nil, nil, pointed_err
      end
      labels[#labels + 1] = pointed_name
      consumed = consumed or (i + 2)
      break
    end
    if (len & 0xc0) ~= 0 then
      return nil, nil, error_mod.new("decode", "unsupported dns label encoding")
    end
    local label = packet:sub(i + 1, i + len)
    if #label ~= len then
      return nil, nil, error_mod.new("decode", "truncated dns label")
    end
    labels[#labels + 1] = label
    i = i + len + 1
  end

  return table.concat(labels, "."), consumed
end

local function encode_txt_values(values)
  if type(values) == "string" then
    values = { values }
  end
  if type(values) ~= "table" then
    return nil, error_mod.new("input", "TXT data must be string or list")
  end
  local out = {}
  for _, value in ipairs(values) do
    if type(value) ~= "string" or #value > 255 then
      return nil, error_mod.new("input", "TXT value must be a string up to 255 bytes")
    end
    out[#out + 1] = string.char(#value)
    out[#out + 1] = value
  end
  return table.concat(out)
end

local function decode_txt_values(rdata)
  local out = {}
  local i = 1
  while i <= #rdata do
    local len = rdata:byte(i)
    if not len or i + len > #rdata + 1 then
      return nil, error_mod.new("decode", "truncated TXT data")
    end
    out[#out + 1] = rdata:sub(i + 1, i + len)
    i = i + len + 1
  end
  return out
end

function M.encode_question(name, qtype, qclass)
  local encoded_name, name_err = encode_name(name)
  if not encoded_name then
    return nil, name_err
  end
  return encoded_name .. u16be(qtype or M.TYPE_PTR) .. u16be(qclass or M.CLASS_IN)
end

function M.encode_record(record)
  local encoded_name, name_err = encode_name(record.name)
  if not encoded_name then
    return nil, name_err
  end
  local rtype = record.type
  local rdata
  if rtype == M.TYPE_PTR then
    local ptr_name, ptr_err = encode_name(record.data)
    if not ptr_name then
      return nil, ptr_err
    end
    rdata = ptr_name
  elseif rtype == M.TYPE_TXT then
    local txt, txt_err = encode_txt_values(record.data)
    if not txt then
      return nil, txt_err
    end
    rdata = txt
  else
    rdata = record.rdata or ""
  end
  return encoded_name
    .. u16be(rtype)
    .. u16be(record.class or M.CLASS_IN)
    .. u32be(record.ttl or 120)
    .. u16be(#rdata)
    .. rdata
end

function M.encode_message(message)
  local questions = message.questions or {}
  local answers = message.answers or {}
  local authorities = message.authorities or {}
  local additionals = message.additionals or {}
  local flags = message.flags or (message.response and 0x8400 or 0)
  local out = {
    u16be(message.id or 0),
    u16be(flags),
    u16be(#questions),
    u16be(#answers),
    u16be(#authorities),
    u16be(#additionals),
  }
  for _, question in ipairs(questions) do
    local encoded, err = M.encode_question(question.name, question.type, question.class)
    if not encoded then
      return nil, err
    end
    out[#out + 1] = encoded
  end
  for _, section in ipairs({ answers, authorities, additionals }) do
    for _, record in ipairs(section) do
      local encoded, err = M.encode_record(record)
      if not encoded then
        return nil, err
      end
      out[#out + 1] = encoded
    end
  end
  return table.concat(out)
end

local function decode_question(packet, offset)
  local name, next_offset, name_err = decode_name(packet, offset)
  if not name then
    return nil, nil, name_err
  end
  local qtype = read_u16(packet, next_offset)
  local qclass = read_u16(packet, next_offset + 2)
  if not qtype or not qclass then
    return nil, nil, error_mod.new("decode", "truncated dns question")
  end
  return { name = name, type = qtype, class = qclass }, next_offset + 4
end

local function decode_record(packet, offset)
  local name, next_offset, name_err = decode_name(packet, offset)
  if not name then
    return nil, nil, name_err
  end
  local rtype = read_u16(packet, next_offset)
  local rclass = read_u16(packet, next_offset + 2)
  local ttl = read_u32(packet, next_offset + 4)
  local rdlength = read_u16(packet, next_offset + 8)
  if not rtype or not rclass or not ttl or not rdlength then
    return nil, nil, error_mod.new("decode", "truncated dns record")
  end
  local rdata_offset = next_offset + 10
  local rdata = packet:sub(rdata_offset, rdata_offset + rdlength - 1)
  if #rdata ~= rdlength then
    return nil, nil, error_mod.new("decode", "truncated dns rdata")
  end

  local data = rdata
  if rtype == M.TYPE_PTR then
    local ptr_name, _, ptr_err = decode_name(packet, rdata_offset)
    if not ptr_name then
      return nil, nil, ptr_err
    end
    data = ptr_name
  elseif rtype == M.TYPE_TXT then
    local txt, txt_err = decode_txt_values(rdata)
    if not txt then
      return nil, nil, txt_err
    end
    data = txt
  end

  return {
    name = name,
    type = rtype,
    class = rclass,
    ttl = ttl,
    data = data,
    rdata = rdata,
  }, rdata_offset + rdlength
end

function M.decode_message(packet)
  if type(packet) ~= "string" or #packet < 12 then
    return nil, error_mod.new("decode", "dns packet header is truncated")
  end
  local msg = {
    id = read_u16(packet, 1),
    flags = read_u16(packet, 3),
    questions = {},
    answers = {},
    authorities = {},
    additionals = {},
  }
  local qdcount = read_u16(packet, 5)
  local ancount = read_u16(packet, 7)
  local nscount = read_u16(packet, 9)
  local arcount = read_u16(packet, 11)
  local offset = 13
  for _ = 1, qdcount do
    local question, next_offset, err = decode_question(packet, offset)
    if not question then
      return nil, err
    end
    msg.questions[#msg.questions + 1] = question
    offset = next_offset
  end
  for _, target in ipairs({
    { count = ancount, list = msg.answers },
    { count = nscount, list = msg.authorities },
    { count = arcount, list = msg.additionals },
  }) do
    for _ = 1, target.count do
      local record, next_offset, err = decode_record(packet, offset)
      if not record then
        return nil, err
      end
      target.list[#target.list + 1] = record
      offset = next_offset
    end
  end
  return msg
end

M.encode_name = encode_name
M.decode_name = decode_name

return M
