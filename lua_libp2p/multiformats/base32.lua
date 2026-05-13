--- Base32 codec.
local error_mod = require("lua_libp2p.error")

local M = {}

local ALPHABET = "abcdefghijklmnopqrstuvwxyz234567"
local INDEX = {}
for i = 1, #ALPHABET do
  INDEX[ALPHABET:sub(i, i)] = i - 1
end

--- data string
--- string text
function M.encode_nopad(data)
  if data == "" then
    return ""
  end

  local out = {}
  local buffer = 0
  local bits = 0

  for i = 1, #data do
    buffer = ((buffer << 8) | data:byte(i)) & 0xFFFFFFFF
    bits = bits + 8
    while bits >= 5 do
      local idx = (buffer >> (bits - 5)) & 0x1F
      out[#out + 1] = ALPHABET:sub(idx + 1, idx + 1)
      bits = bits - 5
    end
    if bits > 0 then
      buffer = buffer & ((1 << bits) - 1)
    else
      buffer = 0
    end
  end

  if bits > 0 then
    local idx = (buffer << (5 - bits)) & 0x1F
    out[#out + 1] = ALPHABET:sub(idx + 1, idx + 1)
  end

  return table.concat(out)
end

--- text string
--- string|nil bytes
--- table|nil err
function M.decode_nopad(text)
  if text == "" then
    return ""
  end

  local out = {}
  local buffer = 0
  local bits = 0
  local lower = text:lower()

  for i = 1, #lower do
    local ch = lower:sub(i, i)
    local value = INDEX[ch]
    if value == nil then
      return nil, error_mod.new("decode", "invalid base32 character", { char = ch })
    end

    buffer = ((buffer << 5) | value) & 0xFFFFFFFF
    bits = bits + 5
    while bits >= 8 do
      local byte = (buffer >> (bits - 8)) & 0xFF
      out[#out + 1] = string.char(byte)
      bits = bits - 8
    end
    if bits > 0 then
      buffer = buffer & ((1 << bits) - 1)
    else
      buffer = 0
    end
  end

  if bits > 0 and buffer ~= 0 then
    return nil, error_mod.new("decode", "invalid base32 trailing bits")
  end

  return table.concat(out)
end

return M
