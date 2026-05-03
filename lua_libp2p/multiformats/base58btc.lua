--- Base58btc codec.
-- @module lua_libp2p.multiformats.base58btc
local error_mod = require("lua_libp2p.error")

local M = {}

local ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
local INDEX = {}
for i = 1, #ALPHABET do
  INDEX[ALPHABET:sub(i, i)] = i - 1
end

function M.encode(data)
  if data == "" then
    return ""
  end

  local bytes = { data:byte(1, #data) }
  local zeros = 0
  while zeros < #bytes and bytes[zeros + 1] == 0 do
    zeros = zeros + 1
  end

  local result = {}
  local start_at = zeros + 1
  while start_at <= #bytes do
    local remainder = 0
    for i = start_at, #bytes do
      local value = bytes[i] + remainder * 256
      bytes[i] = math.floor(value / 58)
      remainder = value % 58
    end

    result[#result + 1] = ALPHABET:sub(remainder + 1, remainder + 1)
    while start_at <= #bytes and bytes[start_at] == 0 do
      start_at = start_at + 1
    end
  end

  for _ = 1, zeros do
    result[#result + 1] = "1"
  end

  local out = {}
  for i = #result, 1, -1 do
    out[#out + 1] = result[i]
  end
  return table.concat(out)
end

function M.decode(text)
  if text == "" then
    return ""
  end

  local bytes = { 0 }
  for i = 1, #text do
    local ch = text:sub(i, i)
    local value = INDEX[ch]
    if value == nil then
      return nil, error_mod.new("decode", "invalid base58 character", { char = ch })
    end

    local carry = value
    for j = 1, #bytes do
      local x = bytes[j] * 58 + carry
      bytes[j] = x % 256
      carry = math.floor(x / 256)
    end
    while carry > 0 do
      bytes[#bytes + 1] = carry % 256
      carry = math.floor(carry / 256)
    end
  end

  local leading_ones = 0
  while leading_ones < #text and text:sub(leading_ones + 1, leading_ones + 1) == "1" do
    leading_ones = leading_ones + 1
  end

  local out = {}
  for _ = 1, leading_ones do
    out[#out + 1] = string.char(0)
  end
  for i = #bytes, 1, -1 do
    out[#out + 1] = string.char(bytes[i])
  end
  return table.concat(out)
end

return M
