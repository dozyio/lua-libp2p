--- Unsigned varint encode/decode helpers.
local error_mod = require("lua_libp2p.error")

local M = {}

--- n integer
--- string|nil bytes
--- table|nil err
function M.encode_u64(n)
  if type(n) ~= "number" or n < 0 then
    return nil, error_mod.new("input", "varint value must be a non-negative number")
  end

  local out = {}
  local value = n
  repeat
    local byte = value % 128
    value = math.floor(value / 128)
    if value > 0 then
      byte = byte + 128
    end
    out[#out + 1] = string.char(byte)
  until value == 0

  return table.concat(out)
end

--- data string
--- offset? integer
--- integer|nil value
--- integer|table next_offset_or_err
function M.decode_u64(data, offset)
  if type(data) ~= "string" then
    return nil, error_mod.new("input", "varint input must be bytes")
  end

  local result = 0
  local shift = 0
  local i = offset or 1
  local start = i

  while i <= #data do
    local byte = data:byte(i)
    local value = byte % 128
    result = result + value * (2 ^ shift)
    i = i + 1

    if byte < 128 then
      local min_enc = M.encode_u64(result)
      local observed = data:sub(start, i - 1)
      if observed ~= min_enc then
        return nil, error_mod.new("decode", "non-minimal varint encoding")
      end
      return result, i
    end

    shift = shift + 7
    if shift > 63 then
      return nil, error_mod.new("decode", "varint too large")
    end
  end

  return nil, error_mod.new("decode", "truncated varint")
end

return M
