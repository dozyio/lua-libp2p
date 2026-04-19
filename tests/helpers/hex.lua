local M = {}

function M.decode(hex)
  local cleaned = hex:gsub("%s+", "")
  if #cleaned % 2 ~= 0 then
    return nil, "hex string has odd length"
  end
  return (cleaned:gsub("..", function(pair)
    return string.char(tonumber(pair, 16))
  end))
end

function M.encode(bytes)
  return (bytes:gsub(".", function(ch)
    return string.format("%02x", string.byte(ch))
  end))
end

return M
