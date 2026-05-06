--- Table utility helpers.
-- @module lua_libp2p.util.tables
local M = {}

function M.copy_list(values)
  local out = {}
  for i, value in ipairs(values or {}) do
    out[i] = value
  end
  return out
end

return M
