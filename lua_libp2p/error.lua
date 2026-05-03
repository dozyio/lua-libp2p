--- Structured error helpers.
-- @module lua_libp2p.error
local M = {}

local Error = {}
Error.__index = Error

function Error:new(kind, message, context)
  return setmetatable({
    kind = kind or "unknown",
    message = message or "",
    context = context or {},
  }, self)
end

function Error:__tostring()
  if self.message == "" then
    return self.kind
  end
  return string.format("%s: %s", self.kind, self.message)
end

--- Create a structured error object.
-- @tparam string kind Error kind/category.
-- @tparam[opt] string message Human-readable message.
-- @tparam[opt] table context Structured context payload.
-- @treturn table err
function M.new(kind, message, context)
  return Error:new(kind, message, context)
end

--- Wrap an underlying error as cause.
-- @tparam string kind Error kind/category.
-- @tparam[opt] string message Human-readable message.
-- @param err Underlying cause.
-- @tparam[opt] table context Extra context fields.
-- @treturn table wrapped
function M.wrap(kind, message, err, context)
  local merged = context or {}
  merged.cause = err
  return Error:new(kind, message, merged)
end

--- Check whether a value is a structured error.
-- @param value Candidate value.
-- @treturn boolean
function M.is_error(value)
  return getmetatable(value) == Error
end

return M
