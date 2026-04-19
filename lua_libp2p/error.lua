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

function M.new(kind, message, context)
  return Error:new(kind, message, context)
end

function M.wrap(kind, message, err, context)
  local merged = context or {}
  merged.cause = err
  return Error:new(kind, message, merged)
end

function M.is_error(value)
  return getmetatable(value) == Error
end

return M
