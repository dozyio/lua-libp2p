--- Structured error helpers.
---@class Libp2pError
---@field kind string Error kind/category.
---@field message string Human-readable message.
---@field context table Structured context payload.

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
---@param kind string Error kind/category.
---@param message? string Human-readable message.
---@param context? table Structured context payload.
---@return Libp2pError err
function M.new(kind, message, context)
  return Error:new(kind, message, context)
end

--- Wrap an underlying error as cause.
---@param kind string Error kind/category.
---@param message? string Human-readable message.
---@param err any Underlying cause.
---@param context? table Extra context fields.
---@return Libp2pError wrapped
function M.wrap(kind, message, err, context)
  local merged = context or {}
  merged.cause = err
  return Error:new(kind, message, merged)
end

--- Check whether a value is a structured error.
---@param value any Candidate value.
---@return boolean
function M.is_error(value)
  return getmetatable(value) == Error
end

return M
