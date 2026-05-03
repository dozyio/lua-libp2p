--- Lightweight structured logging helpers.
-- @module lua_libp2p.log
local M = {}

local LEVELS = {
  debug = 10,
  info = 20,
  warn = 30,
  error = 40,
}

local current_level = LEVELS.info

local function now_iso8601()
  return os.date("!%Y-%m-%dT%H:%M:%SZ")
end

local function stringify_fields(fields)
  if not fields then
    return ""
  end

  local parts = {}
  for k, v in pairs(fields) do
    parts[#parts + 1] = string.format("%s=%s", tostring(k), tostring(v))
  end
  table.sort(parts)
  return table.concat(parts, " ")
end

--- Set minimum log level.
-- @tparam string name One of `debug|info|warn|error`.
-- @treturn true|nil ok
-- @treturn[opt] string err
function M.set_level(name)
  local level = LEVELS[name]
  if not level then
    return nil, "invalid log level"
  end
  current_level = level
  return true
end

--- Emit a structured log line.
-- @tparam string level_name One of `debug|info|warn|error`.
-- @tparam string message Log message.
-- @tparam[opt] table fields Key/value metadata.
-- @treturn true|nil ok
-- @treturn[opt] string err
function M.log(level_name, message, fields)
  local level = LEVELS[level_name]
  if not level then
    return nil, "invalid log level"
  end
  if level < current_level then
    return true
  end

  local suffix = stringify_fields(fields)
  if suffix ~= "" then
    io.stderr:write(string.format("%s [%s] %s %s\n", now_iso8601(), level_name, message, suffix))
  else
    io.stderr:write(string.format("%s [%s] %s\n", now_iso8601(), level_name, message))
  end

  return true
end

--- Shortcut for `debug` level logging.
function M.debug(message, fields)
  return M.log("debug", message, fields)
end

--- Shortcut for `info` level logging.
function M.info(message, fields)
  return M.log("info", message, fields)
end

--- Shortcut for `warn` level logging.
function M.warn(message, fields)
  return M.log("warn", message, fields)
end

--- Shortcut for `error` level logging.
function M.error(message, fields)
  return M.log("error", message, fields)
end

return M
