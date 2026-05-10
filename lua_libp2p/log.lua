--- Lightweight structured logging helpers.
-- @module lua_libp2p.log
local tables = require("lua_libp2p.util.tables")

local M = {}

local LEVELS = {
  debug = 10,
  info = 20,
  warn = 30,
  error = 40,
}

local current_level = LEVELS.info
local subsystem_levels = nil
local default_subsystem_level = nil

local function trim(value)
  return tostring(value or ""):match("^%s*(.-)%s*$")
end

local function split_csv(value)
  local out = {}
  for part in tostring(value or ""):gmatch("[^,]+") do
    local item = trim(part)
    if item ~= "" then out[#out + 1] = item end
  end
  return out
end

local function parse_level(value)
  return LEVELS[trim(value):lower()]
end

local function field_subsystem(fields)
  if type(fields) ~= "table" then return nil end
  local subsystem = fields.subsystem
  if type(subsystem) ~= "string" or subsystem == "" then return nil end
  return subsystem
end

local function with_subsystem(name, fields)
  local out = {}
  for k, v in pairs(fields or {}) do
    out[k] = v
  end
  if out.subsystem == nil then
    out.subsystem = name
  end
  return out
end

local function level_name_for(level)
  for name, value in pairs(LEVELS) do
    if value == level then return name end
  end
  return nil
end

local function configure_from_spec(spec)
  local text = trim(spec)
  if text == "" then
    subsystem_levels = nil
    default_subsystem_level = nil
    return true
  end

  local all_level = parse_level(text)
  if all_level then
    current_level = all_level
    subsystem_levels = nil
    default_subsystem_level = nil
    return true
  end

  local levels = {}
  local default_level = nil
  for _, item in ipairs(split_csv(text)) do
    local name, level = item:match("^([^=]+)=(.+)$")
    if name then
      name = trim(name)
      local parsed_level = parse_level(level)
      if not parsed_level then
        return nil, "invalid log level"
      end
      if name == "*" or name == "all" then
        default_level = parsed_level
      else
        levels[name] = parsed_level
      end
    else
      levels[item] = LEVELS.debug
    end
  end

  subsystem_levels = levels
  default_subsystem_level = default_level
  return true
end

local function env(name)
  local ok, value = pcall(os.getenv, name)
  if ok then return value end
  return nil
end

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

--- Configure subsystem logging.
-- `spec` accepts `debug`, `info`, `warn`, `error`, comma-separated subsystem
-- names, or `subsystem=level` pairs. `*`/`all` sets the fallback level.
-- Examples: `kad_dht,host`, `kad_dht=debug,host=info,*=warn`.
-- @tparam string spec
-- @treturn true|nil ok
-- @treturn[opt] string err
function M.configure(spec)
  return configure_from_spec(spec)
end

--- Reset logging to default info-level, all-subsystem behavior.
function M.reset()
  current_level = LEVELS.info
  subsystem_levels = nil
  default_subsystem_level = nil
  return true
end

--- Capture current logging configuration for later restore.
-- @treturn table snapshot
function M.snapshot()
  return {
    current_level = current_level,
    subsystem_levels = tables.copy_table(subsystem_levels),
    default_subsystem_level = default_subsystem_level,
  }
end

--- Restore logging configuration from a prior snapshot.
-- @tparam table snapshot
-- @treturn true|nil ok
-- @treturn[opt] string err
function M.restore(snapshot)
  if type(snapshot) ~= "table" then
    return nil, "invalid log snapshot"
  end
  current_level = tonumber(snapshot.current_level) or LEVELS.info
  subsystem_levels = nil
  if type(snapshot.subsystem_levels) == "table" then
    subsystem_levels = tables.copy_table(snapshot.subsystem_levels)
  end
  default_subsystem_level = snapshot.default_subsystem_level
  return true
end

--- Return whether a log event would be emitted.
function M.enabled(level_name, fields)
  local level = LEVELS[level_name]
  if not level then
    return nil, "invalid log level"
  end
  if subsystem_levels ~= nil or default_subsystem_level ~= nil then
    local subsystem = field_subsystem(fields)
    local subsystem_level = subsystem and subsystem_levels and subsystem_levels[subsystem] or default_subsystem_level
    if subsystem_level == nil then
      return false
    end
    return level >= subsystem_level
  end
  return level >= current_level
end

--- Return current global level name.
function M.level()
  return level_name_for(current_level)
end

--- Create a logger that automatically attaches a subsystem field.
-- @tparam string name Subsystem name.
-- @treturn table logger
function M.subsystem(name)
  if type(name) ~= "string" or name == "" then
    error("log subsystem name must be a non-empty string", 2)
  end
  return {
    log = function(level_name, message, fields)
      return M.log(level_name, message, with_subsystem(name, fields))
    end,
    debug = function(message, fields)
      return M.debug(message, with_subsystem(name, fields))
    end,
    info = function(message, fields)
      return M.info(message, with_subsystem(name, fields))
    end,
    warn = function(message, fields)
      return M.warn(message, with_subsystem(name, fields))
    end,
    error = function(message, fields)
      return M.error(message, with_subsystem(name, fields))
    end,
    enabled = function(level_name, fields)
      return M.enabled(level_name, with_subsystem(name, fields))
    end,
  }
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
  local enabled, enabled_err = M.enabled(level_name, fields)
  if enabled == nil then
    return nil, enabled_err
  end
  if not enabled then
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

local env_level = env("LIBP2P_LOG_LEVEL")
if env_level and env_level ~= "" then
  M.set_level(trim(env_level):lower())
end

local env_spec = env("LIBP2P_LOG")
if env_spec and env_spec ~= "" then
  M.configure(env_spec)
end

return M
