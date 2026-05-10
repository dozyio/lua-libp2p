local log = require("lua_libp2p.log")

local function assert_enabled(level, fields, expected, message)
  local enabled, err = log.enabled(level, fields)
  if enabled ~= expected then
    return nil, err or message
  end
  return true
end

local function run()
  log.reset()

  local ok, err = assert_enabled("info", { subsystem = "host" }, true, "info should be enabled by default")
  if not ok then
    return nil, err
  end
  ok, err = assert_enabled("debug", { subsystem = "host" }, false, "debug should be disabled by default")
  if not ok then
    return nil, err
  end

  ok, err = log.configure("kad_dht,host")
  if not ok then
    return nil, err
  end
  ok, err = assert_enabled("debug", { subsystem = "kad_dht" }, true, "named subsystem should enable debug logs")
  if not ok then
    return nil, err
  end
  ok, err = assert_enabled("debug", { subsystem = "identify" }, false, "unnamed subsystem should be filtered")
  if not ok then
    return nil, err
  end
  ok, err = assert_enabled("error", {}, false, "missing subsystem should be filtered when subsystem filter is active")
  if not ok then
    return nil, err
  end

  ok, err = log.configure("kad_dht=debug,host=info,*=warn")
  if not ok then
    return nil, err
  end
  ok, err = assert_enabled("debug", { subsystem = "kad_dht" }, true, "per-subsystem debug level should apply")
  if not ok then
    return nil, err
  end
  ok, err = assert_enabled("debug", { subsystem = "host" }, false, "per-subsystem info level should suppress debug")
  if not ok then
    return nil, err
  end
  ok, err = assert_enabled("info", { subsystem = "host" }, true, "per-subsystem info level should emit info")
  if not ok then
    return nil, err
  end
  ok, err = assert_enabled("info", { subsystem = "identify" }, false, "fallback warn level should suppress info")
  if not ok then
    return nil, err
  end
  ok, err = assert_enabled("warn", { subsystem = "identify" }, true, "fallback warn level should emit warn")
  if not ok then
    return nil, err
  end

  local kad_log = log.subsystem("kad_dht")
  local kad_enabled, kad_enabled_err = kad_log.enabled("debug")
  if kad_enabled ~= true then
    return nil, kad_enabled_err or "subsystem logger should attach subsystem fields"
  end
  local override_enabled, override_err = kad_log.enabled("debug", { subsystem = "host" })
  if override_enabled ~= false then
    return nil, override_err or "explicit subsystem field should override subsystem logger default"
  end

  ok, err = log.configure("debug")
  if not ok then
    return nil, err
  end
  ok, err = assert_enabled("debug", { subsystem = "any" }, true, "level-only config should set global level")
  if not ok then
    return nil, err
  end

  local bad, bad_err = log.configure("host=trace")
  if bad ~= nil or bad_err ~= "invalid log level" then
    return nil, "invalid subsystem level should be rejected"
  end

  log.reset()
  return true
end

return {
  name = "structured logging subsystem filters",
  run = run,
}
