local M = {}

function M.shell_quote(text)
  return "'" .. tostring(text):gsub("'", "'\\''") .. "'"
end

function M.write_file(path, content)
  local f, err = io.open(path, "wb")
  if not f then
    return nil, err
  end
  local ok, write_err = f:write(content)
  f:close()
  if not ok then
    return nil, write_err
  end
  return true
end

function M.read_file(path)
  local f = io.open(path, "rb")
  if not f then
    return nil
  end
  local data = f:read("*a")
  f:close()
  return data
end

function M.spawn_lua_background(script_path, args, log_path)
  local cmd = "lua " .. M.shell_quote(script_path)
  for _, value in ipairs(args or {}) do
    cmd = cmd .. " " .. M.shell_quote(value)
  end
  if log_path then
    cmd = cmd .. " >" .. M.shell_quote(log_path) .. " 2>&1"
  end
  cmd = cmd .. " &"

  local ok = os.execute(cmd)
  if ok ~= true and ok ~= 0 then
    return nil, cmd
  end
  return true, cmd
end

function M.remove_files(paths)
  for _, path in ipairs(paths or {}) do
    os.remove(path)
  end
end

function M.run_luv_child_file_case(opts)
  local options = opts or {}
  local uv = options.uv or require("luv")
  local host = options.host
  local child_script = os.tmpname() .. ".lua"
  local child_out = os.tmpname() .. ".txt"
  local child_log = os.tmpname() .. ".log"
  local files = { child_script, child_out, child_log }

  local wrote, write_err = M.write_file(child_script, options.child_source)
  if not wrote then
    if host then
      host:stop()
    end
    return nil, write_err
  end

  local args = {}
  for _, value in ipairs(options.child_args or {}) do
    args[#args + 1] = value
  end
  args[#args + 1] = child_out

  local spawn_ok = M.spawn_lua_background(child_script, args, child_log)
  if not spawn_ok then
    if host then
      host:stop()
    end
    M.remove_files(files)
    return nil, options.spawn_error or "failed to spawn child process"
  end

  local completed = false
  local timeout_hit = false
  local timeout_timer
  local poll_timer = assert(uv.new_timer())
  poll_timer:start(options.poll_delay_ms or 10, options.poll_interval_ms or 20, function()
    local content = M.read_file(child_out)
    if content then
      completed = true
      poll_timer:stop()
      poll_timer:close()
      if timeout_timer then
        timeout_timer:stop()
        timeout_timer:close()
      end
      if host then
        host:stop()
      end
    end
  end)

  timeout_timer = assert(uv.new_timer())
  timeout_timer:start(options.timeout_ms or 3000, 0, function()
    timeout_hit = true
    timeout_timer:stop()
    timeout_timer:close()
    pcall(function()
      poll_timer:stop()
      poll_timer:close()
    end)
    if host then
      host:stop()
    end
  end)

  uv.run("default")

  local result = M.read_file(child_out)
  local log_out = M.read_file(child_log)
  M.remove_files(files)

  if timeout_hit then
    return nil, options.timeout_error or "timed out waiting for child process"
  end
  if not completed then
    return nil, options.incomplete_error or "child process did not complete"
  end
  if options.expected_result ~= nil and result ~= options.expected_result then
    return nil, (options.result_error_prefix or "child process failed: ") .. tostring(result or log_out)
  end

  return true, result, log_out
end

return M
