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

return M
