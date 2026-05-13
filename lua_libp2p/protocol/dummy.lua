--- Dummy protocol helpers for tests/examples.
local M = {}

function M.handle(conn)
  local payload, err = conn:read()
  if not payload then
    return nil, err
  end

  local ok, write_err = conn:write("dummy-ok:" .. payload)
  if not ok then
    return nil, write_err
  end

  return true
end

function M.request(conn, payload)
  local ok, err = conn:write(payload)
  if not ok then
    return nil, err
  end

  return conn:read()
end

return M
