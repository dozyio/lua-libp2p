local error_mod = require("lua_libp2p.error")
local loopback = require("tests.helpers.loopback")

local function run()
  local a, _ = loopback.new_pair()

  local ok, close_err = a:close()
  if not ok then
    return nil, close_err
  end

  local _, read_err = a:read()
  if not read_err then
    return nil, "expected read error on closed endpoint"
  end
  if not error_mod.is_error(read_err) then
    return nil, "expected typed error object"
  end
  if read_err.kind ~= "closed" then
    return nil, "expected closed error kind"
  end

  local _, write_err = a:write("x")
  if not write_err then
    return nil, "expected write error on closed endpoint"
  end
  if write_err.kind ~= "closed" then
    return nil, "expected closed write error kind"
  end

  return true
end

return {
  name = "loopback closed endpoint errors",
  run = run,
}
