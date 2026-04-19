local dummy = require("lua_libp2p.protocol.dummy")
local loopback = require("tests.helpers.loopback")

local function run()
  local client, server = loopback.new_pair()

  local sent = "hello-libp2p"
  local write_ok, write_err = client:write(sent)
  if not write_ok then
    return nil, write_err
  end

  local ok, handle_err = dummy.handle(server)
  if not ok then
    return nil, handle_err
  end

  local response, req_err = client:read()
  if not response then
    return nil, req_err
  end

  if response ~= "dummy-ok:" .. sent then
    return nil, "unexpected response: " .. tostring(response)
  end

  return true
end

return {
  name = "dummy loopback protocol",
  run = run,
}
