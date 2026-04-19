local ed25519 = require("lua_libp2p.crypto.ed25519")
local host = require("lua_libp2p.host")
local ping = require("lua_libp2p.protocol.ping")

local function run()
  local keypair, key_err = ed25519.generate_keypair()
  if not keypair then
    return nil, key_err
  end

  local h, h_err = host.new({
    identity = keypair,
    listen_addrs = { "/ip4/127.0.0.1/tcp/0" },
    transports = { "tcp" },
    security_transports = { "/plaintext/2.0.0" },
    muxers = { "/yamux/1.0.0" },
  })
  if not h then
    return nil, h_err
  end

  local ok, reg_err = h:handle(ping.ID, function()
    return true
  end)
  if not ok then
    return nil, reg_err
  end

  local addrs, listen_err = h:listen()
  if not addrs then
    return nil, listen_err
  end
  if #addrs ~= 1 then
    return nil, "expected one listen address"
  end
  if not addrs[1]:match("^/ip4/127.0.0.1/tcp/%d+$") then
    return nil, "unexpected listen address shape"
  end

  local listed = h:get_listen_addrs()
  if #listed ~= 1 or listed[1] ~= addrs[1] then
    return nil, "get_listen_addrs mismatch"
  end

  local started, start_err = h:start({ max_iterations = 1, blocking = true, poll_interval = 0 })
  if not started then
    return nil, start_err
  end
  if not h:is_running() then
    return nil, "host should be running after start"
  end

  local stopped, stop_err = h:stop()
  if not stopped then
    return nil, stop_err
  end
  if h:is_running() then
    return nil, "host should not be running after stop"
  end

  local _, _, dial_err = h:dial("12D3KooWCryG7Mon9orvQxcS1rYZjotPgpwoJNHHKcLLfE4Hf5mV")
  if not dial_err then
    return nil, "expected dial error for peer id without address"
  end

  h:close()
  return true
end

return {
  name = "host config and lifecycle",
  run = run,
}
