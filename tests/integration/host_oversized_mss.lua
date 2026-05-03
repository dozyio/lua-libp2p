local host_mod = require("lua_libp2p.host")
local mss = require("lua_libp2p.multistream_select.protocol")
local tcp = require("lua_libp2p.transport_tcp.transport")
local varint = require("lua_libp2p.multiformats.varint")

local function close_quiet(value)
  if value and type(value.close) == "function" then
    pcall(function()
      value:close()
    end)
  end
end

local function run()
  local h, host_err = host_mod.new({
    runtime = "luv",
    listen_addrs = { "/ip4/127.0.0.1/tcp/0" },
    blocking = false,
    accept_timeout = 0.05,
  })
  if not h then
    return nil, host_err
  end

  local started, start_err = h:start()
  if not started then
    close_quiet(h)
    return nil, start_err
  end

  local addrs = h:get_multiaddrs_raw()
  if #addrs == 0 then
    close_quiet(h)
    return nil, "expected host to expose at least one listen address"
  end

  local attacker, dial_err = tcp.dial(addrs[1], {
    timeout = 1,
    io_timeout = 1,
  })
  if not attacker then
    close_quiet(h)
    return nil, dial_err
  end

  local oversized_len, len_err = varint.encode_u64(mss.MAX_PROTOCOL_LENGTH + 1)
  if not oversized_len then
    close_quiet(attacker)
    close_quiet(h)
    return nil, len_err
  end

  local wrote, write_err = attacker:write(oversized_len)
  if not wrote then
    close_quiet(attacker)
    close_quiet(h)
    return nil, write_err
  end

  local ok, poll_err = h:poll_once(0.2)
  close_quiet(attacker)
  if not ok then
    close_quiet(h)
    return nil, poll_err
  end

  if not h:is_running() then
    close_quiet(h)
    return nil, "host should remain running after oversized multistream frame"
  end

  ok, poll_err = h:poll_once(0)
  if not ok then
    close_quiet(h)
    return nil, poll_err
  end

  local stopped, stop_err = h:stop()
  if not stopped then
    close_quiet(h)
    return nil, stop_err
  end

  return true
end

return {
  name = "host ignores oversized mss frame",
  run = run,
}
