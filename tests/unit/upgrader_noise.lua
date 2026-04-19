local ed25519 = require("lua_libp2p.crypto.ed25519")
local mss = require("lua_libp2p.protocol.mss")
local upgrader = require("lua_libp2p.network.upgrader")
local yamux = require("lua_libp2p.muxer.yamux")

local function new_pair()
  local a = { _in = "", _peer = nil, _closed = false }
  local b = { _in = "", _peer = a, _closed = false }
  a._peer = b

  local function read(self, n)
    while #self._in < n do
      if self._closed then
        return nil, "closed"
      end
      coroutine.yield("wait")
    end
    local out = self._in:sub(1, n)
    self._in = self._in:sub(n + 1)
    return out
  end

  local function write(self, payload)
    if self._closed then
      return nil, "closed"
    end
    self._peer._in = self._peer._in .. payload
    return true
  end

  local function close(self)
    self._closed = true
    return true
  end

  a.read = read
  a.write = write
  a.close = close
  b.read = read
  b.write = write
  b.close = close
  return a, b
end

local function drive(co1, co2)
  local done1, done2 = false, false
  local r1, e1, r2, e2

  for _ = 1, 30000 do
    if not done1 then
      local ok, a, b = coroutine.resume(co1)
      if not ok then
        return nil, "outbound coroutine panic: " .. tostring(a)
      end
      if coroutine.status(co1) == "dead" then
        done1 = true
        r1, e1 = a, b
        if r1 == nil and e1 ~= nil then
          return { r1 = r1, e1 = e1, r2 = r2, e2 = e2 }
        end
      end
    end

    if not done2 then
      local ok, a, b = coroutine.resume(co2)
      if not ok then
        return nil, "inbound coroutine panic: " .. tostring(a)
      end
      if coroutine.status(co2) == "dead" then
        done2 = true
        r2, e2 = a, b
        if r2 == nil and e2 ~= nil then
          return { r1 = r1, e1 = e1, r2 = r2, e2 = e2 }
        end
      end
    end

    if done1 and done2 then
      return { r1 = r1, e1 = e1, r2 = r2, e2 = e2 }
    end
  end

  return nil, "upgrader noise scheduler iteration budget exceeded"
end

local function run()
  local client_id = assert(ed25519.generate_keypair())
  local server_id = assert(ed25519.generate_keypair())

  local a, b = new_pair()
  local out_conn, out_state, out_err
  local in_conn, in_state, in_err

  local co_out = coroutine.create(function()
    out_conn, out_state, out_err = upgrader.upgrade_outbound(a, {
      local_keypair = client_id,
      security_protocols = { "/noise" },
      muxer_protocols = { yamux.PROTOCOL_ID },
    })
    return out_conn, out_err
  end)

  local co_in = coroutine.create(function()
    in_conn, in_state, in_err = upgrader.upgrade_inbound(b, {
      local_keypair = server_id,
      security_protocols = { "/noise" },
      muxer_protocols = { yamux.PROTOCOL_ID },
    })
    return in_conn, in_err
  end)

  local ran, ran_err = drive(co_out, co_in)
  if not ran then
    return nil, ran_err
  end
  if not out_conn then
    return nil, tostring(out_err or ran.e1 or "outbound upgrade failed without error")
  end
  if not in_conn then
    return nil, tostring(in_err or ran.e2 or "inbound upgrade failed without error")
  end
  if out_state.security ~= "/noise" or in_state.security ~= "/noise" then
    return nil, "expected noise security negotiation"
  end

  local cs = assert(out_conn:new_stream_raw())
  for _ = 1, 6 do
    local _ = in_conn:process_one()
    local accepted = in_conn:accept_stream_raw()
    if accepted then
      local ok = cs:write("abc")
      if not ok then
        return nil, "failed to write on noise-upgraded yamux stream"
      end
      in_conn:process_one()
      local got = accepted:read_now()
      if got ~= "abc" then
        return nil, "noise upgrader stream payload mismatch"
      end
      return true
    end
  end

  return nil, "failed to open/accept stream after noise upgrade"
end

return {
  name = "connection upgrader noise+yamux",
  run = run,
}
