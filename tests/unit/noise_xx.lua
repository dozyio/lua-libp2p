local ed25519 = require("lua_libp2p.crypto.ed25519")
local noise = require("lua_libp2p.connection_encrypter_noise.protocol")

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

local function run_coroutines(co1, co2)
  local done1, done2 = false, false
  local res1, res2
  local err1, err2

  for _ = 1, 20000 do
    if not done1 then
      local ok, r1, r2, r3 = coroutine.resume(co1)
      if not ok then
        return nil, "outbound coroutine failed: " .. tostring(r1)
      end
      if coroutine.status(co1) == "dead" then
        done1 = true
        res1, err1 = r1, r2
        if res1 == nil and err1 ~= nil then
          return { res1 = res1, err1 = err1, res2 = res2, err2 = err2 }
        end
      end
    end

    if not done2 then
      local ok, r1, r2, r3 = coroutine.resume(co2)
      if not ok then
        return nil, "inbound coroutine failed: " .. tostring(r1)
      end
      if coroutine.status(co2) == "dead" then
        done2 = true
        res2, err2 = r1, r2
        if res2 == nil and err2 ~= nil then
          return { res1 = res1, err1 = err1, res2 = res2, err2 = err2 }
        end
      end
    end

    if done1 and done2 then
      return { res1 = res1, err1 = err1, res2 = res2, err2 = err2 }
    end
  end

  return nil, "noise handshake scheduler exceeded iteration budget"
end

local function run()
  local client_id = assert(ed25519.generate_keypair())
  local server_id = assert(ed25519.generate_keypair())

  local a, b = new_pair()

  local out_secure, out_state, out_err
  local in_secure, in_state, in_err

  local co_out = coroutine.create(function()
    out_secure, out_state, out_err = noise.handshake_xx_outbound(a, {
      identity_keypair = client_id,
    })
    return out_secure, out_err
  end)

  local co_in = coroutine.create(function()
    in_secure, in_state, in_err = noise.handshake_xx_inbound(b, {
      identity_keypair = server_id,
    })
    return in_secure, in_err
  end)

  local ran, ran_err = run_coroutines(co_out, co_in)
  if not ran then
    return nil, ran_err
  end

  if not out_secure then
    return nil, out_err
  end
  if not in_secure then
    return nil, in_err
  end

  if not out_state or not out_state.remote_peer then
    return nil, "missing outbound remote peer state"
  end
  if not in_state or not in_state.remote_peer then
    return nil, "missing inbound remote peer state"
  end

  local ok, werr = out_secure:write("hello")
  if not ok then
    return nil, werr
  end
  local got, rerr = in_secure:read(5)
  if not got then
    return nil, rerr
  end
  if got ~= "hello" then
    return nil, "noise secure transport message mismatch (out->in)"
  end

  ok, werr = in_secure:write("world")
  if not ok then
    return nil, werr
  end
  got, rerr = out_secure:read(5)
  if not got then
    return nil, rerr
  end
  if got ~= "world" then
    return nil, "noise secure transport message mismatch (in->out)"
  end

  return true
end

return {
  name = "noise xx handshake and secure transport",
  run = run,
}
