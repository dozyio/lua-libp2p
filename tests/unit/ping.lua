local ping = require("lua_libp2p.protocol_ping.protocol")

local function new_buffered_conn(initial)
  local conn = {
    _in = initial or "",
    _out = "",
  }

  function conn:read(n)
    local want = n or #self._in
    if #self._in < want then
      return nil, "unexpected EOF"
    end
    local out = self._in:sub(1, want)
    self._in = self._in:sub(want + 1)
    return out
  end

  function conn:write(payload)
    self._out = self._out .. payload
    return true
  end

  function conn:writes()
    return self._out
  end

  return conn
end

local function new_echo_conn()
  local conn = {
    _in = "",
    _out = "",
  }

  function conn:read(n)
    local want = n or #self._in
    if #self._in < want then
      return nil, "unexpected EOF"
    end
    local out = self._in:sub(1, want)
    self._in = self._in:sub(want + 1)
    return out
  end

  function conn:write(payload)
    self._out = self._out .. payload
    self._in = self._in .. payload
    return true
  end

  return conn
end

local function run()
  local payload = string.rep("a", ping.PAYLOAD_SIZE)

  local server = new_buffered_conn(payload)
  local ok, err = ping.handle_once(server)
  if not ok then
    return nil, err
  end
  if server:writes() ~= payload then
    return nil, "ping handler did not echo payload"
  end

  local client = new_echo_conn()
  local result, ping_err = ping.ping_once(client, payload)
  if not result then
    return nil, ping_err
  end
  if result.payload ~= payload then
    return nil, "ping result payload mismatch"
  end
  if type(result.rtt_seconds) ~= "number" or result.rtt_seconds < 0 then
    return nil, "ping result RTT is invalid"
  end

  local _, bad_size_err = ping.ping_once(client, "short")
  if not bad_size_err then
    return nil, "expected ping payload size validation error"
  end

  return true
end

return {
  name = "ping protocol echo and RTT",
  run = run,
}
