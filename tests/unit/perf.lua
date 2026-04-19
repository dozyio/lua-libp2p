local error_mod = require("lua_libp2p.error")
local perf = require("lua_libp2p.protocol.perf")

local function new_pair()
  local a = {
    _in = "",
    _closed_read = false,
    _closed_write = false,
    _peer = nil,
  }
  local b = {
    _in = "",
    _closed_read = false,
    _closed_write = false,
    _peer = a,
  }
  a._peer = b

  local function read(self, n)
    while #self._in < n do
      if self._closed_read then
        return nil, error_mod.new("closed", "stream closed during read")
      end
      coroutine.yield("wait")
    end
    local out = self._in:sub(1, n)
    self._in = self._in:sub(n + 1)
    return out
  end

  local function read_now(self)
    if #self._in == 0 then
      return nil
    end
    local out = self._in
    self._in = ""
    return out
  end

  local function write(self, payload)
    if self._closed_write then
      return nil, error_mod.new("closed", "stream closed for writing")
    end
    self._peer._in = self._peer._in .. payload
    return true
  end

  local function close_write(self)
    if self._closed_write then
      return true
    end
    self._closed_write = true
    self._peer._closed_read = true
    return true
  end

  a.read = read
  a.read_now = read_now
  a.write = write
  a.close_write = close_write

  b.read = read
  b.read_now = read_now
  b.write = write
  b.close_write = close_write

  return a, b
end

local function drive(co1, co2)
  local done1, done2 = false, false
  local r1, e1, r2, e2
  for _ = 1, 500000 do
    if not done1 then
      local ok, a, b = coroutine.resume(co1)
      if not ok then
        return nil, "client coroutine panic: " .. tostring(a)
      end
      if coroutine.status(co1) == "dead" then
        done1 = true
        r1, e1 = a, b
      end
    end

    if not done2 then
      local ok, a, b = coroutine.resume(co2)
      if not ok then
        return nil, "server coroutine panic: " .. tostring(a)
      end
      if coroutine.status(co2) == "dead" then
        done2 = true
        r2, e2 = a, b
      end
    end

    if done1 and done2 then
      return { r1 = r1, e1 = e1, r2 = r2, e2 = e2 }
    end
  end
  return nil, "scheduler budget exceeded"
end

local function run()
  local encoded = assert(perf.encode_u64be(4294967303))
  local decoded = assert(perf.decode_u64be(encoded))
  if decoded ~= 4294967303 then
    return nil, "u64 be encode/decode mismatch"
  end

  local client_conn, server_conn = new_pair()
  local client_result, client_err
  local server_result, server_err

  local co_client = coroutine.create(function()
    client_result, client_err = perf.measure_once(client_conn, 130, 257, {
      write_block_size = 64,
    })
    return client_result, client_err
  end)

  local co_server = coroutine.create(function()
    server_result, server_err = perf.handle(server_conn, {
      write_block_size = 64,
    })
    return server_result, server_err
  end)

  local ran, ran_err = drive(co_client, co_server)
  if not ran then
    return nil, ran_err
  end

  if not client_result then
    return nil, client_err or ran.e1 or "client perf measurement failed"
  end
  if not server_result then
    return nil, server_err or ran.e2 or "server perf handler failed"
  end

  if client_result.upload_bytes ~= 130 then
    return nil, "client upload bytes mismatch"
  end
  if client_result.download_bytes ~= 257 then
    return nil, "client download bytes mismatch"
  end
  if server_result.uploaded_bytes ~= 130 then
    return nil, "server uploaded bytes mismatch"
  end
  if server_result.download_bytes ~= 257 then
    return nil, "server download bytes mismatch"
  end

  return true
end

return {
  name = "perf protocol upload/download",
  run = run,
}
