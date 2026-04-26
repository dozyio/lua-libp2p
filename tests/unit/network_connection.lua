local connection = require("lua_libp2p.network.connection")
local mss = require("lua_libp2p.protocol.mss")

local function new_scripted_stream(incoming)
  local stream = {
    _in = incoming or "",
    _out = "",
  }

  function stream:read(n)
    local want = n or #self._in
    if #self._in < want then
      return nil, "unexpected EOF"
    end
    local out = self._in:sub(1, want)
    self._in = self._in:sub(want + 1)
    return out
  end

  function stream:write(payload)
    self._out = self._out .. payload
    return true
  end

  function stream:writes()
    return self._out
  end

  function stream:close()
    return true
  end

  return stream
end

local function new_fake_muxer(stream)
  local muxer = {
    stream = stream,
    opened = false,
  }

  function muxer:open_stream()
    if self.opened then
      return nil, "already opened"
    end
    self.opened = true
    return self.stream
  end

  function muxer:accept_stream_now()
    return nil
  end

  function muxer:process_one()
    return true
  end

  return muxer
end

local function run()
  local direct_raw = new_scripted_stream("")
  local direct = connection.from_raw(direct_raw)
  local s1, s1_err = direct:new_stream_raw()
  if not s1 then
    return nil, s1_err
  end
  local s2 = direct:new_stream_raw()
  if s2 ~= nil then
    return nil, "expected raw stream consumption guard"
  end

  local proto = "/toy/1.0.0"
  local inbound = table.concat({
    assert(mss.encode_frame(mss.PROTOCOL_ID)),
    assert(mss.encode_frame(proto)),
  })
  local stream = new_scripted_stream(inbound)
  local mux_conn = connection.from_raw(new_scripted_stream(""), {
    session = new_fake_muxer(stream),
  })

  local selected_stream, selected, sel_err = mux_conn:new_stream({ proto })
  if not selected_stream then
    return nil, sel_err
  end
  if selected ~= proto then
    return nil, "unexpected selected protocol"
  end

  local expected_writes = table.concat({
    assert(mss.encode_frame(mss.PROTOCOL_ID)),
    assert(mss.encode_frame(proto)),
  })
  if stream:writes() ~= expected_writes then
    return nil, "network connection did not negotiate using mss"
  end

  if mux_conn:session() == nil then
    return nil, "expected generic session accessor"
  end

  return true
end

return {
  name = "network connection abstraction",
  run = run,
}
