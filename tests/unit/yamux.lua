local error_mod = require("lua_libp2p.error")
local yamux = require("lua_libp2p.muxer.yamux")

local function new_scripted_conn(incoming)
  local conn = {
    _in = incoming or "",
    _out = "",
  }

  function conn:read(n)
    local want = n or #self._in
    if #self._in < want then
      return nil, error_mod.new("io", "unexpected EOF")
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

local function run()
  local header, enc_err = yamux.encode_header({
    type = yamux.TYPE.DATA,
    flags = yamux.FLAG.SYN,
    stream_id = 3,
    length = 5,
  })
  if not header then
    return nil, enc_err
  end

  local decoded, dec_err = yamux.decode_header(header)
  if not decoded then
    return nil, dec_err
  end
  if decoded.type ~= yamux.TYPE.DATA or decoded.flags ~= yamux.FLAG.SYN then
    return nil, "yamux header decode mismatch"
  end
  if decoded.stream_id ~= 3 or decoded.length ~= 5 then
    return nil, "yamux header numeric decode mismatch"
  end

  local payload = "hello"
  local writer = new_scripted_conn("")
  local ok, write_err = yamux.write_frame(writer, {
    type = yamux.TYPE.DATA,
    flags = 0,
    stream_id = 9,
    payload = payload,
  })
  if not ok then
    return nil, write_err
  end

  local reader = new_scripted_conn(writer:writes())
  local frame, frame_err = yamux.read_frame(reader)
  if not frame then
    return nil, frame_err
  end
  if frame.stream_id ~= 9 or frame.payload ~= payload then
    return nil, "yamux frame read/write mismatch"
  end

  local backlog_conn = new_scripted_conn("")
  local session = yamux.new_session(backlog_conn, { is_client = true, max_ack_backlog = 1 })
  local s1, s1_err = session:open_stream()
  if not s1 then
    return nil, s1_err
  end
  local s2, s2_err = session:open_stream()
  if s2 ~= nil then
    return nil, "expected ack backlog limit to reject second open"
  end
  if not s2_err or s2_err.kind ~= "backlog" then
    return nil, "expected backlog error kind on open_stream"
  end

  local inbound_writer = new_scripted_conn("")
  assert(yamux.write_frame(inbound_writer, {
    type = yamux.TYPE.WINDOW_UPDATE,
    flags = yamux.FLAG.SYN,
    stream_id = 2,
    length = yamux.INITIAL_STREAM_WINDOW,
  }))
  assert(yamux.write_frame(inbound_writer, {
    type = yamux.TYPE.WINDOW_UPDATE,
    flags = yamux.FLAG.SYN,
    stream_id = 4,
    length = yamux.INITIAL_STREAM_WINDOW,
  }))

  local inbound_conn = new_scripted_conn(inbound_writer:writes())
  local server = yamux.new_session(inbound_conn, { is_client = true, max_accept_backlog = 1 })
  local _, p1_err = server:process_one()
  if p1_err then
    return nil, p1_err
  end
  local _, p2_err = server:process_one()
  if p2_err then
    return nil, p2_err
  end
  if server:accept_stream_now() == nil then
    return nil, "expected first inbound stream accepted"
  end
  if server:accept_stream_now() ~= nil then
    return nil, "expected second inbound stream to be reset due to backlog"
  end

  return true
end

return {
  name = "yamux frame codec",
  run = run,
}
