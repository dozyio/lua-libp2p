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

  local watcher_conn = new_scripted_conn("")
  local readable_registered = false
  local write_registered = false
  function watcher_conn:watch_luv_readable(on_readable)
    readable_registered = type(on_readable) == "function"
    return function()
      return true
    end
  end
  function watcher_conn:watch_luv_write(on_write)
    write_registered = type(on_write) == "function"
    return function()
      return true
    end
  end
  local watcher_session = yamux.new_session(watcher_conn, { is_client = true })
  if not watcher_session:watch_luv_readable(function() end) or not readable_registered then
    return nil, "yamux session should delegate readable watcher"
  end
  if not watcher_session:watch_luv_write(function() end) or not write_registered then
    return nil, "yamux session should delegate write watcher"
  end

  local stream = assert(watcher_session:open_stream())
  local co = coroutine.create(function()
    return stream:read(1)
  end)
  local ok_resume, yielded = coroutine.resume(co)
  if not ok_resume then
    return nil, yielded
  end
  if type(yielded) ~= "table" or yielded.type ~= "read" or yielded.connection ~= stream then
    return nil, "yamux stream wait should yield scheduler-compatible read reason"
  end
  stream.send_window = 0
  local write_co = coroutine.create(function()
    return stream:write("x")
  end)
  local ok_write_resume, write_yielded = coroutine.resume(write_co)
  if not ok_write_resume then
    return nil, write_yielded
  end
  if type(write_yielded) ~= "table" or write_yielded.type ~= "write" or write_yielded.connection ~= stream then
    return nil, "yamux window wait should yield scheduler-compatible write reason"
  end
  if not watcher_session:has_stream_waiters() or not watcher_session:has_waiters() then
    return nil, "yamux session should track stream-local waiters"
  end

  local notify_writer = new_scripted_conn("")
  assert(yamux.write_frame(notify_writer, {
    type = yamux.TYPE.WINDOW_UPDATE,
    flags = yamux.FLAG.SYN,
    stream_id = 2,
    length = yamux.INITIAL_STREAM_WINDOW,
  }))
  assert(yamux.write_frame(notify_writer, {
    type = yamux.TYPE.DATA,
    flags = 0,
    stream_id = 2,
    payload = "x",
  }))
  assert(yamux.write_frame(notify_writer, {
    type = yamux.TYPE.WINDOW_UPDATE,
    flags = 0,
    stream_id = 2,
    length = 1,
  }))
  local notify_session = yamux.new_session(new_scripted_conn(notify_writer:writes()), { is_client = true })
  assert(notify_session:process_one())
  local notify_stream = assert(notify_session:accept_stream_now())
  notify_stream.send_window = 0
  local read_woken = 0
  local write_woken = 0
  assert(notify_stream:watch_luv_readable(function()
    read_woken = read_woken + 1
  end))
  assert(notify_stream:watch_luv_write(function()
    write_woken = write_woken + 1
  end))
  assert(notify_session:process_one())
  if read_woken == 0 then
    return nil, "yamux data frame should wake stream readable watchers"
  end
  if notify_session._stream_read_waiters[notify_stream.id] ~= nil then
    return nil, "yamux data wake should clear read waiter tracking"
  end
  assert(notify_session:process_one())
  if write_woken == 0 then
    return nil, "yamux window update should wake stream write watchers"
  end
  if notify_session._stream_write_waiters[notify_stream.id] ~= nil then
    return nil, "yamux window wake should clear write waiter tracking"
  end

  local pump_writer = new_scripted_conn("")
  assert(yamux.write_frame(pump_writer, {
    type = yamux.TYPE.WINDOW_UPDATE,
    flags = yamux.FLAG.SYN,
    stream_id = 2,
    length = yamux.INITIAL_STREAM_WINDOW,
  }))
  assert(yamux.write_frame(pump_writer, {
    type = yamux.TYPE.DATA,
    flags = 0,
    stream_id = 2,
    payload = "a",
  }))
  local pump_session = yamux.new_session(new_scripted_conn(pump_writer:writes()), { is_client = true })
  local pumped, pump_err = pump_session:pump_ready(2)
  if not pumped then
    return nil, pump_err
  end
  if pumped ~= 2 then
    return nil, "yamux pump_ready should process available frames"
  end
  local pump_stream = pump_session:accept_stream_now()
  if not pump_stream or pump_stream:read_now() ~= "a" then
    return nil, "yamux pump_ready should deliver stream data"
  end

  return true
end

return {
  name = "yamux frame codec",
  run = run,
}
