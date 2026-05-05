--- Yamux stream multiplexer implementation.
-- @module lua_libp2p.muxer.yamux
local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log").subsystem("yamux")

local M = {}

M.PROTOCOL_ID = "/yamux/1.0.0"
M.VERSION = 0
M.HEADER_LENGTH = 12
M.INITIAL_STREAM_WINDOW = 256 * 1024

M.TYPE = {
  DATA = 0x0,
  WINDOW_UPDATE = 0x1,
  PING = 0x2,
  GO_AWAY = 0x3,
}

M.FLAG = {
  SYN = 0x1,
  ACK = 0x2,
  FIN = 0x4,
  RST = 0x8,
}

M.GO_AWAY = {
  NORMAL = 0x0,
  PROTOCOL_ERROR = 0x1,
  INTERNAL_ERROR = 0x2,
}

local function has_flag(flags, flag)
  return (flags & flag) == flag
end

local function stream_error(stream_id, kind, message, fields)
  local context = fields or {}
  context.stream_id = stream_id
  return error_mod.new(kind, message, context)
end

local function error_stream_id(err)
  if error_mod.is_error(err) and err.context then
    return err.context.stream_id
  end
  return nil
end

local function read_exact(conn, n)
  if n == 0 then
    return ""
  end

  local out = {}
  local remaining = n
  while remaining > 0 do
    local chunk, err = conn:read(remaining)
    if not chunk then
      return nil, err or error_mod.new("io", "unexpected EOF")
    end
    if #chunk == 0 then
      return nil, error_mod.new("io", "unexpected empty read")
    end
    out[#out + 1] = chunk
    remaining = remaining - #chunk
  end

  return table.concat(out)
end

local function u16be(n)
  return string.char((n >> 8) & 0xFF, n & 0xFF)
end

local function u32be(n)
  return string.char((n >> 24) & 0xFF, (n >> 16) & 0xFF, (n >> 8) & 0xFF, n & 0xFF)
end

local function parse_u16be(s, offset)
  local a, b = s:byte(offset, offset + 1)
  return a * 256 + b
end

local function parse_u32be(s, offset)
  local a, b, c, d = s:byte(offset, offset + 3)
  return (((a * 256 + b) * 256 + c) * 256 + d)
end

function M.encode_header(header)
  local version = header.version
  if version == nil then
    version = M.VERSION
  end
  local frame_type = header.type
  local flags = header.flags or 0
  local stream_id = header.stream_id or 0
  local length = header.length or 0

  if version ~= M.VERSION then
    return nil, error_mod.new("input", "yamux unsupported version", { version = version })
  end
  if type(frame_type) ~= "number" or frame_type < 0 or frame_type > 0xFF then
    return nil, error_mod.new("input", "yamux invalid frame type")
  end

  return string.char(version) .. string.char(frame_type) .. u16be(flags) .. u32be(stream_id) .. u32be(length)
end

function M.decode_header(bytes)
  if type(bytes) ~= "string" or #bytes ~= M.HEADER_LENGTH then
    return nil, error_mod.new("decode", "yamux header must be 12 bytes")
  end

  local version = bytes:byte(1)
  if version ~= M.VERSION then
    return nil, error_mod.new("decode", "yamux invalid version", { version = version })
  end

  return {
    version = version,
    type = bytes:byte(2),
    flags = parse_u16be(bytes, 3),
    stream_id = parse_u32be(bytes, 5),
    length = parse_u32be(bytes, 9),
  }
end

function M.write_frame(conn, frame)
  local payload = frame.payload or ""
  if type(payload) ~= "string" then
    return nil, stream_error(frame.stream_id, "input", "yamux payload must be bytes")
  end

  local header, header_err = M.encode_header({
    type = frame.type,
    flags = frame.flags or 0,
    stream_id = frame.stream_id or 0,
    length = frame.length or #payload,
  })
  if not header then
    return nil, header_err
  end

  local ok, err = conn:write(header .. payload)
  if not ok then
    return nil, err
  end
  return true
end

function M.read_frame(conn)
  local header_bytes, header_err = read_exact(conn, M.HEADER_LENGTH)
  if not header_bytes then
    return nil, header_err
  end

  local header, decode_err = M.decode_header(header_bytes)
  if not header then
    return nil, decode_err
  end

  local payload = ""
  if header.type == M.TYPE.DATA and header.length > 0 then
    payload, header_err = read_exact(conn, header.length)
    if not payload then
      return nil, header_err
    end
  end

  return {
    type = header.type,
    flags = header.flags,
    stream_id = header.stream_id,
    length = header.length,
    payload = payload,
  }
end

local Stream = {}
Stream.__index = Stream

local function can_yield()
  return type(coroutine.isyieldable) == "function" and coroutine.isyieldable()
end

function Stream:new(session, stream_id)
  return setmetatable({
    session = session,
    id = stream_id,
    send_window = session.initial_stream_window,
    recv_window = session.initial_stream_window,
    recvq = {},
    recv_buf = "",
    local_closed = false,
    remote_closed = false,
    reset = false,
    acked = false,
    _read_watch_callbacks = {},
    _write_watch_callbacks = {},
    _next_watch_id = 1,
  }, self)
end

function Stream:watch_luv_readable(on_readable)
  if type(on_readable) ~= "function" then
    return nil, error_mod.new("input", "on_readable callback is required")
  end
  local watch_id = self._next_watch_id
  self._next_watch_id = watch_id + 1
  self._read_watch_callbacks[watch_id] = on_readable
  if #self.recv_buf > 0 or #self.recvq > 0 or self.remote_closed or self.reset then
    on_readable()
  end
  return function()
    self._read_watch_callbacks[watch_id] = nil
    return true
  end
end

function Stream:watch_luv_write(on_write)
  if type(on_write) ~= "function" then
    return nil, error_mod.new("input", "on_write callback is required")
  end
  local watch_id = self._next_watch_id
  self._next_watch_id = watch_id + 1
  self._write_watch_callbacks[watch_id] = on_write
  if self.send_window > 0 or self.local_closed or self.reset then
    on_write()
  end
  return function()
    self._write_watch_callbacks[watch_id] = nil
    return true
  end
end

function Stream:_notify_readable()
  self.session._stream_read_waiters[self.id] = nil
  for _, cb in pairs(self._read_watch_callbacks) do
    cb()
  end
end

function Stream:_notify_writable()
  self.session._stream_write_waiters[self.id] = nil
  for _, cb in pairs(self._write_watch_callbacks) do
    cb()
  end
end

function Stream:_mark_read_waiter()
  self.session._stream_read_waiters[self.id] = true
  self.session._has_waiters = true
end

function Stream:_mark_write_waiter()
  self.session._stream_write_waiters[self.id] = true
  self.session._has_waiters = true
end

function Stream:read(length)
  if self.reset then
    return nil, stream_error(self.id, "closed", "yamux stream is reset")
  end
  if type(length) ~= "number" or length <= 0 then
    return nil, stream_error(self.id, "input", "yamux stream read length must be positive")
  end

  while #self.recv_buf < length do
      if #self.recvq > 0 then
        self.recv_buf = self.recv_buf .. table.remove(self.recvq, 1)
    else
      if self.remote_closed then
        if #self.recv_buf > 0 then
          local out = self.recv_buf
          self.recv_buf = ""
          return out
        end
        return nil, stream_error(self.id, "closed", "yamux stream closed during read")
      end
      if self.session._pump_error then
        return nil, self.session._pump_error
      end
      if can_yield() then
        self:_mark_read_waiter()
        coroutine.yield({ type = "read", connection = self, stream_id = self.id })
      else
        return nil, stream_error(self.id, "busy", "yamux stream read requires scheduler waiter context")
      end
    end
  end

  local out = self.recv_buf:sub(1, length)
  self.recv_buf = self.recv_buf:sub(length + 1)
  return out
end

function Stream:write(payload)
  if self.reset then
    return nil, stream_error(self.id, "closed", "yamux stream is reset")
  end
  if self.local_closed then
    return nil, stream_error(self.id, "closed", "yamux stream is closed for writing")
  end
  if type(payload) ~= "string" then
    return nil, stream_error(self.id, "input", "yamux stream payload must be bytes")
  end

  local offset = 1
  while offset <= #payload do
    while self.send_window <= 0 do
      if self.session._pump_error then
        return nil, self.session._pump_error
      end
      if can_yield() then
        self:_mark_write_waiter()
        coroutine.yield({ type = "write", connection = self, stream_id = self.id })
      else
        return nil, stream_error(self.id, "busy", "yamux stream write requires scheduler waiter context")
      end
    end

    local remaining = #payload - offset + 1
    local to_send = self.send_window
    if to_send > remaining then
      to_send = remaining
    end

    local chunk = payload:sub(offset, offset + to_send - 1)
    local ok, err = M.write_frame(self.session.conn, {
      type = M.TYPE.DATA,
      flags = 0,
      stream_id = self.id,
      payload = chunk,
    })
    if not ok then
      return nil, err
    end
    self.send_window = self.send_window - to_send
    offset = offset + to_send
  end

  return true
end

function Stream:close_write()
  if self.local_closed then
    return true
  end
  local ok, err = M.write_frame(self.session.conn, {
    type = M.TYPE.DATA,
    flags = M.FLAG.FIN,
    stream_id = self.id,
    length = 0,
    payload = "",
  })
  if not ok then
    return nil, err
  end
  self.local_closed = true
  log.debug("yamux stream closed for writing", {
    stream_id = self.id,
  })
  return true
end

function Stream:reset_now()
  local ok, err = M.write_frame(self.session.conn, {
    type = M.TYPE.WINDOW_UPDATE,
    flags = M.FLAG.RST,
    stream_id = self.id,
    length = 0,
    payload = "",
  })
  if not ok then
    return nil, err
  end
  self.local_closed = true
  self.remote_closed = true
  self.reset = true
  log.debug("yamux stream reset sent", {
    stream_id = self.id,
  })
  return true
end

function Stream:read_now()
  if #self.recv_buf > 0 then
    local data = self.recv_buf
    self.recv_buf = ""
    return data
  end
  if #self.recvq == 0 then
    return nil
  end
  local data = table.remove(self.recvq, 1)
  return data
end

function Stream:close()
  return self:close_write()
end

local Session = {}
Session.__index = Session

--- Create yamux session.
-- `opts.is_client` (`boolean`) picks odd/even stream id side.
-- `opts.initial_stream_window` (`number`) overrides default window.
-- `opts.max_ack_backlog` / `opts.max_accept_backlog` bound queues.
-- `opts.scheduler_driven` (`boolean`) forces scheduler waiter mode.
function Session:new(conn, opts)
  local options = opts or {}
  local is_client = not not options.is_client
  local scheduler_driven = false
  if options.scheduler_driven ~= nil then
    scheduler_driven = options.scheduler_driven == true
  elseif type(conn) == "table" then
    scheduler_driven = type(conn.watch_luv_readable) == "function" or type(conn.watch_luv_write) == "function"
  end
  local session = setmetatable({
    conn = conn,
    is_client = is_client,
    initial_stream_window = options.initial_stream_window or M.INITIAL_STREAM_WINDOW,
    next_stream_id = is_client and 1 or 2,
    streams = {},
    inflight = {},
    max_ack_backlog = options.max_ack_backlog or 256,
    max_accept_backlog = options.max_accept_backlog or 256,
    pending_accept = {},
    go_away = false,
    _processing = false,
    _pump_error = nil,
    _has_waiters = false,
    _stream_read_waiters = {},
    _stream_write_waiters = {},
    _scheduler_driven = scheduler_driven,
  }, self)
  log.debug("yamux session created", {
    role = is_client and "client" or "server",
    scheduler_driven = scheduler_driven,
    initial_stream_window = session.initial_stream_window,
    max_ack_backlog = session.max_ack_backlog,
    max_accept_backlog = session.max_accept_backlog,
  })
  return session
end

function Session:has_waiters()
  if self._has_waiters == true then
    return true
  end
  return next(self._stream_read_waiters) ~= nil or next(self._stream_write_waiters) ~= nil
end

function Session:has_stream_waiters()
  return next(self._stream_read_waiters) ~= nil or next(self._stream_write_waiters) ~= nil
end

function Session:_wake_all_stream_waiters()
  for _, stream in pairs(self.streams or {}) do
    stream:_notify_readable()
    stream:_notify_writable()
  end
end

function Session:watch_luv_readable(on_readable)
  if self.conn and type(self.conn.watch_luv_readable) == "function" then
    return self.conn:watch_luv_readable(on_readable)
  end
  return nil, error_mod.new("unsupported", "yamux session connection does not support luv readable watches")
end

function Session:watch_luv_write(on_write)
  if self.conn and type(self.conn.watch_luv_write) == "function" then
    return self.conn:watch_luv_write(on_write)
  end
  return nil, error_mod.new("unsupported", "yamux session connection does not support luv write watches")
end

function Session:_is_local_stream_id(stream_id)
  if self.is_client then
    return (stream_id % 2) == 1
  end
  return (stream_id % 2) == 0
end

function Session:_stream_for_inbound(stream_id, has_syn)
  local stream = self.streams[stream_id]
  if stream then
    return stream
  end
  if not has_syn then
    return nil, stream_error(stream_id, "protocol", "yamux frame for unknown stream without SYN")
  end
  if self:_is_local_stream_id(stream_id) then
    return nil, stream_error(stream_id, "protocol", "yamux inbound stream id has wrong parity")
  end

  stream = Stream:new(self, stream_id)
  stream.acked = true

  if #self.pending_accept >= self.max_accept_backlog then
    log.debug("yamux inbound stream rejected", {
      stream_id = stream_id,
      cause = "accept backlog exceeded",
    })
    local ok, err = M.write_frame(self.conn, {
      type = M.TYPE.WINDOW_UPDATE,
      flags = M.FLAG.RST,
      stream_id = stream_id,
      length = 0,
      payload = "",
    })
    if not ok then
      return nil, err
    end
    return nil, stream_error(stream_id, "backlog", "yamux accept backlog exceeded")
  end

  self.streams[stream_id] = stream
  self.pending_accept[#self.pending_accept + 1] = stream
  log.debug("yamux inbound stream opened", {
    stream_id = stream_id,
    pending_accept = #self.pending_accept,
  })

  local ok, err = M.write_frame(self.conn, {
    type = M.TYPE.WINDOW_UPDATE,
    flags = M.FLAG.ACK,
    stream_id = stream_id,
    length = self.initial_stream_window,
    payload = "",
  })
  if not ok then
    return nil, err
  end

  return stream
end

function Session:open_stream()
  local inflight_count = 0
  for _ in pairs(self.inflight) do
    inflight_count = inflight_count + 1
  end
  if inflight_count >= self.max_ack_backlog then
    return nil, stream_error(self.next_stream_id, "backlog", "yamux ack backlog exceeded")
  end

  local stream_id = self.next_stream_id
  self.next_stream_id = self.next_stream_id + 2

  local stream = Stream:new(self, stream_id)
  self.streams[stream_id] = stream
  self.inflight[stream_id] = true
  log.debug("yamux outbound stream opening", {
    stream_id = stream_id,
    inflight = inflight_count + 1,
  })

  local ok, err = M.write_frame(self.conn, {
    type = M.TYPE.WINDOW_UPDATE,
    flags = M.FLAG.SYN,
    stream_id = stream_id,
    length = self.initial_stream_window,
    payload = "",
  })
  if not ok then
    self.streams[stream_id] = nil
    self.inflight[stream_id] = nil
    log.debug("yamux outbound stream open failed", {
      stream_id = stream_id,
      cause = tostring(err),
    })
    return nil, err
  end

  return stream
end

function Session:accept_stream_now()
  if #self.pending_accept == 0 then
    return nil
  end
  local stream = table.remove(self.pending_accept, 1)
  log.debug("yamux inbound stream accepted", {
    stream_id = stream.id,
    pending_accept = #self.pending_accept,
  })
  return stream
end

function Session:_process_one_unlocked()
  local frame, frame_err = M.read_frame(self.conn)
  if not frame then
    return nil, frame_err
  end

  if frame.type == M.TYPE.PING then
    if has_flag(frame.flags, M.FLAG.SYN) then
      local ok, err = M.write_frame(self.conn, {
        type = M.TYPE.PING,
        flags = M.FLAG.ACK,
        stream_id = 0,
        length = frame.length,
      })
      if not ok then
        return nil, err
      end
    end
    return frame
  end

  if frame.type == M.TYPE.GO_AWAY then
    self.go_away = true
    log.debug("yamux go away received", {
      code = frame.length,
    })
    return frame
  end

  local stream, stream_err = self:_stream_for_inbound(frame.stream_id, has_flag(frame.flags, M.FLAG.SYN))
  if not stream then
    if error_mod.is_error(stream_err) and stream_err.kind == "backlog" then
      return frame
    end
    return nil, stream_err
  end

  if has_flag(frame.flags, M.FLAG.ACK) then
    if self.inflight[frame.stream_id] then
      log.debug("yamux outbound stream opened", {
        stream_id = frame.stream_id,
      })
    end
    stream.acked = true
    self.inflight[frame.stream_id] = nil
  end
  if has_flag(frame.flags, M.FLAG.RST) then
    stream.reset = true
    stream.local_closed = true
    stream.remote_closed = true
    self.inflight[frame.stream_id] = nil
    log.debug("yamux stream reset received", {
      stream_id = frame.stream_id,
    })
    stream:_notify_readable()
    stream:_notify_writable()
  end
  if has_flag(frame.flags, M.FLAG.FIN) then
    stream.remote_closed = true
    log.debug("yamux stream remote closed", {
      stream_id = frame.stream_id,
    })
    stream:_notify_readable()
  end

  if frame.type == M.TYPE.WINDOW_UPDATE then
    stream.send_window = stream.send_window + frame.length
    stream:_notify_writable()
  elseif frame.type == M.TYPE.DATA then
    if #frame.payload ~= frame.length then
      return nil, stream_error(frame.stream_id, "protocol", "yamux data frame length mismatch")
    end
    if frame.length > 0 then
      stream.recvq[#stream.recvq + 1] = frame.payload
      stream:_notify_readable()
      stream.recv_window = stream.recv_window - frame.length
      local ok, err = M.write_frame(self.conn, {
        type = M.TYPE.WINDOW_UPDATE,
        flags = 0,
        stream_id = frame.stream_id,
        length = frame.length,
        payload = "",
      })
      if not ok then
        return nil, err
      end
      stream.recv_window = stream.recv_window + frame.length
    end
  else
    return nil, stream_error(frame.stream_id, "protocol", "yamux unknown frame type", { type = frame.type })
  end

  return frame
end

function Session:process_one()
  if self._pump_error then
    return nil, self._pump_error
  end
  self._has_waiters = self:has_stream_waiters()
  if self._processing then
    if type(coroutine.isyieldable) == "function" and coroutine.isyieldable() then
      while self._processing do
        coroutine.yield({ type = "read", connection = self })
      end
      return true
    end
    self._has_waiters = true
    return nil, error_mod.new("busy", "yamux read pump is already active")
  end

  self._processing = true
  local result = { pcall(function()
    return self:_process_one_unlocked()
  end) }
  self._processing = false

  if not result[1] then
    self._pump_error = error_mod.new("protocol", "yamux read pump panicked", {
      cause = result[2],
      stream_id = error_stream_id(result[2]),
    })
    log.debug("yamux pump error set", {
      cause = tostring(result[2]),
      kind = "protocol",
      stream_id = error_stream_id(result[2]),
    })
    self:_wake_all_stream_waiters()
    return nil, self._pump_error
  end
  if result[2] == nil and result[3] and not (error_mod.is_error(result[3]) and result[3].kind == "timeout") then
    self._pump_error = result[3]
    log.debug("yamux pump error set", {
      cause = tostring(result[3]),
      kind = error_mod.is_error(result[3]) and result[3].kind or nil,
      stream_id = error_stream_id(result[3]),
    })
    self:_wake_all_stream_waiters()
  end
  self._has_waiters = self:has_stream_waiters()
  return result[2], result[3]
end

function Session:pump_ready(max_frames)
  local limit = max_frames or 64
  local processed = 0
  self._pump_owner_active = true
  while processed < limit do
    local frame, err = self:process_one()
    if not frame then
      if err and error_mod.is_error(err) and (err.kind == "timeout" or err.kind == "busy") then
        self._pump_owner_active = false
        return processed
      end
      self._pump_owner_active = false
      return nil, err
    end
    processed = processed + 1
  end
  self._pump_owner_active = false
  return processed
end

function Session:close()
  if not self._pump_error then
    self._pump_error = error_mod.new("closed", "yamux session closed")
    log.debug("yamux session close set pump error", {
      cause = tostring(self._pump_error),
    })
  end
  self:_wake_all_stream_waiters()
  if self.conn and type(self.conn.close) == "function" then
    return self.conn:close()
  end
  return true
end

--- Construct yamux session wrapper.
-- Forwards `opts.<field>` to @{Session:new}.
-- @tparam table conn
-- @tparam[opt] table opts
-- @treturn table session
function M.new_session(conn, opts)
  return Session:new(conn, opts)
end

return M
