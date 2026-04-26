local error_mod = require("lua_libp2p.error")

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
    return nil, error_mod.new("input", "yamux payload must be bytes")
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
  }, self)
end

function Stream:read(length)
  if self.reset then
    return nil, error_mod.new("closed", "yamux stream is reset")
  end
  if type(length) ~= "number" or length <= 0 then
    return nil, error_mod.new("input", "yamux stream read length must be positive")
  end

  while #self.recv_buf < length do
    if #self.recvq > 0 then
      self.recv_buf = self.recv_buf .. table.remove(self.recvq, 1)
    else
      if self.remote_closed then
        return nil, error_mod.new("closed", "yamux stream closed during read")
      end
      if self.session._pump_error then
        return nil, self.session._pump_error
      end
      if can_yield() then
        self.session._has_waiters = true
        coroutine.yield({ kind = "yamux_stream_wait", session = self.session, stream_id = self.id })
        goto continue_read_wait
      end
      local _, err = self.session:process_one()
      if err then
        return nil, err
      end
      if type(coroutine.isyieldable) == "function" and coroutine.isyieldable() then
        coroutine.yield({ kind = "yamux_stream_wait", session = self.session, stream_id = self.id })
      end
      if self.reset then
        return nil, error_mod.new("closed", "yamux stream reset during read")
      end
      ::continue_read_wait::
    end
  end

  local out = self.recv_buf:sub(1, length)
  self.recv_buf = self.recv_buf:sub(length + 1)
  return out
end

function Stream:write(payload)
  if self.reset then
    return nil, error_mod.new("closed", "yamux stream is reset")
  end
  if self.local_closed then
    return nil, error_mod.new("closed", "yamux stream is closed for writing")
  end
  if type(payload) ~= "string" then
    return nil, error_mod.new("input", "yamux stream payload must be bytes")
  end

  local offset = 1
  while offset <= #payload do
    while self.send_window <= 0 do
      if self.session._pump_error then
        return nil, self.session._pump_error
      end
      if can_yield() then
        self.session._has_waiters = true
        coroutine.yield({ kind = "yamux_window_wait", session = self.session, stream_id = self.id })
        goto continue_window_wait
      end
      local _, proc_err = self.session:process_one()
      if proc_err then
        return nil, proc_err
      end
      if self.reset then
        return nil, error_mod.new("closed", "yamux stream reset during write")
      end
      ::continue_window_wait::
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

function Session:new(conn, opts)
  local options = opts or {}
  local is_client = not not options.is_client
  return setmetatable({
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
  }, self)
end

function Session:has_waiters()
  return self._has_waiters == true
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
    return nil, error_mod.new("protocol", "yamux frame for unknown stream without SYN", { stream_id = stream_id })
  end
  if self:_is_local_stream_id(stream_id) then
    return nil, error_mod.new("protocol", "yamux inbound stream id has wrong parity", { stream_id = stream_id })
  end

  stream = Stream:new(self, stream_id)
  stream.acked = true

  if #self.pending_accept >= self.max_accept_backlog then
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
    return nil, error_mod.new("backlog", "yamux accept backlog exceeded")
  end

  self.streams[stream_id] = stream
  self.pending_accept[#self.pending_accept + 1] = stream

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
    return nil, error_mod.new("backlog", "yamux ack backlog exceeded")
  end

  local stream_id = self.next_stream_id
  self.next_stream_id = self.next_stream_id + 2

  local stream = Stream:new(self, stream_id)
  self.streams[stream_id] = stream
  self.inflight[stream_id] = true

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
    return nil, err
  end

  return stream
end

function Session:accept_stream_now()
  if #self.pending_accept == 0 then
    return nil
  end
  return table.remove(self.pending_accept, 1)
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
    stream.acked = true
    self.inflight[frame.stream_id] = nil
  end
  if has_flag(frame.flags, M.FLAG.RST) then
    stream.reset = true
    stream.local_closed = true
    stream.remote_closed = true
    self.inflight[frame.stream_id] = nil
  end
  if has_flag(frame.flags, M.FLAG.FIN) then
    stream.remote_closed = true
  end

  if frame.type == M.TYPE.WINDOW_UPDATE then
    stream.send_window = stream.send_window + frame.length
  elseif frame.type == M.TYPE.DATA then
    if #frame.payload ~= frame.length then
      return nil, error_mod.new("protocol", "yamux data frame length mismatch")
    end
    if frame.length > 0 then
      stream.recvq[#stream.recvq + 1] = frame.payload
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
    return nil, error_mod.new("protocol", "yamux unknown frame type", { type = frame.type })
  end

  return frame
end

function Session:process_one()
  if self._pump_error then
    return nil, self._pump_error
  end
  self._has_waiters = false
  if self._processing then
    if type(coroutine.isyieldable) == "function" and coroutine.isyieldable() then
      while self._processing do
        coroutine.yield({ kind = "yamux_read_pump", session = self })
      end
      return true
    end
    return nil, error_mod.new("state", "yamux read pump is already active")
  end

  self._processing = true
  local result = { pcall(function()
    return self:_process_one_unlocked()
  end) }
  self._processing = false

  if not result[1] then
    self._pump_error = error_mod.new("protocol", "yamux read pump panicked", { cause = result[2] })
    return nil, self._pump_error
  end
  if result[2] == nil and result[3] and not (error_mod.is_error(result[3]) and result[3].kind == "timeout") then
    self._pump_error = result[3]
  end
  return result[2], result[3]
end

function M.new_session(conn, opts)
  return Session:new(conn, opts)
end

return M
