local error_mod = require("lua_libp2p.error")
local mss = require("lua_libp2p.multistream_select.protocol")

local M = {}

local Connection = {}
Connection.__index = Connection

function Connection:new(raw_conn, opts)
  local options = opts or {}
  return setmetatable({
    _raw_conn = raw_conn,
    _session = options.session,
    _direct_consumed = false,
  }, self)
end

function Connection:raw()
  return self._raw_conn
end

function Connection:session()
  return self._session
end

function Connection:has_waiters()
  return self._session and type(self._session.has_waiters) == "function" and self._session:has_waiters() or false
end

function Connection:socket()
  if self._raw_conn and self._raw_conn.socket then
    return self._raw_conn:socket()
  end
  return nil
end

function Connection:watch_luv_readable(on_readable)
  if self._raw_conn and type(self._raw_conn.watch_luv_readable) == "function" then
    return self._raw_conn:watch_luv_readable(on_readable)
  end
  return nil, error_mod.new("unsupported", "raw connection does not support luv readable watches")
end

function Connection:watch_luv_write(on_write)
  if self._raw_conn and type(self._raw_conn.watch_luv_write) == "function" then
    return self._raw_conn:watch_luv_write(on_write)
  end
  return nil, error_mod.new("unsupported", "raw connection does not support luv write watches")
end

function Connection:set_context(ctx)
  if self._raw_conn and type(self._raw_conn.set_context) == "function" then
    return self._raw_conn:set_context(ctx)
  end
  return true
end

function Connection:process_one()
  return self:pump_once()
end

function Connection:pump_once()
  if not self._session or type(self._session.process_one) ~= "function" then
    return nil
  end
  return self._session:process_one()
end

function Connection:pump_ready(max_frames)
  if not self._session then
    return 0
  end
  if type(self._session.pump_ready) == "function" then
    return self._session:pump_ready(max_frames)
  end
  if type(self._session.process_one) == "function" then
    local frame, err = self._session:process_one()
    if not frame then
      return 0, err
    end
    return 1
  end
  return 0
end

function Connection:new_stream_raw()
  if self._session then
    if type(self._session.open_stream) ~= "function" then
      return nil, error_mod.new("unsupported", "connection session cannot open streams")
    end
    return self._session:open_stream()
  end
  if self._direct_consumed then
    return nil, error_mod.new("state", "raw connection already consumed as stream")
  end
  self._direct_consumed = true
  return self._raw_conn
end

function Connection:accept_stream_raw()
  if self._session then
    if type(self._session.accept_stream_now) ~= "function" then
      return nil, error_mod.new("unsupported", "connection session cannot accept streams")
    end
    return self._session:accept_stream_now()
  end
  if self._direct_consumed then
    return nil, error_mod.new("state", "raw connection already consumed as stream")
  end
  self._direct_consumed = true
  return self._raw_conn
end

function Connection:new_stream(protocols)
  local stream, stream_err = self:new_stream_raw()
  if not stream then
    return nil, nil, stream_err
  end
  if protocols == nil then
    return stream
  end
  local selected, select_err = mss.select(stream, protocols)
  if not selected then
    return nil, nil, select_err
  end
  return stream, selected
end

function Connection:accept_stream(router)
  local stream, stream_err = self:accept_stream_raw()
  if not stream then
    return nil, nil, nil, stream_err
  end
  if not router then
    return stream
  end
  local protocol_id, handler, options, neg_err = router:negotiate(stream)
  if not protocol_id then
    return nil, nil, nil, neg_err
  end
  return stream, protocol_id, handler, options
end

function Connection:close()
  if self._session and type(self._session.close) == "function" then
    return self._session:close()
  end
  return self._raw_conn:close()
end

function M.from_raw(raw_conn, opts)
  return Connection:new(raw_conn, opts)
end

function M.from_session(raw_conn, session)
  return Connection:new(raw_conn, {
    session = session,
  })
end

return M
