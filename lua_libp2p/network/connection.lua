local error_mod = require("lua_libp2p.error")
local mss = require("lua_libp2p.protocol.mss")
local yamux = require("lua_libp2p.muxer.yamux")

local M = {}

local Connection = {}
Connection.__index = Connection

function Connection:new(raw_conn, opts)
  local options = opts or {}
  return setmetatable({
    _raw_conn = raw_conn,
    _muxer = options.muxer_session,
    _direct_consumed = false,
  }, self)
end

function Connection:raw()
  return self._raw_conn
end

function Connection:muxer_session()
  return self._muxer
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

function Connection:process_one()
  if not self._muxer then
    return nil
  end
  return self._muxer:process_one()
end

function Connection:new_stream_raw()
  if self._muxer then
    return self._muxer:open_stream()
  end
  if self._direct_consumed then
    return nil, error_mod.new("state", "raw connection already consumed as stream")
  end
  self._direct_consumed = true
  return self._raw_conn
end

function Connection:accept_stream_raw()
  if self._muxer then
    return self._muxer:accept_stream_now()
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
  local protocol_id, handler, neg_err = router:negotiate(stream)
  if not protocol_id then
    return nil, nil, nil, neg_err
  end
  return stream, protocol_id, handler
end

function Connection:close()
  return self._raw_conn:close()
end

function M.from_raw(raw_conn, opts)
  return Connection:new(raw_conn, opts)
end

function M.with_yamux(raw_conn, opts)
  local options = opts or {}
  local muxer = yamux.new_session(raw_conn, {
    is_client = not not options.is_client,
    initial_stream_window = options.initial_stream_window,
    max_ack_backlog = options.max_ack_backlog,
    max_accept_backlog = options.max_accept_backlog,
  })

  return Connection:new(raw_conn, {
    muxer_session = muxer,
  })
end

return M
