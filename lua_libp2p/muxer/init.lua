--- Stream muxer registry.
-- @module lua_libp2p.muxer
---@class Libp2pMuxerConfig
---@field yamux? boolean Enable yamux (`/yamux/1.0.0`). Default: true.
local error_mod = require("lua_libp2p.error")
local yamux = require("lua_libp2p.muxer.yamux")

local M = {}

M.yamux = yamux
M.YAMUX = yamux.PROTOCOL_ID

local order = { "yamux" }

local registry = {
  yamux = {
    protocol_id = yamux.PROTOCOL_ID,
    new_session = function(raw_conn, opts)
      local options = opts or {}
      return yamux.new_session(raw_conn, {
        is_client = not not options.is_client,
        initial_stream_window = options.initial_stream_window,
        max_ack_backlog = options.max_ack_backlog,
        max_accept_backlog = options.max_accept_backlog,
      })
    end,
  },
}

local by_protocol = {}
for name, entry in pairs(registry) do
  by_protocol[entry.protocol_id] = entry
  entry.name = name
end

function M.protocol_ids()
  local out = {}
  for _, name in ipairs(order) do
    out[#out + 1] = registry[name].protocol_id
  end
  return out
end

function M.normalize_protocols(spec)
  if spec == nil then
    return M.protocol_ids()
  end
  if type(spec) ~= "table" then
    return nil, error_mod.new("input", "muxers must be a map")
  end

  if #spec > 0 then
    return nil, error_mod.new("input", "muxer protocol ID lists are not supported; use a map")
  end

  local out = {}
  for _, name in ipairs(order) do
    local muxer_spec = spec[name]
    if muxer_spec ~= nil and muxer_spec ~= false then
      out[#out + 1] = registry[name].protocol_id
    end
  end
  for name in pairs(spec) do
    if registry[name] == nil then
      return nil,
        error_mod.new("input", "unknown muxer", {
          muxer = name,
        })
    end
  end
  if #out == 0 then
    return nil, error_mod.new("input", "muxers must enable at least one muxer")
  end
  return out
end

function M.get(protocol_id)
  return by_protocol[protocol_id]
end

--- Create a muxer session for a negotiated protocol.
-- `opts` supports `is_client`, `initial_stream_window`, `max_ack_backlog`,
-- and `max_accept_backlog`.
-- @tparam string protocol_id
-- @tparam table raw_conn
-- @tparam[opt] table opts
-- @treturn table|nil session
-- @treturn[opt] table err
function M.new_session(protocol_id, raw_conn, opts)
  local impl = by_protocol[protocol_id]
  if not impl then
    return nil, error_mod.new("unsupported", "muxer protocol not supported", { protocol = protocol_id })
  end
  return impl.new_session(raw_conn, opts)
end

return M
