local error_mod = require("lua_libp2p.error")
local yamux = require("lua_libp2p.muxer.yamux")

local M = {}

M.yamux = yamux


local registry = {
  [yamux.PROTOCOL_ID] = {
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

function M.protocol_ids()
  local out = {}
  for protocol_id in pairs(registry) do
    out[#out + 1] = protocol_id
  end
  table.sort(out)
  return out
end

function M.get(protocol_id)
  return registry[protocol_id]
end

function M.new_session(protocol_id, raw_conn, opts)
  local impl = registry[protocol_id]
  if not impl then
    return nil, error_mod.new("unsupported", "muxer protocol not supported", { protocol = protocol_id })
  end
  return impl.new_session(raw_conn, opts)
end

return M
