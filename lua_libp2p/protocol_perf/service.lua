--- Perf protocol service.
---@class Libp2pPerfConfig
---@field write_block_size? integer Chunk size for replies.
---@field yield_every_bytes? integer Cooperative yield cadence.

local perf = require("lua_libp2p.protocol_perf.protocol")
local log = require("lua_libp2p.log").subsystem("perf")

local M = {}
M.provides = { "perf" }
M.requires = {}

--- Construct perf service instance.
-- `opts.write_block_size` (`number`) sets chunk size for replies.
-- `opts.yield_every_bytes` (`number`) controls cooperative yield cadence.
--- host table Host instance.
--- opts? table
--- table service
function M.new(host, opts)
  local options = opts or {}
  local svc = {}

  function svc:start()
    return host:handle(perf.ID, function(stream, ctx)
      local fields = {
        peer_id = ctx and ctx.state and ctx.state.remote_peer_id or nil,
        connection_id = ctx and ctx.state and ctx.state.connection_id or nil,
        direction = ctx and ctx.state and ctx.state.direction or nil,
      }
      log.debug("perf handler opened", fields)
      local result, err = perf.handle(stream, {
        write_block_size = options.write_block_size,
        yield_every_bytes = options.yield_every_bytes,
      })
      if not result then
        log.debug("perf handler failed", {
          peer_id = fields.peer_id,
          connection_id = fields.connection_id,
          direction = fields.direction,
          cause = tostring(err),
        })
        return nil, err
      end
      log.debug("perf handler completed", {
        peer_id = fields.peer_id,
        connection_id = fields.connection_id,
        direction = fields.direction,
        uploaded_bytes = result.uploaded_bytes,
        download_bytes = result.download_bytes,
      })
      return result
    end)
  end

  return svc
end

return M
