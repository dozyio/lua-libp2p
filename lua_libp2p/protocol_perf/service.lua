--- Perf protocol service.
-- @module lua_libp2p.protocol_perf.service
local perf = require("lua_libp2p.protocol_perf.protocol")

local M = {}
M.provides = { "perf" }
M.requires = {}

--- Construct perf service instance.
-- `opts.write_block_size` (`number`) sets chunk size for replies.
-- `opts.yield_every_bytes` (`number`) controls cooperative yield cadence.
-- @tparam table host Host instance.
-- @tparam[opt] table opts
-- @treturn table service
function M.new(host, opts)
  local options = opts or {}
  local svc = {}

  function svc:start()
    return host:handle(perf.ID, function(stream)
      return perf.handle(stream, {
        write_block_size = options.write_block_size,
        yield_every_bytes = options.yield_every_bytes,
      })
    end)
  end

  return svc
end

return M
