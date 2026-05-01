local perf = require("lua_libp2p.protocol_perf.protocol")

local M = {}

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
