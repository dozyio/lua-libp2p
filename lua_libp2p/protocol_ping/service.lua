local ping = require("lua_libp2p.protocol_ping.protocol")

local M = {}
M.provides = { "ping" }
M.requires = {}

function M.new(host)
  local svc = {}

  function svc:start()
    return host:handle(ping.ID, function(stream)
      return ping.handle(stream)
    end, { run_on_limited_connection = true })
  end

  function svc:ping(target, opts)
    local options = opts or {}
    local function run(call_opts)
      local stream, selected, conn, state_or_err = host:new_stream(target, { ping.ID }, call_opts)
      if not stream then
        return nil, state_or_err
      end
      local result, ping_err = ping.ping_once(stream)
      if type(stream.close) == "function" then
        pcall(function() stream:close() end)
      end
      if not result then
        return nil, ping_err
      end
      return {
        protocol = selected,
        rtt_seconds = result.rtt_seconds,
        sent = result.sent,
        received = result.received,
        connection = conn,
        state = state_or_err,
      }
    end

    if options.ctx then
      return run(options)
    end
    local task, task_err = host:spawn_task("services.ping.ping", function(ctx)
      local call_opts = {}
      for k, v in pairs(options) do
        call_opts[k] = v
      end
      call_opts.ctx = call_opts.ctx or ctx
      return run(call_opts)
    end, { service = "ping" })
    if not task then
      return nil, task_err
    end
    return host:run_until_task(task, {
      timeout = options.timeout,
      poll_interval = options.poll_interval,
    })
  end

  return svc
end

return M
