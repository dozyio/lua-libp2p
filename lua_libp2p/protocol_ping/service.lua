--- Ping protocol service.
---@class Libp2pPingConfig

local ping = require("lua_libp2p.protocol_ping.protocol")
local log = require("lua_libp2p.log").subsystem("ping")

local M = {}
M.provides = { "ping" }
M.requires = {}

--- Construct ping service instance.
function M.new(host)
  local svc = {}

  function svc:start()
    return host:handle(ping.ID, function(stream, ctx)
      local fields = {
        peer_id = ctx and ctx.state and ctx.state.remote_peer_id or nil,
        connection_id = ctx and ctx.state and ctx.state.connection_id or nil,
        direction = ctx and ctx.state and ctx.state.direction or nil,
      }
      log.debug("ping handler opened", fields)
      local ok, err = ping.handle(stream)
      if not ok then
        log.debug("ping handler failed", {
          peer_id = fields.peer_id,
          connection_id = fields.connection_id,
          direction = fields.direction,
          cause = tostring(err),
        })
        return nil, err
      end
      log.debug("ping handler closed", fields)
      return true
    end, { run_on_limited_connection = true })
  end

  function svc:ping(target, opts)
    local options = opts or {}
    local function run(call_opts)
      log.debug("ping outbound begin", {
        target = type(target) == "table" and (target.peer_id or target.addr or target.multiaddr) or target,
      })
      local stream, selected, conn, state_or_err = host:new_stream(target, { ping.ID }, call_opts)
      if not stream then
        log.debug("ping outbound stream failed", {
          target = type(target) == "table" and (target.peer_id or target.addr or target.multiaddr) or target,
          cause = tostring(state_or_err),
        })
        return nil, state_or_err
      end
      log.debug("ping outbound stream opened", {
        peer_id = state_or_err and state_or_err.remote_peer_id or nil,
        connection_id = state_or_err and state_or_err.connection_id or nil,
        direction = state_or_err and state_or_err.direction or nil,
        protocol = selected,
      })
      local result, ping_err = ping.ping_once(stream)
      if type(stream.close) == "function" then
        pcall(function()
          stream:close()
        end)
      end
      if not result then
        log.debug("ping outbound failed", {
          peer_id = state_or_err and state_or_err.remote_peer_id or nil,
          connection_id = state_or_err and state_or_err.connection_id or nil,
          direction = state_or_err and state_or_err.direction or nil,
          protocol = selected,
          cause = tostring(ping_err),
        })
        return nil, ping_err
      end
      log.debug("ping outbound completed", {
        peer_id = state_or_err and state_or_err.remote_peer_id or nil,
        connection_id = state_or_err and state_or_err.connection_id or nil,
        direction = state_or_err and state_or_err.direction or nil,
        protocol = selected,
        rtt = result.rtt_seconds,
      })
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
