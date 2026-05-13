--- Identify protocol service.
-- Registers `/ipfs/id/*` handlers and provides request helpers.
---@class Libp2pIdentifyConfig
---@field include_push? boolean Register `/ipfs/id/push/1.0.0`. Default: true.
---@field run_on_connection_open? boolean Run identify after peer connection open. Default: true.
---@field timeout? number Identify request timeout.
---@field io_timeout? number Identify stream IO timeout.
---@field poll_interval? number Scheduler poll interval while waiting.

local identify = require("lua_libp2p.protocol_identify.protocol")

local M = {}
M.provides = { "identify" }
M.requires = {}

--- Construct identify service instance.
-- `opts.include_push` (`boolean`, default `true`) registers `/ipfs/id/push/1.0.0`.
-- Additional options are forwarded to `identify.enable_run_on_connection_open`.
--- host table Host instance.
--- opts? table
--- table service
function M.new(host, opts)
  local options = opts or {}
  local svc = {}

  function svc:start()
    local ok, err = host:handle(identify.ID, function(stream, ctx)
      return host:_handle_identify(stream, ctx)
    end, { run_on_limited_connection = true })
    if not ok then
      return nil, err
    end

    local hook_ok, hook_err = identify.enable_run_on_connection_open(host, options)
    if not hook_ok then
      return nil, hook_err
    end

    if options.include_push ~= false then
      ok, err = host:handle(identify.PUSH_ID, function(stream, ctx)
        return host:_handle_identify(stream, ctx)
      end, { run_on_limited_connection = true })
      if not ok then
        return nil, err
      end
    end
    return true
  end

  function svc:identify(target, call_opts)
    local request_opts = call_opts or {}
    if request_opts.ctx then
      return host:_request_identify(target, request_opts)
    end
    local task, task_err = host:spawn_task("services.identify.identify", function(ctx)
      local options_copy = {}
      for k, v in pairs(request_opts) do
        options_copy[k] = v
      end
      options_copy.ctx = options_copy.ctx or ctx
      return host:_request_identify(target, options_copy)
    end, { service = "identify" })
    if not task then
      return nil, task_err
    end
    return host:run_until_task(task, {
      timeout = request_opts.timeout,
      poll_interval = request_opts.poll_interval,
    })
  end

  return svc
end

return M
