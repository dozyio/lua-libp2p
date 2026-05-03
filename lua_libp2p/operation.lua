--- Operation wrapper for sync/async result handling.
-- @module lua_libp2p.operation
local error_mod = require("lua_libp2p.error")

local Operation = {}
Operation.__index = Operation

local function nils_then_error(count, err)
  local out = {}
  for i = 1, math.max(count or 1, 1) do
    out[i] = nil
  end
  out[math.max(count or 1, 1) + 1] = err
  return table.unpack(out, 1, math.max(count or 1, 1) + 1)
end

--- Create an operation wrapper around a task.
-- `opts.result_count` controls how many nil slots are returned before an error.
-- @tparam table host Host instance.
-- @tparam table task Task handle.
-- @tparam[opt] table opts
-- @treturn table|nil operation
-- @treturn[opt] table err
function Operation.new(host, task, opts)
  if type(host) ~= "table" or type(task) ~= "table" then
    return nil, error_mod.new("input", "operation requires host and task")
  end
  local options = opts or {}
  return setmetatable({
    _host = host,
    _task = task,
    _result_count = options.result_count or 1,
  }, Operation)
end

function Operation:id()
  return self._task and self._task.id or nil
end

function Operation:task()
  return self._task
end

function Operation:status()
  return self._task and self._task.status or "unknown"
end

function Operation:cancel()
  if not (self._host and self._task and type(self._host.cancel_task) == "function") then
    return nil, error_mod.new("state", "operation cannot be cancelled")
  end
  return self._host:cancel_task(self._task.id)
end

--- Wait for operation completion and return task results.
-- `opts` supports `ctx`, `timeout`, and `poll_interval`.
-- @tparam[opt] table opts
-- @return Task result values, plus trailing error slot.
function Operation:result(opts)
  local options = opts or {}
  local task = self._task
  if not task then
    return nil, error_mod.new("state", "operation has no task")
  end

  if not (options.ctx and type(options.ctx.await_task) == "function") then
    if not (self._host and type(self._host.run_until_task) == "function") then
      return nil, error_mod.new("state", "operation requires host run_until_task")
    end
  end
  local _, wait_err
  if options.ctx and type(options.ctx.await_task) == "function" then
    _, wait_err = options.ctx:await_task(task)
  else
    _, wait_err = self._host:run_until_task(task, options)
  end
  if task.status ~= "completed" then
    return nils_then_error(self._result_count, wait_err or task.error)
  end
  local out = {}
  local results = task.results or { n = 1, task.result }
  for i = 1, results.n or #results do
    out[i] = results[i]
  end
  out[(results.n or #results) + 1] = nil
  return table.unpack(out, 1, (results.n or #results) + 1)
end

local M = {}

--- Module constructor alias for @{Operation.new}.
-- `opts.result_count` behaves the same as @{Operation.new}.
function M.new(host, task, opts)
  return Operation.new(host, task, opts)
end

M.Operation = Operation

return M
