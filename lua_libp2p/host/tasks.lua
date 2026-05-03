--- Host cooperative task scheduler internals.
-- @module lua_libp2p.host.tasks
local error_mod = require("lua_libp2p.error")

local M = {}

M.DEFAULT_TASK_RESUME_BUDGET = 128

local function now_seconds()
  local ok_socket, socket = pcall(require, "socket")
  if ok_socket and type(socket.gettime) == "function" then
    return socket.gettime()
  end
  return os.time()
end

local function pack_returns(...)
  return { n = select("#", ...), ... }
end

local function unpack_returns(values)
  if type(values) ~= "table" then
    return nil
  end
  return table.unpack(values, 1, values.n or #values)
end

local function map_count(values)
  local count = 0
  for _ in pairs(values or {}) do
    count = count + 1
  end
  return count
end

local function emit_task_event(host, name, payload)
  if type(host.emit) == "function" then
    return host:emit(name, payload)
  end
  return true
end

local function make_task_context(task)
  local ctx = {}
  function ctx:id()
    return task.id
  end
  function ctx:name()
    return task.name
  end
  function ctx:cancelled()
    return task.cancelled == true
  end
  function ctx:checkpoint()
    if task.cancelled then
      return nil, error_mod.new("cancelled", "task cancelled", { task_id = task.id, name = task.name })
    end
    return coroutine.yield({ type = "checkpoint" })
  end
  function ctx:sleep(seconds)
    if type(seconds) ~= "number" or seconds < 0 then
      return nil, error_mod.new("input", "sleep seconds must be a non-negative number")
    end
    if task.cancelled then
      return nil, error_mod.new("cancelled", "task cancelled", { task_id = task.id, name = task.name })
    end
    return coroutine.yield({ type = "sleep", until_at = now_seconds() + seconds })
  end
  function ctx:wait_read(connection)
    if not connection then
      return nil, error_mod.new("input", "wait_read requires connection")
    end
    if task.cancelled then
      return nil, error_mod.new("cancelled", "task cancelled", { task_id = task.id, name = task.name })
    end
    return coroutine.yield({ type = "read", connection = connection })
  end
  function ctx:wait_dial(connection)
    if not connection then
      return nil, error_mod.new("input", "wait_dial requires connection")
    end
    if task.cancelled then
      return nil, error_mod.new("cancelled", "task cancelled", { task_id = task.id, name = task.name })
    end
    return coroutine.yield({ type = "dial", connection = connection })
  end
  function ctx:wait_write(connection)
    if not connection then
      return nil, error_mod.new("input", "wait_write requires connection")
    end
    if task.cancelled then
      return nil, error_mod.new("cancelled", "task cancelled", { task_id = task.id, name = task.name })
    end
    return coroutine.yield({ type = "write", connection = connection })
  end
  function ctx:await_task(child_task)
    if type(child_task) ~= "table" then
      return nil, error_mod.new("input", "await_task requires task table")
    end
    if task.cancelled then
      return nil, error_mod.new("cancelled", "task cancelled", { task_id = task.id, name = task.name })
    end
    if child_task.status ~= "completed" and child_task.status ~= "failed" and child_task.status ~= "cancelled" then
      local ok, err = coroutine.yield({ type = "task_wait", tasks = { child_task } })
      if ok == nil and err then
        return nil, err
      end
    end
    if child_task.status ~= "completed" then
      return nil, child_task.error or error_mod.new("state", "task did not complete", {
        task_id = child_task.id,
        name = child_task.name,
        status = child_task.status,
      })
    end
    if child_task.results then
      return unpack_returns(child_task.results)
    end
    return child_task.result
  end
  function ctx:await_any_task(child_tasks)
    if type(child_tasks) ~= "table" then
      return nil, error_mod.new("input", "await_any_task requires a task list")
    end
    if #child_tasks == 0 then
      return nil, error_mod.new("input", "await_any_task requires at least one task")
    end
    if task.cancelled then
      return nil, error_mod.new("cancelled", "task cancelled", { task_id = task.id, name = task.name })
    end
    for _, child_task in ipairs(child_tasks) do
      if type(child_task) == "table"
        and (child_task.status == "completed" or child_task.status == "failed" or child_task.status == "cancelled")
      then
        return true
      end
    end
    return coroutine.yield({ type = "task_wait", tasks = child_tasks })
  end
  return ctx
end

function M.install(Host)
  function Host:spawn_task(name, fn, opts)
    if type(name) ~= "string" or name == "" then
      return nil, error_mod.new("input", "task name must be non-empty")
    end
    if type(fn) ~= "function" then
      return nil, error_mod.new("input", "task function is required")
    end
    local options = opts or {}
    local id = self._next_task_id
    self._next_task_id = id + 1
    local task = {
      id = id,
      name = name,
      service = options.service,
      status = "ready",
      started_at = now_seconds(),
      updated_at = now_seconds(),
      wake_at = nil,
      result = nil,
      error = nil,
      cancelled = false,
    }
    local ctx = make_task_context(task)
    task.co = coroutine.create(function()
      return fn(ctx)
    end)
    self._tasks[id] = task
    if options.priority == "front" then
      table.insert(self._task_queue, 1, id)
    else
      self._task_queue[#self._task_queue + 1] = id
    end
    emit_task_event(self, "task:started", {
      task_id = id,
      name = name,
      service = task.service,
    })
    return task
  end

  function Host:get_task(task_id)
    return self._tasks[task_id]
  end

  function Host:list_tasks()
    local out = {}
    for _, task in pairs(self._tasks) do
      out[#out + 1] = task
    end
    table.sort(out, function(a, b) return a.id < b.id end)
    return out
  end

  function Host:task_stats()
    local stats = {
      total = 0,
      queue_depth = #self._task_queue,
      identify_inflight = map_count(self._identify_inflight),
      by_status = {},
      by_name = {},
      by_service = {},
    }
    for _, task in pairs(self._tasks) do
      stats.total = stats.total + 1
      local status = task.status or "unknown"
      stats.by_status[status] = (stats.by_status[status] or 0) + 1
      local name = task.name or "unknown"
      stats.by_name[name] = (stats.by_name[name] or 0) + 1
      local service = task.service or "unknown"
      stats.by_service[service] = (stats.by_service[service] or 0) + 1
    end
    return stats
  end

  function Host:_cleanup_task_waiters(task)
    if not task then
      return false
    end
    local waiting_on = task.waiting_on
    if waiting_on then
      local waiter_maps = {
        waiting_read = self._task_read_waiters,
        waiting_dial = self._task_dial_waiters,
        waiting_write = self._task_write_waiters,
      }
      local waiters = waiter_maps[task.status] and waiter_maps[task.status][waiting_on]
      if waiters then
        waiters[task.id] = nil
        if next(waiters) == nil then
          waiter_maps[task.status][waiting_on] = nil
        end
      end
    end
    if type(task.read_unwatch) == "function" then
      pcall(task.read_unwatch)
    end
    if type(task.dial_unwatch) == "function" then
      pcall(task.dial_unwatch)
    end
    if type(task.write_unwatch) == "function" then
      pcall(task.write_unwatch)
    end
    if type(task.task_unwatch) == "function" then
      pcall(task.task_unwatch)
    end
    task.read_unwatch = nil
    task.dial_unwatch = nil
    task.write_unwatch = nil
    task.task_unwatch = nil
    task.waiting_on = nil
    task._read_woke_before_unwatch = nil
    task._dial_woke_before_unwatch = nil
    task._write_woke_before_unwatch = nil
    return true
  end

  function Host:cancel_task(task_id)
    local task = self._tasks[task_id]
    if not task then
      return false
    end
    if task.status == "completed" or task.status == "failed" or task.status == "cancelled" then
      return false
    end
    self:_cleanup_task_waiters(task)
    task.cancelled = true
    task.status = "cancelled"
    task.updated_at = now_seconds()
    emit_task_event(self, "task:cancelled", {
      task_id = task.id,
      name = task.name,
      service = task.service,
    })
    self:_wake_task_completion_waiters(task)
    return true
  end

  function Host:_enqueue_task(task)
    if not task or task.status ~= "ready" then
      return false
    end
    self._task_queue[#self._task_queue + 1] = task.id
    return true
  end

  function Host:run_until_task(task, opts)
    if type(task) ~= "table" then
      return nil, error_mod.new("input", "task table is required")
    end
    local options = opts or {}
    local poll_interval = options.poll_interval
    if poll_interval == nil then
      poll_interval = 0.01
    end
    local timeout = options.timeout
    local deadline = timeout and (now_seconds() + timeout) or nil

    while task.status ~= "completed" and task.status ~= "failed" and task.status ~= "cancelled" do
      if deadline and now_seconds() >= deadline then
        return nil, error_mod.new("timeout", "task wait timed out", {
          task_id = task.id,
          name = task.name,
        })
      end
      local ok, err = self:_poll_once(poll_interval)
      if not ok then
        return nil, err
      end
    end
    if task.status ~= "completed" then
      return nil, task.error or error_mod.new("state", "task did not complete", {
        task_id = task.id,
        name = task.name,
        status = task.status,
      })
    end
    if task.results then
      return unpack_returns(task.results)
    end
    return task.result
  end

  function Host:wait_task(task, opts)
    if type(task) ~= "table" then
      return nil, error_mod.new("input", "task table is required")
    end
    local options = opts or {}
    if options.ctx and type(options.ctx.await_task) == "function" then
      return options.ctx:await_task(task)
    end
    return self:run_until_task(task, options)
  end

  function Host:sleep(seconds, opts)
    if type(seconds) ~= "number" or seconds < 0 then
      return nil, error_mod.new("input", "sleep duration must be a non-negative number")
    end
    local options = opts or {}
    if options.ctx and type(options.ctx.sleep) == "function" then
      local slept, sleep_err = options.ctx:sleep(seconds)
      if slept == nil and sleep_err then
        return nil, sleep_err
      end
      return true
    end
    local task, task_err = self:spawn_task("host.sleep", function(ctx)
      local slept, sleep_err = ctx:sleep(seconds)
      if slept == nil and sleep_err then
        return nil, sleep_err
      end
      return true
    end, { service = "host" })
    if not task then
      return nil, task_err
    end
    return self:run_until_task(task, options)
  end

  function Host:_wait_task_read(task, connection)
    if not task or not connection then
      return false
    end
    local waiters = self._task_read_waiters[connection]
    if not waiters then
      waiters = {}
      self._task_read_waiters[connection] = waiters
    end
    waiters[task.id] = true
    task.status = "waiting_read"
    task.waiting_on = connection
    task.updated_at = now_seconds()
    if task.read_unwatch == nil and type(connection.watch_luv_readable) == "function" then
      local ok, unwatch = pcall(connection.watch_luv_readable, connection, function()
        self:_wake_task_readers(connection)
      end)
      if ok and type(unwatch) == "function" then
        if task._read_woke_before_unwatch then
          task._read_woke_before_unwatch = nil
          pcall(unwatch)
        elseif task.status == "waiting_read" then
          task.read_unwatch = unwatch
        else
          pcall(unwatch)
        end
      end
    end
    return true
  end

  function Host:_wake_task_readers(connection)
    local waiters = self._task_read_waiters[connection]
    if not waiters then
      return false
    end
    self._task_read_waiters[connection] = nil
    for task_id in pairs(waiters) do
      local task = self._tasks[task_id]
      if task and task.status == "waiting_read" and not task.cancelled then
        if type(task.read_unwatch) == "function" then
          pcall(task.read_unwatch)
          task.read_unwatch = nil
        else
          task._read_woke_before_unwatch = true
        end
        if type(task.dial_unwatch) == "function" then
          pcall(task.dial_unwatch)
          task.dial_unwatch = nil
        end
        if type(task.write_unwatch) == "function" then
          pcall(task.write_unwatch)
          task.write_unwatch = nil
        end
        task.status = "ready"
        task.waiting_on = nil
        task.updated_at = now_seconds()
        self:_enqueue_task(task)
      end
    end
    return true
  end

  function Host:_wait_task_dial(task, connection)
    if not task or not connection then
      return false
    end
    local waiters = self._task_dial_waiters[connection]
    if not waiters then
      waiters = {}
      self._task_dial_waiters[connection] = waiters
    end
    waiters[task.id] = true
    task.status = "waiting_dial"
    task.waiting_on = connection
    task.updated_at = now_seconds()
    if task.dial_unwatch == nil and type(connection.watch_luv_connect) == "function" then
      local ok, unwatch = pcall(connection.watch_luv_connect, connection, function()
        self:_wake_task_dialers(connection)
      end)
      if ok and type(unwatch) == "function" then
        if task._dial_woke_before_unwatch then
          task._dial_woke_before_unwatch = nil
          pcall(unwatch)
        elseif task.status == "waiting_dial" then
          task.dial_unwatch = unwatch
        else
          pcall(unwatch)
        end
      end
    end
    return true
  end

  function Host:_wake_task_dialers(connection)
    local waiters = self._task_dial_waiters[connection]
    if not waiters then
      return false
    end
    self._task_dial_waiters[connection] = nil
    for task_id in pairs(waiters) do
      local task = self._tasks[task_id]
      if task and task.status == "waiting_dial" and not task.cancelled then
        if type(task.dial_unwatch) == "function" then
          pcall(task.dial_unwatch)
          task.dial_unwatch = nil
        else
          task._dial_woke_before_unwatch = true
        end
        if type(task.write_unwatch) == "function" then
          pcall(task.write_unwatch)
          task.write_unwatch = nil
        end
        task.status = "ready"
        task.waiting_on = nil
        task.updated_at = now_seconds()
        self:_enqueue_task(task)
      end
    end
    return true
  end

  function Host:_wait_task_write(task, connection)
    if not task or not connection then
      return false
    end
    local waiters = self._task_write_waiters[connection]
    if not waiters then
      waiters = {}
      self._task_write_waiters[connection] = waiters
    end
    waiters[task.id] = true
    task.status = "waiting_write"
    task.waiting_on = connection
    task.updated_at = now_seconds()
    if task.write_unwatch == nil and type(connection.watch_luv_write) == "function" then
      local ok, unwatch = pcall(connection.watch_luv_write, connection, function()
        self:_wake_task_writers(connection)
      end)
      if ok and type(unwatch) == "function" then
        if task._write_woke_before_unwatch then
          task._write_woke_before_unwatch = nil
          pcall(unwatch)
        elseif task.status == "waiting_write" then
          task.write_unwatch = unwatch
        else
          pcall(unwatch)
        end
      end
    end
    return true
  end

  function Host:_wake_task_writers(connection)
    local waiters = self._task_write_waiters[connection]
    if not waiters then
      return false
    end
    self._task_write_waiters[connection] = nil
    for task_id in pairs(waiters) do
      local task = self._tasks[task_id]
      if task and task.status == "waiting_write" and not task.cancelled then
        if type(task.write_unwatch) == "function" then
          pcall(task.write_unwatch)
          task.write_unwatch = nil
        else
          task._write_woke_before_unwatch = true
        end
        task.status = "ready"
        task.waiting_on = nil
        task.updated_at = now_seconds()
        self:_enqueue_task(task)
      end
    end
    return true
  end

  function Host:_wait_task_completion(task, child_tasks)
    if not task or type(child_tasks) ~= "table" then
      return false
    end
    local watched_ids = {}
    for _, child_task in ipairs(child_tasks) do
      if type(child_task) == "table" and type(child_task.id) == "number" then
        if child_task.status == "completed" or child_task.status == "failed" or child_task.status == "cancelled" then
          task.status = "ready"
          task.updated_at = now_seconds()
          self:_enqueue_task(task)
          return true
        end
        local waiters = self._task_completion_waiters[child_task.id]
        if not waiters then
          waiters = {}
          self._task_completion_waiters[child_task.id] = waiters
        end
        waiters[task.id] = true
        watched_ids[#watched_ids + 1] = child_task.id
      end
    end
    if #watched_ids == 0 then
      task.status = "ready"
      task.updated_at = now_seconds()
      self:_enqueue_task(task)
      return true
    end
    task.status = "waiting_task"
    task.waiting_on = "task"
    task.updated_at = now_seconds()
    task.task_unwatch = function()
      for _, child_id in ipairs(watched_ids) do
        local waiters = self._task_completion_waiters[child_id]
        if waiters then
          waiters[task.id] = nil
          if next(waiters) == nil then
            self._task_completion_waiters[child_id] = nil
          end
        end
      end
      return true
    end
    return true
  end

  function Host:_wake_task_completion_waiters(child_task)
    if not child_task or type(child_task.id) ~= "number" then
      return false
    end
    local waiters = self._task_completion_waiters[child_task.id]
    if not waiters then
      return false
    end
    self._task_completion_waiters[child_task.id] = nil
    for task_id in pairs(waiters) do
      local task = self._tasks[task_id]
      if task and task.status == "waiting_task" and not task.cancelled then
        if type(task.task_unwatch) == "function" then
          pcall(task.task_unwatch)
          task.task_unwatch = nil
        end
        task.status = "ready"
        task.waiting_on = nil
        task.updated_at = now_seconds()
        self:_enqueue_task(task)
      end
    end
    return true
  end

  function Host:_run_background_tasks(opts)
    local options = opts or {}
    local budget = options.max_resumes or self._task_resume_budget or M.DEFAULT_TASK_RESUME_BUDGET
    local now = now_seconds()

    for _, task in pairs(self._tasks) do
      if task.status == "sleeping" and task.wake_at and task.wake_at <= now then
        task.status = "ready"
        task.wake_at = nil
        task.updated_at = now
        self:_enqueue_task(task)
      end
    end

    local resumes = 0
    while resumes < budget and #self._task_queue > 0 do
      local task_id = table.remove(self._task_queue, 1)
      local task = self._tasks[task_id]
      if not task or task.status ~= "ready" then
        goto continue_tasks
      end
      if task.cancelled then
        self:_cleanup_task_waiters(task)
        task.status = "cancelled"
        task.updated_at = now_seconds()
        goto continue_tasks
      end

      resumes = resumes + 1
      task.status = "running"
      task.updated_at = now_seconds()
      local resumed = pack_returns(coroutine.resume(task.co))
      local ok = resumed[1]
      local result_or_yield = resumed[2]
      local extra = resumed[3]
      task.updated_at = now_seconds()
      if not ok then
        self:_cleanup_task_waiters(task)
        task.status = "failed"
        task.error = error_mod.new("protocol", "task panicked", {
          task_id = task.id,
          name = task.name,
          cause = result_or_yield,
          traceback = debug.traceback(task.co, tostring(result_or_yield)),
        })
        emit_task_event(self, "task:failed", {
          task_id = task.id,
          name = task.name,
          service = task.service,
          error = task.error,
        })
        self:_wake_task_completion_waiters(task)
        goto continue_tasks
      end
      if coroutine.status(task.co) == "dead" then
        self:_cleanup_task_waiters(task)
        if result_or_yield == nil and extra then
          task.status = "failed"
          task.error = extra
          emit_task_event(self, "task:failed", {
            task_id = task.id,
            name = task.name,
            service = task.service,
            error = task.error,
          })
          self:_wake_task_completion_waiters(task)
        else
          task.status = "completed"
          task.result = result_or_yield
          task.results = { n = math.max((resumed.n or 1) - 1, 0) }
          for result_index = 2, resumed.n or 1 do
            task.results[result_index - 1] = resumed[result_index]
          end
          emit_task_event(self, "task:completed", {
            task_id = task.id,
            name = task.name,
            service = task.service,
            result = task.result,
          })
          self:_wake_task_completion_waiters(task)
        end
        goto continue_tasks
      end

      local yielded = result_or_yield
      if type(yielded) == "table" and yielded.type == "sleep" then
        task.status = "sleeping"
        task.wake_at = yielded.until_at or now_seconds()
      elseif type(yielded) == "table" and yielded.type == "read" then
        self:_wait_task_read(task, yielded.connection or yielded.conn or yielded.watchable)
      elseif type(yielded) == "table" and yielded.type == "dial" then
        self:_wait_task_dial(task, yielded.connection or yielded.conn or yielded.watchable)
      elseif type(yielded) == "table" and yielded.type == "write" then
        self:_wait_task_write(task, yielded.connection or yielded.conn or yielded.watchable)
      elseif type(yielded) == "table" and yielded.type == "task_wait" then
        self:_wait_task_completion(task, yielded.tasks or yielded.task or yielded.child_tasks)
      else
        task.status = "ready"
        self:_enqueue_task(task)
      end

      ::continue_tasks::
    end

    return true
  end
end

return M
