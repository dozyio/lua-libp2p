--- Host cooperative task scheduler internals.
-- @module lua_libp2p.host.tasks
local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log").subsystem("host")

local M = {}

M.DEFAULT_TASK_RESUME_BUDGET = 128
M.DEFAULT_TASK_RETENTION = 2048
M.DEFAULT_TASK_PRUNE_INTERVAL = 1

local function now_seconds()
  local ok_socket, socket = pcall(require, "socket")
  if ok_socket and type(socket.gettime) == "function" then
    return socket.gettime()
  end
  return os.time()
end

local function debug_perf_add(host, key, elapsed_seconds)
  local perf = type(host) == "table" and rawget(host, "_debug_perf") or nil
  if type(perf) ~= "table" then
    return
  end
  perf[key .. "_calls"] = (perf[key .. "_calls"] or 0) + 1
  perf[key .. "_ms"] = (perf[key .. "_ms"] or 0) + (elapsed_seconds * 1000)
end

local function debug_perf_add_group(host, group_key, item_key, elapsed_seconds)
  local perf = type(host) == "table" and rawget(host, "_debug_perf") or nil
  if type(perf) ~= "table" then
    return
  end
  local group = perf[group_key]
  if type(group) ~= "table" then
    group = {}
    perf[group_key] = group
  end
  local item = group[tostring(item_key or "unknown")]
  if type(item) ~= "table" then
    item = { calls = 0, ms = 0 }
    group[tostring(item_key or "unknown")] = item
  end
  item.calls = (item.calls or 0) + 1
  item.ms = (item.ms or 0) + (elapsed_seconds * 1000)
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

local function task_queue_depth(host)
  local head = host._task_queue_head or 1
  local tail = host._task_queue_tail or 0
  if tail < head then
    return 0
  end
  return tail - head + 1
end

local function task_queue_push_back(host, task_id)
  local tail = (host._task_queue_tail or 0) + 1
  host._task_queue_tail = tail
  if not host._task_queue_head then
    host._task_queue_head = 1
  end
  host._task_queue[tail] = task_id
end

local function task_queue_push_front(host, task_id)
  local head = (host._task_queue_head or 1) - 1
  host._task_queue_head = head
  if not host._task_queue_tail or host._task_queue_tail < head then
    host._task_queue_tail = head
  end
  host._task_queue[head] = task_id
end

local function task_queue_pop_front(host)
  local head = host._task_queue_head or 1
  local tail = host._task_queue_tail or 0
  if tail < head then
    return nil
  end
  local task_id = host._task_queue[head]
  host._task_queue[head] = nil
  head = head + 1
  if head > tail then
    host._task_queue_head = 1
    host._task_queue_tail = 0
  else
    host._task_queue_head = head
  end
  return task_id
end

local function should_prune_task(task)
  if type(task) ~= "table" then
    return false
  end
  return task.status == "completed" or task.status == "failed" or task.status == "cancelled"
end

local function prune_task_history(self)
  local retention = tonumber(self._task_retention) or M.DEFAULT_TASK_RETENTION
  if retention < 0 then
    return
  end
  local now = now_seconds()
  local interval = tonumber(self._task_prune_interval) or M.DEFAULT_TASK_PRUNE_INTERVAL
  if interval > 0 and self._last_task_prune_at and (now - self._last_task_prune_at) < interval then
    return
  end
  self._last_task_prune_at = now

  local total = 0
  for _ in pairs(self._tasks) do
    total = total + 1
  end
  if total <= retention then
    return
  end

  local pruneable = {}
  for task_id, task in pairs(self._tasks) do
    if should_prune_task(task) then
      pruneable[#pruneable + 1] = {
        task_id = task_id,
        updated_at = tonumber(task.updated_at) or 0,
      }
    end
  end

  if #pruneable == 0 then
    return
  end

  table.sort(pruneable, function(a, b)
    if a.updated_at == b.updated_at then
      return tostring(a.task_id) < tostring(b.task_id)
    end
    return a.updated_at < b.updated_at
  end)

  local to_prune = math.min(total - retention, #pruneable)
  for i = 1, to_prune do
    self._tasks[pruneable[i].task_id] = nil
  end
end

local function emit_task_event(host, name, payload)
  if type(host.emit) == "function" then
    return host:emit(name, payload)
  end
  return true
end

local function is_nonfatal_stream_error(err)
  if not (err and error_mod.is_error(err)) then
    return false
  end
  return err.kind == "timeout"
    or err.kind == "busy"
    or err.kind == "closed"
    or err.kind == "decode"
    or err.kind == "protocol"
    or err.kind == "unsupported"
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

local function release_task_resources(host, task)
  if task and task.stream and type(host._release_stream_resource) == "function" then
    host:_release_stream_resource(task.stream)
    task.stream = nil
  end
end

local function run_task_finalizer(host, task)
  if not task or type(task.on_finished) ~= "function" then
    return true
  end
  local finalizer = task.on_finished
  task.on_finished = nil
  local ok, result, err = pcall(finalizer, host, task)
  if not ok then
    return nil, error_mod.new("protocol", "task finalizer panicked", {
      task_id = task.id,
      name = task.name,
      cause = result,
    })
  end
  if result == nil and err then
    return nil, err
  end
  return true
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
      task_queue_push_front(self, id)
    else
      task_queue_push_back(self, id)
    end
    emit_task_event(self, "task:started", {
      task_id = id,
      name = name,
      service = task.service,
    })
    log.debug("host task started", {
      task_id = id,
      name = name,
      service = task.service,
      queue_depth = task_queue_depth(self),
    })
    return task
  end

  function Host:_task_queue_depth()
    return task_queue_depth(self)
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
      queue_depth = task_queue_depth(self),
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

  function Host:_spawn_handler_task(handler, ctx)
    local task, task_err = self:spawn_task("handler." .. tostring(ctx.protocol or "unknown"), function(task_ctx)
      local handler_ctx = {}
      for k, v in pairs(ctx or {}) do
        handler_ctx[k] = v
      end
      for k, v in pairs(task_ctx) do
        if handler_ctx[k] == nil then
          handler_ctx[k] = v
        end
      end
      local call_ok, result, err = pcall(handler, ctx.stream, handler_ctx)
      self:_release_stream_resource(ctx.stream)
      if not call_ok then
        return nil, error_mod.new("protocol", "handler task panicked", { cause = result })
      end
      if result == nil and err then
        if is_nonfatal_stream_error(err) then
          if ctx.connection and type(ctx.connection.close) == "function" then
            ctx.connection:close()
          end
          return true
        end
        return nil, err
      end
      return result
    end, {
      service = "handler",
      protocol = ctx.protocol,
      peer_id = ctx.peer_id,
    })
    if task then
      task.stream = ctx.stream
    end
    return task, task_err
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
    if self._sleeping_tasks then
      self._sleeping_tasks[task.id] = nil
    end
    self:_cleanup_task_waiters(task)
    task.cancelled = true
    task.status = "cancelled"
    task.updated_at = now_seconds()
    release_task_resources(self, task)
    local final_ok, final_err = run_task_finalizer(self, task)
    if not final_ok then
      return nil, final_err
    end
    emit_task_event(self, "task:cancelled", {
      task_id = task.id,
      name = task.name,
      service = task.service,
    })
    log.debug("host task cancelled", {
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
    task_queue_push_back(self, task.id)
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
    local perf = rawget(self, "_debug_perf")
    local phase_started = perf and now_seconds() or nil

    for task_id in pairs(self._sleeping_tasks or {}) do
      local task = self._tasks[task_id]
      if not task or task.status ~= "sleeping" then
        self._sleeping_tasks[task_id] = nil
      elseif task.wake_at and task.wake_at <= now then
        self._sleeping_tasks[task_id] = nil
        task.status = "ready"
        task.wake_at = nil
        task.updated_at = now
        self:_enqueue_task(task)
      end
    end
    if phase_started then
      debug_perf_add(self, "background_sleep_scan", now_seconds() - phase_started)
      phase_started = now_seconds()
    end

    local resumes = 0
    while resumes < budget and task_queue_depth(self) > 0 do
      local resume_phase_started = perf and now_seconds() or nil
      local task_id = task_queue_pop_front(self)
      local task = self._tasks[task_id]
      if not task or task.status ~= "ready" then
        if resume_phase_started then
          debug_perf_add(self, "background_resume_skip", now_seconds() - resume_phase_started)
        end
        goto continue_tasks
      end
      if task.cancelled then
        self:_cleanup_task_waiters(task)
        task.status = "cancelled"
        task.updated_at = now_seconds()
        if resume_phase_started then
          debug_perf_add(self, "background_resume_cancel", now_seconds() - resume_phase_started)
        end
        goto continue_tasks
      end
      if resume_phase_started then
        debug_perf_add(self, "background_resume_dequeue", now_seconds() - resume_phase_started)
        resume_phase_started = now_seconds()
      end

      resumes = resumes + 1
      task.status = "running"
      task.updated_at = now_seconds()
      local resumed = pack_returns(coroutine.resume(task.co))
      if resume_phase_started then
        local coroutine_elapsed = now_seconds() - resume_phase_started
        debug_perf_add(self, "background_resume_coroutine", coroutine_elapsed)
        debug_perf_add_group(self, "background_resume_by_name", task.name, coroutine_elapsed)
        debug_perf_add_group(self, "background_resume_by_service", task.service or "unknown", coroutine_elapsed)
        resume_phase_started = now_seconds()
      end
      local ok = resumed[1]
      local result_or_yield = resumed[2]
      local extra = resumed[3]
      task.updated_at = now_seconds()
      if not ok then
        self:_cleanup_task_waiters(task)
        release_task_resources(self, task)
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
        log.debug("host task failed", {
          task_id = task.id,
          name = task.name,
          service = task.service,
          cause = tostring(task.error),
        })
        self:_wake_task_completion_waiters(task)
        local final_ok, final_err = run_task_finalizer(self, task)
        if not final_ok then
          return nil, final_err
        end
        if resume_phase_started then
          debug_perf_add(self, "background_resume_failed", now_seconds() - resume_phase_started)
        end
        goto continue_tasks
      end
      if coroutine.status(task.co) == "dead" then
        self:_cleanup_task_waiters(task)
        release_task_resources(self, task)
        if result_or_yield == nil and extra then
          task.status = "failed"
          task.error = extra
          emit_task_event(self, "task:failed", {
            task_id = task.id,
            name = task.name,
            service = task.service,
            error = task.error,
          })
          log.debug("host task failed", {
            task_id = task.id,
            name = task.name,
            service = task.service,
            cause = tostring(task.error),
          })
          self:_wake_task_completion_waiters(task)
          local final_ok, final_err = run_task_finalizer(self, task)
          if not final_ok then
            return nil, final_err
          end
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
          log.debug("host task completed", {
            task_id = task.id,
            name = task.name,
            service = task.service,
          })
          self:_wake_task_completion_waiters(task)
          local final_ok, final_err = run_task_finalizer(self, task)
          if not final_ok then
            return nil, final_err
          end
        end
        if resume_phase_started then
          debug_perf_add(self, "background_resume_completed", now_seconds() - resume_phase_started)
        end
        goto continue_tasks
      end

      local yielded = result_or_yield
      if type(yielded) == "table" and yielded.type == "sleep" then
        task.status = "sleeping"
        task.wake_at = yielded.until_at or now_seconds()
        if self._sleeping_tasks then
          self._sleeping_tasks[task.id] = true
        end
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
      if resume_phase_started then
        debug_perf_add(self, "background_resume_yield", now_seconds() - resume_phase_started)
      end

      ::continue_tasks::
    end
    if phase_started then
      debug_perf_add(self, "background_resume", now_seconds() - phase_started)
      phase_started = now_seconds()
    end

    prune_task_history(self)
    if phase_started then
      debug_perf_add(self, "background_prune", now_seconds() - phase_started)
    end
    return true
  end
end

return M
