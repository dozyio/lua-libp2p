local ed25519 = require("lua_libp2p.crypto.ed25519")
local host_mod = require("lua_libp2p.host")

local function run()
  local keypair = assert(ed25519.generate_keypair())
  local connected_count = 0
  local disconnected_count = 0

  local host, host_err = host_mod.new({
    identity = keypair,
    scheduler_connection_pump = false,
    on = {
      peer_connected = function(payload)
        if payload and payload.peer_id == "peer-a" then
          connected_count = connected_count + 1
        end
      end,
      peer_disconnected = function(payload)
        if payload and payload.peer_id == "peer-a" then
          disconnected_count = disconnected_count + 1
        end
      end,
    },
  })
  if not host then
    return nil, host_err
  end

  local bus_all, bus_all_err = host:subscribe()
  if not bus_all then
    return nil, bus_all_err
  end
  local bus_disconnected = assert(host:subscribe("peer_disconnected"))
  local bus_self = assert(host:subscribe("self_peer_update"))

  local conn = {
    closed = false,
  }
  function conn:close()
    self.closed = true
    return true
  end

  local entry, register_err = host:_register_connection(conn, {
    remote_peer_id = "peer-a",
  })
  if not entry then
    return nil, register_err
  end
  if connected_count ~= 1 then
    return nil, "expected peer_connected callback to fire"
  end

  local ev1 = host:next_event(bus_all)
  if not ev1 or ev1.name ~= "peer_connected" then
    return nil, "expected peer_connected from event bus subscription"
  end
  local ev1b = host:next_event(bus_all)
  if not ev1b or ev1b.name ~= "connection_opened" then
    return nil, "expected connection_opened from event bus subscription"
  end

  local closed, close_err = host:close()
  if not closed then
    return nil, close_err
  end
  if not conn.closed then
    return nil, "expected close() to close tracked connection"
  end
  if disconnected_count ~= 1 then
    return nil, "expected peer_disconnected callback to fire"
  end

  local ev2 = host:next_event(bus_all)
  if not ev2 or ev2.name ~= "peer_disconnected" then
    return nil, "expected peer_disconnected from event bus subscription"
  end

  local ev2b = host:next_event(bus_disconnected)
  if not ev2b or ev2b.name ~= "peer_disconnected" then
    return nil, "expected filtered subscription to receive peer_disconnected"
  end

  local called = 0
  local handler = function()
    called = called + 1
  end
  assert(host:on("peer_connected", handler))
  if not host:off("peer_connected", handler) then
    return nil, "expected off() to remove registered handler"
  end

  local c2 = { close = function() return true end }
  local e2, e2_err = host:_register_connection(c2, { remote_peer_id = "peer-b" })
  if not e2 then
    return nil, e2_err
  end
  if called ~= 0 then
    return nil, "removed handler should not be called"
  end

  if not host:unsubscribe(bus_disconnected) then
    return nil, "expected unsubscribe to remove subscription"
  end
  local self_update_ok, self_update_err = host:_emit_self_peer_update_if_changed()
  if not self_update_ok then
    return nil, self_update_err
  end
  local self_update = host:next_event(bus_self)
  if not self_update or self_update.name ~= "self_peer_update" then
    return nil, "expected self_peer_update event"
  end
  local no_update_ok, no_update_err = host:_emit_self_peer_update_if_changed()
  if not no_update_ok then
    return nil, no_update_err
  end
  if host:next_event(bus_self) ~= nil then
    return nil, "self_peer_update should only emit when addrs change"
  end
  if not host:unsubscribe(bus_self) then
    return nil, "expected unsubscribe to remove self subscription"
  end
  if not host:unsubscribe(bus_all) then
    return nil, "expected unsubscribe to remove subscription"
  end

  local task_events = assert(host:subscribe())
  local steps = {}
  local task = assert(host:spawn_task("test.task", function(ctx)
    steps[#steps + 1] = "start"
    ctx:checkpoint()
    steps[#steps + 1] = "after_checkpoint"
    ctx:sleep(0)
    steps[#steps + 1] = "after_sleep"
    return "ok"
  end, { service = "tests" }))
  if task.status ~= "ready" then
    return nil, "spawned task should start ready"
  end
  assert(host:_run_background_tasks({ max_resumes = 1 }))
  if steps[1] ~= "start" or task.status ~= "ready" then
    return nil, "checkpoint should yield and requeue task"
  end
  assert(host:_run_background_tasks({ max_resumes = 1 }))
  if steps[2] ~= "after_checkpoint" or task.status ~= "sleeping" then
    return nil, "sleep should yield task"
  end
  assert(host:_run_background_tasks({ max_resumes = 1 }))
  if steps[3] ~= "after_sleep" or task.status ~= "completed" or task.result ~= "ok" then
    return nil, "task should complete after sleep wake"
  end
  local saw_started = false
  local saw_completed = false
  while true do
    local event = host:next_event(task_events)
    if not event then
      break
    end
    if event.name == "task:started" and event.payload.task_id == task.id then
      saw_started = true
    elseif event.name == "task:completed" and event.payload.task_id == task.id then
      saw_completed = true
    end
  end
  if not saw_started or not saw_completed then
    return nil, "task lifecycle events should be emitted"
  end
  local cancelled = assert(host:spawn_task("test.cancel", function(ctx)
    ctx:sleep(10)
    return true
  end))
  if not host:cancel_task(cancelled.id) then
    return nil, "cancel_task should cancel running task"
  end
  if cancelled.status ~= "cancelled" then
    return nil, "cancelled task should be marked cancelled"
  end
  local unwatch_called = false
  local readable = {}
  function readable:watch_luv_readable(on_readable)
    self.on_readable = on_readable
    return function()
      unwatch_called = true
    end
  end
  local read_steps = {}
  local read_task = assert(host:spawn_task("test.read_wait", function(ctx)
    read_steps[#read_steps + 1] = "waiting"
    ctx:wait_read(readable)
    read_steps[#read_steps + 1] = "resumed"
    return true
  end))
  assert(host:_run_background_tasks({ max_resumes = 1 }))
  if read_task.status ~= "waiting_read" or read_steps[1] ~= "waiting" then
    return nil, "read yield should park task until readable"
  end
  assert(host:_run_background_tasks({ max_resumes = 1 }))
  if read_steps[2] ~= nil then
    return nil, "read-waiting task should not resume before wake"
  end
  if type(readable.on_readable) ~= "function" then
    return nil, "read wait should register readable watcher"
  end
  readable.on_readable()
  assert(host:_run_background_tasks({ max_resumes = 1 }))
  if read_task.status ~= "completed" or read_steps[2] ~= "resumed" then
    return nil, "read wake should resume waiting task"
  end
  if not unwatch_called then
    return nil, "read wake should unregister readable watcher"
  end
  local dial_unwatch_called = false
  local dialable = {}
  function dialable:watch_luv_connect(on_connect)
    self.on_connect = on_connect
    return function()
      dial_unwatch_called = true
    end
  end
  local dial_steps = {}
  local dial_task = assert(host:spawn_task("test.dial_wait", function(ctx)
    dial_steps[#dial_steps + 1] = "waiting"
    ctx:wait_dial(dialable)
    dial_steps[#dial_steps + 1] = "resumed"
    return true
  end))
  assert(host:_run_background_tasks({ max_resumes = 1 }))
  if dial_task.status ~= "waiting_dial" or dial_steps[1] ~= "waiting" then
    return nil, "dial yield should park task until connect callback"
  end
  assert(host:_run_background_tasks({ max_resumes = 1 }))
  if dial_steps[2] ~= nil then
    return nil, "dial-waiting task should not resume before wake"
  end
  dialable.on_connect()
  assert(host:_run_background_tasks({ max_resumes = 1 }))
  if dial_task.status ~= "completed" or dial_steps[2] ~= "resumed" then
    return nil, "dial wake should resume waiting task"
  end
  if not dial_unwatch_called then
    return nil, "dial wake should unregister connect watcher"
  end
  local write_unwatch_called = false
  local writable = {}
  function writable:watch_luv_write(on_write)
    self.on_write = on_write
    return function()
      write_unwatch_called = true
    end
  end
  local write_steps = {}
  local write_task = assert(host:spawn_task("test.write_wait", function(ctx)
    write_steps[#write_steps + 1] = "waiting"
    ctx:wait_write(writable)
    write_steps[#write_steps + 1] = "resumed"
    return true
  end))
  assert(host:_run_background_tasks({ max_resumes = 1 }))
  if write_task.status ~= "waiting_write" or write_steps[1] ~= "waiting" then
    return nil, "write yield should park task until write callback"
  end
  assert(host:_run_background_tasks({ max_resumes = 1 }))
  if write_steps[2] ~= nil then
    return nil, "write-waiting task should not resume before wake"
  end
  writable.on_write()
  assert(host:_run_background_tasks({ max_resumes = 1 }))
  if write_task.status ~= "completed" or write_steps[2] ~= "resumed" then
    return nil, "write wake should resume waiting task"
  end
  if not write_unwatch_called then
    return nil, "write wake should unregister write watcher"
  end

  local immediate_read_unwatch = false
  local immediate_readable = {}
  function immediate_readable:watch_luv_readable(on_readable)
    on_readable()
    return function()
      immediate_read_unwatch = true
    end
  end
  local immediate_read_task = assert(host:spawn_task("test.immediate_read_wait", function(ctx)
    ctx:wait_read(immediate_readable)
    return true
  end))
  assert(host:_run_background_tasks({ max_resumes = 1 }))
  if (immediate_read_task.status ~= "ready" and immediate_read_task.status ~= "completed")
    or immediate_read_task.read_unwatch ~= nil
  then
    return nil, "immediate read watcher wake should not leave stale watcher"
  end

  local immediate_write_unwatch = false
  local immediate_writable = {}
  function immediate_writable:watch_luv_write(on_write)
    on_write()
    return function()
      immediate_write_unwatch = true
    end
  end
  local immediate_write_task = assert(host:spawn_task("test.immediate_write_wait", function(ctx)
    ctx:wait_write(immediate_writable)
    return true
  end))
  assert(host:_run_background_tasks({ max_resumes = 1 }))
  if (immediate_write_task.status ~= "ready" and immediate_write_task.status ~= "completed")
    or immediate_write_task.write_unwatch ~= nil
  then
    return nil, "immediate write watcher wake should not leave stale watcher"
  end

  local cleanup = {
    read = false,
    dial = false,
    write = false,
  }
  local cleanup_task = assert(host:spawn_task("test.cleanup_waiters", function()
    return true
  end))
  cleanup_task.read_unwatch = function()
    cleanup.read = true
  end
  cleanup_task.dial_unwatch = function()
    cleanup.dial = true
  end
  cleanup_task.write_unwatch = function()
    cleanup.write = true
  end
  assert(host:_run_background_tasks({ max_resumes = 10 }))
  if not (cleanup.read and cleanup.dial and cleanup.write) then
    return nil, "completed task should clean all watcher callbacks"
  end

  local wait_host = assert(host_mod.new({
    identity = keypair,
    scheduler_connection_pump = false,
  }))
  local run_task = assert(wait_host:spawn_task("test.run_until_task", function()
    return "run-result"
  end))
  local run_result, run_err = wait_host:run_until_task(run_task, { poll_interval = 0 })
  if not run_result then
    return nil, run_err
  end
  if run_result ~= "run-result" then
    return nil, "run_until_task should return task result"
  end

  local parent_task = assert(wait_host:spawn_task("test.await_parent", function(ctx)
    local child = assert(wait_host:spawn_task("test.await_child", function()
      return "child-result"
    end))
    return ctx:await_task(child)
  end))
  local parent_result, parent_err = wait_host:run_until_task(parent_task, { poll_interval = 0 })
  if not parent_result then
    return nil, parent_err
  end
  if parent_result ~= "child-result" then
    return nil, "await_task should return child task result"
  end

  local any_parent = assert(wait_host:spawn_task("test.await_any_parent", function(ctx)
    local slow = assert(wait_host:spawn_task("test.await_any_slow", function(child_ctx)
      child_ctx:sleep(1)
      return "slow"
    end))
    local fast = assert(wait_host:spawn_task("test.await_any_fast", function()
      return "fast"
    end))
    local waited, wait_err = ctx:await_any_task({ slow, fast })
    if waited == nil and wait_err then
      return nil, wait_err
    end
    return fast.status
  end))
  local any_result, any_err = wait_host:run_until_task(any_parent, { poll_interval = 0 })
  if not any_result then
    return nil, any_err
  end
  if any_result ~= "completed" then
    return nil, "await_any_task should resume when any child task completes"
  end
  assert(host:unsubscribe(task_events))

  return true
end

return {
  name = "host lifecycle events queue and hooks",
  run = run,
}
