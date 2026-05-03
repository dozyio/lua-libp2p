local ed25519 = require("lua_libp2p.crypto.ed25519")
local host_mod = require("lua_libp2p.host")

local function run()
  local keypair = assert(ed25519.generate_keypair())
  local connected_count = 0
  local disconnected_count = 0

  local host, host_err = host_mod.new({
    identity = keypair,
    runtime = "luv",
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

  local rollback_host = assert(host_mod.new({
    identity = keypair,
    runtime = "luv",
    on = {
      connection_opened = function()
        error("forced event failure")
      end,
    },
  }))
  local rollback_entry, rollback_err = rollback_host:_register_connection({ close = function() return true end }, {
    remote_peer_id = "peer-rollback",
    direction = "outbound",
  })
  if rollback_entry ~= nil or not rollback_err then
    return nil, "event failure should reject connection registration"
  end
  if #rollback_host._connections ~= 0
    or rollback_host._connections_by_peer["peer-rollback"] ~= nil
    or rollback_host.connection_manager:stats().connections ~= 0
  then
    return nil, "failed connection registration should rollback host and manager indexes"
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

  local socket = require("socket")
  local server = assert(socket.bind("127.0.0.1", 0))
  server:settimeout(0)
  local _, port = assert(server:getsockname())
  local client = assert(socket.tcp())
  client:settimeout(0)
  client:connect("127.0.0.1", port)
  local accepted
  for _ = 1, 100 do
    accepted = server:accept()
    if accepted then
      break
    end
    socket.sleep(0.001)
  end
  if not accepted then
    client:close()
    server:close()
    return nil, "test tcp accept should complete"
  end
  accepted:settimeout(0)
  local luv_wait_host = assert(host_mod.new({
    identity = keypair,
    runtime = "luv",
  }))
  local luv_readable = { _socket = accepted }
  local luv_read_resumed = false
  local luv_read_task = assert(luv_wait_host:spawn_task("test.luv_read_wait", function(ctx)
    ctx:wait_read(luv_readable)
    luv_read_resumed = true
    return true
  end))
  assert(luv_wait_host:_run_background_tasks({ max_resumes = 1 }))
  if luv_read_task.status ~= "waiting_read" then
    return nil, "luv read task should park before socket readiness"
  end
  assert(client:send("x"))
  assert(luv_wait_host:_poll_once(0.05))
  if luv_read_task.status ~= "completed" or not luv_read_resumed then
    accepted:close()
    client:close()
    server:close()
    return nil, "luv runtime should wake readable task waiters from socket readiness"
  end
  accepted:close()
  client:close()
  server:close()
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

  local immediate_readable = {}
  function immediate_readable:watch_luv_readable(on_readable)
    on_readable()
    return function() end
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

  local immediate_writable = {}
  function immediate_writable:watch_luv_write(on_write)
    on_write()
    return function() end
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
    task = false,
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
  cleanup_task.task_unwatch = function()
    cleanup.task = true
  end
  assert(host:_run_background_tasks({ max_resumes = 10 }))
  if not (cleanup.read and cleanup.dial and cleanup.write and cleanup.task) then
    return nil, "completed task should clean all watcher callbacks"
  end

  local wait_host = assert(host_mod.new({
    identity = keypair,
    runtime = "luv",
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

  local wait_task = assert(wait_host:spawn_task("test.wait_task", function()
    return "wait-result"
  end))
  local wait_result, wait_err = wait_host:wait_task(wait_task, { poll_interval = 0 })
  if not wait_result then
    return nil, wait_err
  end
  if wait_result ~= "wait-result" then
    return nil, "wait_task should return task result"
  end

  local slept, sleep_err = wait_host:sleep(0, { poll_interval = 0 })
  if not slept then
    return nil, sleep_err
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
    local waited, await_err = ctx:await_any_task({ slow, fast })
    if waited == nil and await_err then
      return nil, await_err
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

  local empty_wait = assert(wait_host:spawn_task("test.await_any_empty", function(ctx)
    local ok, err = ctx:await_any_task({})
    if ok ~= nil or not err or err.kind ~= "input" then
      return nil, "await_any_task should reject empty task lists"
    end
    return true
  end))
  local empty_result, empty_err = wait_host:run_until_task(empty_wait, { poll_interval = 0 })
  if not empty_result then
    return nil, empty_err
  end

  local parked_child = assert(wait_host:spawn_task("test.cancel_wait_child", function(ctx)
    ctx:sleep(10)
    return true
  end))
  local parked_parent = assert(wait_host:spawn_task("test.cancel_wait_parent", function(ctx)
    return ctx:await_task(parked_child)
  end))
  assert(wait_host:_run_background_tasks({ max_resumes = 4 }))
  if parked_parent.status ~= "waiting_task" then
    return nil, "await_task parent should park on child completion"
  end
  if not wait_host._task_completion_waiters[parked_child.id]
    or not wait_host._task_completion_waiters[parked_child.id][parked_parent.id]
  then
    return nil, "await_task should register task completion waiter"
  end
  assert(wait_host:cancel_task(parked_parent.id))
  if wait_host._task_completion_waiters[parked_child.id] ~= nil then
    return nil, "cancelling task waiting on child should clean completion waiter"
  end

  local many_children = {}
  for i = 1, 20 do
    many_children[i] = assert(wait_host:spawn_task("test.await_many_child", function(child_ctx)
      child_ctx:sleep(i)
      return i
    end))
  end
  local many_parent = assert(wait_host:spawn_task("test.await_many_parent", function(ctx)
    return ctx:await_any_task(many_children)
  end))
  assert(wait_host:_run_background_tasks({ max_resumes = 25 }))
  if many_parent.status ~= "waiting_task" then
    return nil, "await_any_task should park parent with many children"
  end
  assert(wait_host:cancel_task(many_parent.id))
  for _, child in ipairs(many_children) do
    if wait_host._task_completion_waiters[child.id] ~= nil then
      return nil, "cancelling await_any_task parent should clean all child waiters"
    end
    wait_host:cancel_task(child.id)
  end

  local close_host = assert(host_mod.new({
    identity = keypair,
    runtime = "luv",
  }))
  local close_cleanup = { read = false, dial = false, write = false, task = false }
  local close_readable = {}
  function close_readable:watch_luv_readable()
    return function()
      close_cleanup.read = true
    end
  end
  local close_writable = {}
  function close_writable:watch_luv_write()
    return function()
      close_cleanup.write = true
    end
  end
  local close_dialable = {}
  function close_dialable:watch_luv_connect()
    return function()
      close_cleanup.dial = true
    end
  end
  local close_child = assert(close_host:spawn_task("test.close_wait_child", function(ctx)
    ctx:sleep(10)
    return true
  end))
  local close_parent = assert(close_host:spawn_task("test.close_wait_parent", function(ctx)
    return ctx:await_task(close_child)
  end))
  local close_read_task = assert(close_host:spawn_task("test.close_wait_read", function(ctx)
    return ctx:wait_read(close_readable)
  end))
  local close_dial_task = assert(close_host:spawn_task("test.close_wait_dial", function(ctx)
    return ctx:wait_dial(close_dialable)
  end))
  local close_write_task = assert(close_host:spawn_task("test.close_wait_write", function(ctx)
    return ctx:wait_write(close_writable)
  end))
  assert(close_host:_run_background_tasks({ max_resumes = 10 }))
  close_parent.task_unwatch = function()
    close_cleanup.task = true
  end
  if close_parent.status ~= "waiting_task"
    or close_read_task.status ~= "waiting_read"
    or close_dial_task.status ~= "waiting_dial"
    or close_write_task.status ~= "waiting_write"
  then
    return nil, "close stress tasks should be parked before host close"
  end
  assert(close_host:close())
  if not (close_cleanup.read and close_cleanup.dial and close_cleanup.write and close_cleanup.task) then
    return nil, "host close should clean waiters for parked tasks"
  end

  local fairness_host = assert(host_mod.new({
    identity = keypair,
    runtime = "luv",
    task_resume_budget = 12,
  }))
  local fairness_readables = {}
  local fairness_counts = { sleeping = 0, waiting = 0, checkpoint = 0 }
  for i = 1, 30 do
    assert(fairness_host:spawn_task("test.fair_sleep", function(ctx)
      ctx:sleep(0)
      fairness_counts.sleeping = fairness_counts.sleeping + 1
      return true
    end))
    local watchable = {}
    function watchable:watch_luv_readable(on_readable)
      self.on_readable = on_readable
      return function() return true end
    end
    fairness_readables[i] = watchable
    assert(fairness_host:spawn_task("test.fair_wait_read", function(ctx)
      ctx:wait_read(watchable)
      fairness_counts.waiting = fairness_counts.waiting + 1
      return true
    end))
    assert(fairness_host:spawn_task("test.fair_checkpoint", function(ctx)
      ctx:checkpoint()
      fairness_counts.checkpoint = fairness_counts.checkpoint + 1
      return true
    end))
  end
  for _ = 1, 12 do
    assert(fairness_host:_run_background_tasks({ max_resumes = 12 }))
  end
  for _, watchable in ipairs(fairness_readables) do
    if type(watchable.on_readable) == "function" then
      watchable.on_readable()
    end
  end
  for _ = 1, 12 do
    assert(fairness_host:_run_background_tasks({ max_resumes = 12 }))
  end
  if fairness_counts.sleeping ~= 30 or fairness_counts.waiting ~= 30 or fairness_counts.checkpoint ~= 30 then
    return nil, "scheduler should make progress across sleeping, io-waiting, and checkpoint tasks"
  end

  local dial_queue_host = assert(host_mod.new({
    identity = keypair,
    runtime = "luv",
    dial_queue = {
      max_parallel_dials = 1,
    },
  }))
  local direct_started = 0
  local active_direct = 0
  local active_peak = 0
  function dial_queue_host:_dial_direct(target)
    direct_started = direct_started + 1
    active_direct = active_direct + 1
    if active_direct > active_peak then
      active_peak = active_direct
    end
    active_direct = active_direct - 1
    return { peer = target.peer_id }, { remote_peer_id = target.peer_id }
  end
  local peer_a = "12D3KooWCryG7Mon9orvQxcS1rYZjotPgpwoJNHHKcLLfE4Hf5mV"
  local peer_b = "12D3KooWQWZLu9qXWPTDnF9rTRrAiVGZrXCbHAvkqYrsG8cW4UHg"
  local dial_results = {}
  local function spawn_dial(name, peer_id)
    return assert(dial_queue_host:spawn_task(name, function(ctx)
      local dial_conn, _, dial_err = dial_queue_host:dial({
        peer_id = peer_id,
        addrs = { "/ip4/127.0.0.1/tcp/4001/p2p/" .. peer_id },
      }, { ctx = ctx })
      if not dial_conn then
        return nil, dial_err
      end
      dial_results[#dial_results + 1] = peer_id
      return true
    end))
  end
  local dial_a1 = spawn_dial("test.dial_queue_a1", peer_a)
  local dial_a2 = spawn_dial("test.dial_queue_a2", peer_a)
  local dial_b = spawn_dial("test.dial_queue_b", peer_b)
  for _ = 1, 100 do
    assert(dial_queue_host:_poll_once(0.02))
    if dial_a1.status == "completed" and dial_a2.status == "completed" and dial_b.status == "completed" then
      break
    end
  end
  if dial_a1.status ~= "completed" or dial_a2.status ~= "completed" or dial_b.status ~= "completed" then
    return nil, "dial queue tasks should complete"
  end
  if direct_started > 3 then
    return nil, "connection manager should not start more dials than requested"
  end
  if active_peak > 1 then
    return nil, "connection manager should respect max_parallel_dials"
  end
  local dial_stats = dial_queue_host.connection_manager:stats()
  if dial_stats.dialed ~= direct_started then
    return nil, "connection manager stats should track executed dials"
  end

  local full_queue_host = assert(host_mod.new({
    identity = keypair,
    runtime = "luv",
    dial_queue = {
      max_parallel_dials = 0,
      max_dial_queue_length = 1,
    },
  }))
  function full_queue_host:_dial_direct()
    return { close = function() return true end }, {}
  end
  local full_peer_a = peer_a
  local full_peer_b = peer_b
  local queued = assert(full_queue_host:spawn_task("test.full_queue_first", function(ctx)
    return full_queue_host:dial({
      peer_id = full_peer_a,
      addrs = { "/ip4/127.0.0.1/tcp/4001/p2p/" .. full_peer_a },
    }, { ctx = ctx })
  end))
  assert(full_queue_host:_poll_once(0.01))
  if queued.status ~= "waiting_task" and queued.status ~= "ready" then
    return nil, "first queued dial should remain pending when no dial slots are available"
  end
  local queue_full = assert(full_queue_host:spawn_task("test.full_queue_second", function(ctx)
    local dial_conn, _, dial_err = full_queue_host:dial({
      peer_id = full_peer_b,
      addrs = { "/ip4/127.0.0.1/tcp/4001/p2p/" .. full_peer_b },
    }, { ctx = ctx })
    if dial_conn ~= nil or not dial_err or dial_err.kind ~= "resource" then
      return nil, "expected dial queue full resource error"
    end
    return true
  end))
  assert(full_queue_host:_poll_once(0.01))
  if queue_full.status ~= "completed" then
    return nil, "dial queue should reject new dials when queue is full"
  end
  full_queue_host:cancel_task(queued.id)
  assert(host:unsubscribe(task_events))

  return true
end

return {
  name = "host lifecycle events queue and hooks",
  run = run,
}
