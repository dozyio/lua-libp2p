local host_mod = require("lua_libp2p.host")

local M = {
  name = "host scheduler hot paths",
}

local function elapsed_seconds(fn)
  collectgarbage("collect")
  local started = os.clock()
  fn()
  return os.clock() - started
end

local function print_result(name, iterations, elapsed)
  local per_iter_ms = iterations > 0 and (elapsed * 1000 / iterations) or 0
  io.stdout:write(
    string.format(
      "  %-34s iterations=%d total_ms=%.2f per_iter_ms=%.4f\n",
      name,
      iterations,
      elapsed * 1000,
      per_iter_ms
    )
  )
end

local function new_host()
  return assert(host_mod.new({
    runtime = "luv",
    listen_addrs = {},
    task_retention = -1,
  }))
end

local function bench_task_queue_resume()
  local h = new_host()
  local task_count = 10000
  for i = 1, task_count do
    assert(h:spawn_task("bench.ready", function()
      return i
    end))
  end

  local elapsed = elapsed_seconds(function()
    assert(h:_run_background_tasks({ max_resumes = task_count }))
  end)
  if h:_task_queue_depth() ~= 0 then
    return nil, "expected task queue to be empty"
  end
  print_result("task_queue_resume", task_count, elapsed)
  return true
end

local function bench_sleep_scan_with_retained_tasks()
  local h = new_host()
  local retained_tasks = 3000
  for i = 1, retained_tasks do
    assert(h:spawn_task("bench.completed", function()
      return i
    end))
  end
  assert(h:_run_background_tasks({ max_resumes = retained_tasks }))

  local sleeping_tasks = 20
  for _ = 1, sleeping_tasks do
    assert(h:spawn_task("bench.sleeping", function(ctx)
      return ctx:sleep(3600)
    end))
  end
  assert(h:_run_background_tasks({ max_resumes = sleeping_tasks }))

  local iterations = 5000
  local elapsed = elapsed_seconds(function()
    for _ = 1, iterations do
      assert(h:_run_background_tasks({ max_resumes = 1 }))
    end
  end)
  print_result("sleep_scan_retained_tasks", iterations, elapsed)
  return true
end

function M.run()
  local ok, err = bench_task_queue_resume()
  if not ok then
    return nil, err
  end
  ok, err = bench_sleep_scan_with_retained_tasks()
  if not ok then
    return nil, err
  end
  return true
end

return M
