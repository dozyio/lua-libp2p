local kad_dht = require("lua_libp2p.kad_dht")

local function fake_hash(value)
  local map = {
    ["local"] = string.char(0) .. string.rep("\0", 31),
    ["peer-a"] = string.char(128) .. string.rep("\0", 31),
  }
  return map[value] or (string.char(255) .. string.rep("\0", 31))
end

local function run()
  local handlers = {}
  local emitted = {}
  local spawned = {}
  local cancelled_task_id = nil
  local next_task_id = 1

  local host = {}

  function host:on(name, fn)
    handlers[name] = handlers[name] or {}
    handlers[name][#handlers[name] + 1] = fn
    return true
  end

  function host:off(name, fn)
    local list = handlers[name]
    if not list then
      return false
    end
    for i = 1, #list do
      if list[i] == fn then
        table.remove(list, i)
        return true
      end
    end
    return false
  end

  function host:emit(name, payload)
    emitted[#emitted + 1] = { name = name, payload = payload }
    return true
  end

  function host:spawn_task(name, fn, opts)
    local task = {
      id = next_task_id,
      name = name,
      status = "ready",
      fn = fn,
      opts = opts,
    }
    next_task_id = next_task_id + 1
    spawned[#spawned + 1] = task
    return task
  end

  function host:cancel_task(task_id)
    cancelled_task_id = task_id
    for _, task in ipairs(spawned) do
      if task.id == task_id then
        task.status = "cancelled"
      end
    end
    return true
  end

  local dht, dht_err = kad_dht.new(host, {
    local_peer_id = "local",
    hash_function = fake_hash,
    maintenance_enabled = true,
    maintenance_interval_seconds = 600,
    maintenance_startup_retry_seconds = 5,
    maintenance_startup_retry_max_seconds = 30,
    maintenance_walk_every = 1,
  })
  if not dht then
    return nil, dht_err
  end

  local refresh_calls = 0
  local walk_calls = 0

  dht.refresh_once = function(_, opts)
    refresh_calls = refresh_calls + 1
    if not opts or opts.max_checks == nil then
      return nil, "expected maintenance refresh options"
    end
    return { checked = 1, healthy = 1, removed = 0, skipped = 0, errors = {} }
  end

  dht.random_walk = function(_, _)
    walk_calls = walk_calls + 1
    return {
      result = function()
        return { queried = 1, responses = 1, added = 0, discovered = 0 }
      end,
    }
  end

  local started, start_err = dht:start()
  if not started then
    return nil, start_err
  end
  if #spawned == 0 or spawned[1].name ~= "kad.maintenance" then
    return nil, "expected maintenance task to be spawned"
  end
  if not dht._maintenance_task or dht._maintenance_task.id ~= spawned[1].id then
    return nil, "expected dht to track maintenance task"
  end
  local maintenance_fn = spawned[1].fn
  local sleeps = {}
  local ok, loop_err = maintenance_fn({
    sleep = function(_, seconds)
      sleeps[#sleeps + 1] = seconds
      dht._running = false
      return true
    end,
  })
  if not ok then
    return nil, loop_err
  end
  if refresh_calls == 0 then
    return nil, "expected maintenance loop to run refresh_once"
  end
  if walk_calls ~= 0 then
    return nil, "maintenance should skip random walk while routing table is empty"
  end
  if sleeps[1] ~= 5 then
    return nil, "expected startup retry interval before first successful walk"
  end

  dht._maintenance_running = true
  dht._running = true
  local suppressed, suppressed_err = dht:add_peer("peer-a", { skip_kad_protocol_filter = true })
  dht._maintenance_running = false
  if not suppressed then
    return nil, suppressed_err
  end
  if #spawned ~= 1 then
    return nil, "maintenance trigger should not spawn while maintenance is running"
  end

  dht._running = true
  local triggered, trigger_err = dht:add_peer("peer-b", { skip_kad_protocol_filter = true })
  if not triggered then
    return nil, trigger_err
  end
  if #spawned ~= 1 then
    return nil, "maintenance trigger should only run when the first routing-table peer is added"
  end

  local trigger_dht, trigger_dht_err = kad_dht.new(host, {
    local_peer_id = "local",
    hash_function = fake_hash,
    maintenance_enabled = true,
  })
  if not trigger_dht then
    return nil, trigger_dht_err
  end
  trigger_dht._running = true
  trigger_dht.refresh_once = dht.refresh_once
  trigger_dht.random_walk = dht.random_walk
  local trigger_peer_added, trigger_peer_err = trigger_dht:add_peer("peer-a", { skip_kad_protocol_filter = true })
  if not trigger_peer_added then
    return nil, trigger_peer_err
  end
  if #spawned ~= 2 or spawned[2].name ~= "kad.maintenance.trigger" then
    return nil, "expected first routing-table peer to trigger maintenance"
  end
  local trigger_ok, trigger_run_err = spawned[2].fn({})
  if not trigger_ok then
    return nil, trigger_run_err
  end
  if walk_calls == 0 then
    return nil, "expected triggered maintenance to run random walk after first peer"
  end

  dht._running = true
  sleeps = {}
  ok, loop_err = maintenance_fn({
    sleep = function(_, seconds)
      sleeps[#sleeps + 1] = seconds
      dht._running = false
      return true
    end,
  })
  if not ok then
    return nil, loop_err
  end
  if sleeps[1] ~= 600 then
    return nil, "expected normal interval after successful startup walk"
  end

  local saw_refresh_event = false
  local saw_walk_event = false
  for _, event in ipairs(emitted) do
    if event.name == "kad_dht:maintenance:refresh" then
      saw_refresh_event = true
    elseif event.name == "kad_dht:maintenance:walk" then
      saw_walk_event = true
    end
  end
  if not saw_refresh_event then
    return nil, "expected maintenance refresh event"
  end
  if not saw_walk_event then
    return nil, "expected maintenance walk event"
  end

  local stopped, stop_err = dht:stop()
  if not stopped then
    return nil, stop_err
  end
  if spawned[1].status ~= "cancelled" then
    return nil, "expected maintenance task cancellation on stop"
  end
  if cancelled_task_id ~= spawned[1].id then
    return nil, "expected stop to cancel tracked maintenance task"
  end
  trigger_dht:stop()

  local backoff_dht, backoff_dht_err = kad_dht.new(host, {
    local_peer_id = "local",
    hash_function = fake_hash,
    maintenance_enabled = true,
    maintenance_interval_seconds = 600,
    maintenance_startup_retry_seconds = 5,
    maintenance_startup_retry_max_seconds = 30,
    maintenance_walk_every = 1,
  })
  if not backoff_dht then
    return nil, backoff_dht_err
  end
  backoff_dht.refresh_once = dht.refresh_once
  local backoff_responses = 0
  backoff_dht.random_walk = function(_, _)
    return {
      result = function()
        return { queried = 1, responses = backoff_responses, added = 0, discovered = 0 }
      end,
    }
  end
  local backoff_peer_added, backoff_peer_err = backoff_dht:add_peer("peer-a", { skip_kad_protocol_filter = true })
  if not backoff_peer_added then
    return nil, backoff_peer_err
  end
  local backoff_started, backoff_start_err = backoff_dht:start()
  if not backoff_started then
    return nil, backoff_start_err
  end
  local backoff_task = spawned[#spawned]
  local backoff_sleeps = {}
  ok, loop_err = backoff_task.fn({
    sleep = function(_, seconds)
      backoff_sleeps[#backoff_sleeps + 1] = seconds
      if #backoff_sleeps == 4 then
        backoff_dht._running = false
      end
      return true
    end,
  })
  if not ok then
    return nil, loop_err
  end
  if table.concat(backoff_sleeps, ",") ~= "5,10,20,30" then
    return nil, "expected startup maintenance backoff to double and cap at 30 seconds"
  end
  backoff_responses = 1
  backoff_dht._running = true
  backoff_sleeps = {}
  ok, loop_err = backoff_task.fn({
    sleep = function(_, seconds)
      backoff_sleeps[#backoff_sleeps + 1] = seconds
      backoff_dht._running = false
      return true
    end,
  })
  if not ok then
    return nil, loop_err
  end
  if backoff_sleeps[1] ~= 600 then
    return nil, "expected maintenance to use normal interval after successful startup walk"
  end
  backoff_dht:stop()

  local dht_disabled, disabled_err = kad_dht.new(host, {
    local_peer_id = "local",
    hash_function = fake_hash,
    maintenance_enabled = false,
  })
  if not dht_disabled then
    return nil, disabled_err
  end
  local spawned_before = #spawned
  local disabled_started, disabled_start_err = dht_disabled:start()
  if not disabled_started then
    return nil, disabled_start_err
  end
  if #spawned ~= spawned_before then
    return nil, "maintenance should not spawn when disabled"
  end
  dht_disabled:stop()

  return true
end

return {
  name = "kad-dht maintenance loop lifecycle",
  run = run,
}
