local ed25519 = require("lua_libp2p.crypto.ed25519")
local host = require("lua_libp2p.host")

local function run()
  local keypair, key_err = ed25519.generate_keypair()
  if not keypair then
    return nil, key_err
  end

  local h, host_err = host.new({
    identity = keypair,
    blocking = false,
  })
  if not h then
    return nil, host_err
  end

  h._poll_once = function(_, timeout)
    return true, timeout
  end
  h._process_runtime_events = function(_, timeout, ready_map)
    return true, ready_map or timeout
  end
  h._run_background_tasks = function(_, opts)
    return true, opts
  end

  local enabled, enable_err = h:enable_perf_stats()
  if not enabled then
    return nil, enable_err
  end

  local poll_ok, poll_err = h:_poll_once(0.01)
  if not poll_ok then
    return nil, poll_err
  end
  local process_ok, process_err = h:_process_runtime_events(0, {})
  if not process_ok then
    return nil, process_err
  end
  local background_ok, background_err = h:_run_background_tasks({})
  if not background_ok then
    return nil, background_err
  end

  local stats = h:perf_stats()
  if type(stats) ~= "table" then
    return nil, "expected perf stats after enabling"
  end
  if stats.poll_once_calls ~= 1 then
    return nil, "expected poll_once perf call count"
  end
  if stats.process_events_calls ~= 1 then
    return nil, "expected process_events perf call count"
  end
  if stats.background_calls ~= 1 then
    return nil, "expected background perf call count"
  end

  stats.kad_rpc_dispatch_by_type = { FIND_NODE = { calls = 1, ms = 2 } }
  h._debug_perf.kad_rpc_dispatch_by_type = { GET_PROVIDERS = { calls = 3, ms = 4 } }
  local reset_snapshot = h:perf_stats({ reset = true })
  if reset_snapshot.kad_rpc_dispatch_by_type.GET_PROVIDERS.calls ~= 3 then
    return nil, "expected perf stats to copy grouped counters"
  end
  local reset_stats = h:perf_stats()
  if reset_stats.poll_once_calls ~= 0 or reset_stats.kad_rpc_dispatch_by_type.GET_PROVIDERS ~= nil then
    return nil, "expected perf stats reset"
  end

  return true
end

return {
  name = "host perf stats",
  run = run,
}
