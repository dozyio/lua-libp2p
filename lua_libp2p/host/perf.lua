--- Optional host performance counters.
local M = {}

local PERF_KEYS = {
  "poll_sync_watchers",
  "poll_uv_wait",
  "poll_process_events",
  "process_ready_waiters",
  "process_pending_relay_inbound",
  "process_pending_inbound",
  "process_listeners",
  "process_router",
  "process_connections",
  "process_background",
  "background_sleep_scan",
  "background_resume",
  "background_prune",
  "background_resume_dequeue",
  "background_resume_skip",
  "background_resume_cancel",
  "background_resume_coroutine",
  "background_resume_yield",
  "background_resume_completed",
  "background_resume_failed",
  "kad_rpc_read",
  "kad_rpc_dispatch",
  "kad_rpc_write",
  "kad_rpc_close_write",
  "kad_closest_lookup",
  "kad_closest_parse_target",
  "kad_closest_build",
  "kad_closest_peer_record",
  "kad_peer_record_peer_bytes",
  "kad_peer_record_get_addrs",
  "kad_peer_record_filter_addrs",
  "kad_peer_record_addr_cache_hit",
  "kad_random_walk_seed_build",
  "kad_random_walk_apply_results",
  "kad_query_seed_enqueue",
  "kad_query_fill_tasks",
  "kad_query_reap_tasks",
  "kad_query_strict_complete",
  "kad_query_running_tasks",
  "kad_query_final_sort",
}

local PERF_GROUP_KEYS = {
  "background_resume_by_name",
  "background_resume_by_service",
  "kad_rpc_dispatch_by_type",
}

local function now_seconds()
  local ok_socket, socket = pcall(require, "socket")
  if ok_socket and type(socket.gettime) == "function" then
    return socket.gettime()
  end
  return os.time()
end

local function ensure_perf(host)
  local perf = rawget(host, "_debug_perf")
  if type(perf) ~= "table" then
    perf = {}
    rawset(host, "_debug_perf", perf)
  end
  return perf
end

local function add_counter(perf, key, elapsed_seconds)
  perf[key .. "_calls"] = (perf[key .. "_calls"] or 0) + 1
  perf[key .. "_ms"] = (perf[key .. "_ms"] or 0) + (elapsed_seconds * 1000)
end

local function copy_perf(input)
  local out = {}
  for key, value in pairs(input or {}) do
    if type(value) == "table" then
      local group = {}
      for group_key, group_value in pairs(value) do
        if type(group_value) == "table" then
          local item = {}
          for item_key, item_value in pairs(group_value) do
            item[item_key] = item_value
          end
          group[group_key] = item
        else
          group[group_key] = group_value
        end
      end
      out[key] = group
    else
      out[key] = value
    end
  end
  return out
end

local function reset_perf(perf)
  perf.poll_once_calls = 0
  perf.poll_once_ms = 0
  perf.process_events_calls = 0
  perf.process_events_ms = 0
  perf.background_calls = 0
  perf.background_ms = 0
  for _, key in ipairs(PERF_KEYS) do
    perf[key .. "_calls"] = 0
    perf[key .. "_ms"] = 0
  end
  for _, key in ipairs(PERF_GROUP_KEYS) do
    perf[key] = {}
  end
end

local function wrap_method(host, originals, name, metric_key)
  local original = host[name]
  if type(original) ~= "function" or originals[name] ~= nil then
    return
  end
  originals[name] = original
  host[name] = function(self, ...)
    local started = now_seconds()
    local ok, err = original(self, ...)
    local perf = rawget(self, "_debug_perf")
    if type(perf) == "table" then
      add_counter(perf, metric_key, now_seconds() - started)
    end
    return ok, err
  end
end

function M.install(Host)
  function Host:enable_perf_stats()
    local perf = ensure_perf(self)
    reset_perf(perf)
    local originals = rawget(self, "_perf_originals")
    if type(originals) ~= "table" then
      originals = {}
      rawset(self, "_perf_originals", originals)
    end
    wrap_method(self, originals, "_poll_once", "poll_once")
    wrap_method(self, originals, "_process_runtime_events", "process_events")
    wrap_method(self, originals, "_run_background_tasks", "background")
    return true
  end

  function Host:perf_stats(opts)
    local perf = rawget(self, "_debug_perf")
    if type(perf) ~= "table" then
      return nil
    end
    local snapshot = copy_perf(perf)
    if opts and opts.reset == true then
      reset_perf(perf)
    end
    return snapshot
  end

  function Host:reset_perf_stats()
    reset_perf(ensure_perf(self))
    return true
  end
end

return M
