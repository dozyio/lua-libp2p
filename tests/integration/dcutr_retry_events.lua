local ed25519 = require("lua_libp2p.crypto.ed25519")
local host_mod = require("lua_libp2p.host")
local dcutr_service = require("lua_libp2p.protocol_dcutr.service")
local identify_service = require("lua_libp2p.protocol_identify.service")

local function run()
  local keypair = assert(ed25519.generate_keypair())
  local host, host_err = host_mod.new({
    runtime = "poll",
    identity = keypair,
    services = {
      identify = { module = identify_service },
      dcutr = { module = dcutr_service, config = { auto_on_relay_connection = false } },
    },
    blocking = false,
  })
  if not host then
    return nil, host_err
  end

  local original_new_stream = host.new_stream
  local new_stream_calls = 0
  host.new_stream = function()
    new_stream_calls = new_stream_calls + 1
    return nil, nil, nil, "forced dcutr stream open failure"
  end

  local failed_sub = assert(host:subscribe("dcutr:attempt:failed"))
  local retry_sub = assert(host:subscribe("dcutr:attempt:retry"))

  local task, task_err = host.dcutr:start_hole_punch("peer-retry", {
    max_attempts = 3,
    retry_delay_seconds = 0,
  })
  if not task then
    host.new_stream = original_new_stream
    return nil, task_err
  end

  local result, run_err = host:run_until_task(task, { poll_interval = 0 })
  host.new_stream = original_new_stream
  if result ~= nil then
    return nil, "expected dcutr retry task to fail"
  end
  if not run_err then
    return nil, "expected dcutr retry task error"
  end
  if new_stream_calls ~= 3 then
    return nil, "expected dcutr retry to attempt stream open max_attempts times"
  end

  local retry_events = 0
  while true do
    local ev = host:next_event(retry_sub)
    if not ev then
      break
    end
    retry_events = retry_events + 1
  end
  if retry_events ~= 2 then
    return nil, "expected dcutr retry event count to be max_attempts-1"
  end

  local failed_events = 0
  local saw_reason = false
  while true do
    local ev = host:next_event(failed_sub)
    if not ev then
      break
    end
    failed_events = failed_events + 1
    if ev.payload and ev.payload.reason == dcutr_service.FAILURE_REASON.STREAM_OPEN_FAILED then
      saw_reason = true
    end
  end
  if failed_events == 0 then
    return nil, "expected dcutr attempt failed events"
  end
  if not saw_reason then
    return nil, "expected dcutr failure reason to include stream_open_failed"
  end

  return true
end

return {
  name = "dcutr retry emits reasoned events",
  run = run,
}
