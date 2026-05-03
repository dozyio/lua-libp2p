local ed25519 = require("lua_libp2p.crypto.ed25519")
local error_mod = require("lua_libp2p.error")
local host_mod = require("lua_libp2p.host")

local function run()
  local h = assert(host_mod.new({
    identity = assert(ed25519.generate_keypair()),
    runtime = "luv",
    listen_addrs = {},
  }))

  local child = assert(h:spawn_task("child", function()
    return "child-result"
  end))
  local parent = assert(h:spawn_task("parent", function(ctx)
    local result, err = ctx:await_task(child)
    if not result then
      return nil, err
    end
    return result .. ":parent"
  end))
  assert(h:_run_background_tasks({ max_resumes = 4 }))
  if child.status ~= "completed" or parent.status ~= "completed" or parent.result ~= "child-result:parent" then
    return nil, "await_task should resume parent after child completes"
  end

  local fatal = assert(h:spawn_task("fatal", function()
    return nil, error_mod.new("io", "forced failure")
  end))
  local waiter = assert(h:spawn_task("waiter", function(ctx)
    local ok, err = ctx:await_task(fatal)
    if ok or not err then
      return nil, error_mod.new("state", "expected child failure")
    end
    return "saw-failure"
  end))
  assert(h:_run_background_tasks({ max_resumes = 4 }))
  if fatal.status ~= "failed" or waiter.status ~= "completed" or waiter.result ~= "saw-failure" then
    return nil, "await_task should surface failed child task"
  end

  local closed = false
  local handler = assert(h:_spawn_handler_task(function()
    return nil, error_mod.new("closed", "nonfatal handler close")
  end, {
    stream = {},
    protocol = "/tests/handler/1.0.0",
    connection = { close = function() closed = true return true end },
  }))
  assert(h:_run_background_tasks({ max_resumes = 1 }))
  if handler.status ~= "completed" or not closed then
    return nil, "handler task should absorb nonfatal stream errors and close connection"
  end

  return true
end

return {
  name = "host task scheduler",
  run = run,
}
