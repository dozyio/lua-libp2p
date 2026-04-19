local ed25519 = require("lua_libp2p.crypto.ed25519")
local error_mod = require("lua_libp2p.error")
local host_mod = require("lua_libp2p.host")
local socket = require("socket")

local NONFATAL_KINDS = { "timeout", "closed", "decode", "protocol", "unsupported" }

local function new_host()
  local keypair = assert(ed25519.generate_keypair())
  local h, err = host_mod.new({
    identity = keypair,
    listen_addrs = {},
  })
  if not h then
    error(err)
  end
  return h
end

local function run_inner()
  for _, kind in ipairs(NONFATAL_KINDS) do
    local h = new_host()
    local closed = false
    local conn = {
      process_one = function()
        return nil, error_mod.new(kind, "nonfatal process error")
      end,
      accept_stream = function()
        return nil
      end,
      close = function()
        closed = true
        return true
      end,
    }
    h._connections = { { conn = conn, state = { remote_peer_id = "peer-a" } } }
    h._connections_by_peer["peer-a"] = h._connections[1]

    local ok, err = h:poll_once(0)
    if not ok then
      return nil, string.format("expected nonfatal process error '%s' to continue: %s", kind, tostring(err))
    end

    if kind == "timeout" then
      if closed then
        return nil, "timeout process error should not close connection"
      end
      if #h._connections ~= 1 then
        return nil, "timeout process error should keep connection"
      end
    else
      if not closed then
        return nil, string.format("process error '%s' should close connection", kind)
      end
      if #h._connections ~= 0 then
        return nil, string.format("process error '%s' should remove connection", kind)
      end
    end
  end

  for _, kind in ipairs(NONFATAL_KINDS) do
    local h = new_host()
    local conn = {
      process_one = function()
        return true
      end,
      accept_stream = function()
        return nil, nil, nil, error_mod.new(kind, "nonfatal stream error")
      end,
      close = function()
        return true
      end,
    }
    h._connections = { { conn = conn, state = { remote_peer_id = "peer-b" } } }
    h._connections_by_peer["peer-b"] = h._connections[1]

    local ok, err = h:poll_once(0)
    if not ok then
      return nil, string.format("expected nonfatal stream error '%s' to continue: %s", kind, tostring(err))
    end
    if #h._connections ~= 1 then
      return nil, string.format("nonfatal stream error '%s' should keep connection", kind)
    end
  end

  for _, kind in ipairs(NONFATAL_KINDS) do
    local h = new_host()
    local closed = false
    h:_spawn_handler_task(function()
      return nil, error_mod.new(kind, "nonfatal handler task error")
    end, {
      stream = {},
      protocol = "/test/1.0.0",
      connection = {
        close = function()
          closed = true
          return true
        end,
      },
      state = {},
    })

    local ok, err = h:_run_handler_tasks()
    if not ok then
      return nil, string.format("expected nonfatal handler error '%s' to continue: %s", kind, tostring(err))
    end
    if not closed then
      return nil, string.format("nonfatal handler error '%s' should close connection", kind)
    end
    if #h._handler_tasks ~= 0 then
      return nil, "handler task should be removed after completion"
    end
  end

  do
    local h = new_host()
    h:_spawn_handler_task(function()
      return nil, error_mod.new("io", "fatal handler task error")
    end, {
      stream = {},
      protocol = "/test/1.0.0",
      connection = { close = function() return true end },
      state = {},
    })

    local ok, err = h:_run_handler_tasks()
    if ok or not err then
      return nil, "expected fatal handler task error to fail host"
    end
  end

  return true
end

local function run()
  local original_select = socket.select
  socket.select = nil
  local ok, a, b = pcall(run_inner)
  socket.select = original_select
  if not ok then
    return nil, a
  end
  return a, b
end

return {
  name = "host nonfatal error handling",
  run = run,
}
