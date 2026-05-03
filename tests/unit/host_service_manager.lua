local ed25519 = require("lua_libp2p.crypto.ed25519")
local host_mod = require("lua_libp2p.host")

local function service_module(name, provides, requires, events)
  return {
    provides = provides,
    requires = requires,
    new = function()
      return {
        start = function()
          events[#events + 1] = "start:" .. name
          return true
        end,
        on_host_started = function()
          events[#events + 1] = "started:" .. name
          return true
        end,
      }
    end,
  }
end

local function contains_in_order(events, first, second)
  local first_index = nil
  local second_index = nil
  for i, event in ipairs(events) do
    if event == first then
      first_index = i
    elseif event == second then
      second_index = i
    end
  end
  return first_index ~= nil and second_index ~= nil and first_index < second_index
end

local function run()
  local keypair = assert(ed25519.generate_keypair())
  local events = {}
  local h, err = host_mod.new({
    identity = keypair,
    runtime = "luv",
    listen_addrs = {},
    services = {
      dependent = { module = service_module("dependent", { "dependent" }, { "base" }, events) },
      base = { module = service_module("base", { "base" }, nil, events) },
    },
  })
  if not h then
    return nil, err
  end
  if h.components.base ~= h.services.base or h.capabilities.dependent ~= h.services.dependent then
    return nil, "service capabilities should be exposed"
  end
  if not contains_in_order(events, "start:base", "start:dependent") then
    return nil, "service manager should start dependencies first"
  end
  local started_ok, started_err = h:start()
  if not started_ok then
    return nil, started_err
  end
  h:stop()
  if not contains_in_order(events, "started:base", "started:dependent") then
    return nil, "on_host_started should follow service start order"
  end

  local missing, missing_err = host_mod.new({
    identity = assert(ed25519.generate_keypair()),
    runtime = "luv",
    services = {
      dependent = { module = service_module("dependent", { "dependent" }, { "missing" }, {}) },
    },
  })
  if missing ~= nil or not missing_err or missing_err.kind ~= "state" then
    return nil, "missing service dependency should fail host construction"
  end

  local duplicate, duplicate_err = host_mod.new({
    identity = assert(ed25519.generate_keypair()),
    runtime = "luv",
    services = {
      a = { module = service_module("a", { "same" }, nil, {}) },
      b = { module = service_module("b", { "same" }, nil, {}) },
    },
  })
  if duplicate ~= nil or not duplicate_err or duplicate_err.kind ~= "input" then
    return nil, "duplicate service capability should fail host construction"
  end

  return true
end

return {
  name = "host service manager boundaries",
  run = run,
}
