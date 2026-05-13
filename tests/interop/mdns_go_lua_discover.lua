package.path = table.concat({
  "./?.lua",
  "./?/init.lua",
  package.path,
}, ";")

local host_mod = require("lua_libp2p.host")
local mdns = require("lua_libp2p.peer_discovery_mdns")

local expected_peer = arg[1]
if type(expected_peer) ~= "string" or expected_peer == "" then
  io.stderr:write("usage: lua tests/interop/mdns_go_lua_discover.lua <go-peer-id> [lua-peer-file]\n")
  os.exit(2)
end
local lua_peer_file = arg[2]

local ok_luv, uv = pcall(require, "luv")
if not ok_luv then
  io.stderr:write("luv is required for mdns interop\n")
  os.exit(2)
end

local h, h_err = host_mod.new({
  runtime = "luv",
  blocking = false,
  listen_addrs = { "/ip4/127.0.0.1/tcp/0" },
  services = {
    mdns = {
      module = mdns,
      config = {
        interval = 0.5,
        peer_name = "lua-mdns-interop",
      },
    },
  },
})
if not h then
  io.stderr:write(tostring(h_err) .. "\n")
  os.exit(1)
end

if type(lua_peer_file) == "string" and lua_peer_file ~= "" then
  local f, file_err = io.open(lua_peer_file, "wb")
  if not f then
    io.stderr:write(tostring(file_err) .. "\n")
    os.exit(1)
  end
  f:write(h:peer_id().id .. "\n")
  f:close()
end

local sub, sub_err = h:subscribe("peer_discovered", { max_queue = 128 })
if not sub then
  io.stderr:write(tostring(sub_err) .. "\n")
  os.exit(1)
end

local started, start_err = h:start()
if not started then
  io.stderr:write(tostring(start_err) .. "\n")
  os.exit(1)
end

local found = false
local failure = nil
local timeout

local poll = assert(uv.new_timer())
poll:start(20, 20, function()
  while true do
    local event, event_err = h:next_event(sub)
    if event_err then
      failure = tostring(event_err)
      break
    end
    if not event then
      break
    end
    if event.payload and event.payload.peer_id == expected_peer then
      local addrs = event.payload.addrs or {}
      if #addrs == 0 then
        failure = "go mdns peer discovered without addresses"
      else
        found = true
      end
      break
    end
  end
  if found or failure then
    poll:stop()
    poll:close()
    if timeout then
      timeout:stop()
      timeout:close()
    end
    h:stop()
  end
end)

timeout = assert(uv.new_timer())
timeout:start(8000, 0, function()
  if found or failure then
    return
  end
  failure = "timed out waiting for go mdns peer " .. expected_peer
  timeout:stop()
  timeout:close()
  pcall(function()
    poll:stop()
    poll:close()
  end)
  h:stop()
end)

uv.run("default")

if failure then
  io.stderr:write(failure .. "\n")
  os.exit(1)
end
if not found then
  io.stderr:write("mdns interop did not complete\n")
  os.exit(1)
end

print("ok")
