local function run()
  local env = {}
  setmetatable(env, { __index = _G })

  local chunk, load_err = loadfile("lua-libp2p-0.1.0-1.rockspec", "t", env)
  if not chunk then
    return nil, load_err
  end

  local ok, exec_err = pcall(chunk)
  if not ok then
    return nil, exec_err
  end

  if type(env.build) ~= "table" or type(env.build.modules) ~= "table" then
    return nil, "rockspec missing build.modules table"
  end

  local modules = env.build.modules
  local required = {
    "lua_libp2p.address_manager",
    "lua_libp2p.bootstrap",
    "lua_libp2p.connection_manager",
    "lua_libp2p.host",
    "lua_libp2p.kbucket",
    "lua_libp2p.kad_dht",
    "lua_libp2p.kad_dht.protocol",
    "lua_libp2p.operation",
    "lua_libp2p.peerstore",
    "lua_libp2p.transport_circuit_relay_v2.protocol",
    "lua_libp2p.protocol_identify.protocol",
    "lua_libp2p.protocol_ping.protocol",
    "lua_libp2p.protocol_perf.protocol",
    "lua_libp2p.relay",
    "lua_libp2p.relay.autorelay",
    "lua_libp2p.transport_circuit_relay_v2.client",
    "lua_libp2p.protocol_identify.service",
    "lua_libp2p.protocol_ping.service",
    "lua_libp2p.protocol_perf.service",
    "lua_libp2p.transport_tcp.transport",
    "lua_libp2p.transport_tcp.luv",
    "lua_libp2p.transport_tcp.luv_native",
  }

  for _, module_name in ipairs(required) do
    if type(modules[module_name]) ~= "string" or modules[module_name] == "" then
      return nil, "rockspec is missing module export: " .. module_name
    end
  end

  return true
end

return {
  name = "rockspec exports host service modules",
  run = run,
}
