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
    "lua_libp2p.autonat",
    "lua_libp2p.autonat.client",
    "lua_libp2p.autonat.server",
    "lua_libp2p.bootstrap",
    "lua_libp2p.connection_encrypter",
    "lua_libp2p.connection_manager",
    "lua_libp2p.datastore",
    "lua_libp2p.datastore.memory",
    "lua_libp2p.datastore.sqlite",
    "lua_libp2p.host",
    "lua_libp2p.host.advertise",
    "lua_libp2p.host.address_policy",
    "lua_libp2p.host.bootstrap",
    "lua_libp2p.host.connections",
    "lua_libp2p.host.dialer",
    "lua_libp2p.host.events",
    "lua_libp2p.host.identify",
    "lua_libp2p.host.listeners",
    "lua_libp2p.host.perf",
    "lua_libp2p.host.protocols",
    "lua_libp2p.host.service_manager",
    "lua_libp2p.host.runtime_luv",
    "lua_libp2p.host.runtime_luv_native",
    "lua_libp2p.host.tasks",
    "lua_libp2p.kad_dht.kbucket",
    "lua_libp2p.kad_dht",
    "lua_libp2p.kad_dht.bootstrap",
    "lua_libp2p.kad_dht.maintenance",
    "lua_libp2p.kad_dht.provider_routing",
    "lua_libp2p.kad_dht.protocol",
    "lua_libp2p.kad_dht.query",
    "lua_libp2p.kad_dht.random_walk",
    "lua_libp2p.kad_dht.providers",
    "lua_libp2p.kad_dht.record_validators",
    "lua_libp2p.kad_dht.records",
    "lua_libp2p.kad_dht.reprovider",
    "lua_libp2p.kad_dht.values",
    "lua_libp2p.operation",
    "lua_libp2p.os_routing",
    "lua_libp2p.os_routing.macos",
    "lua_libp2p.os_routing.linux",
    "lua_libp2p.os_routing.windows",
    "lua_libp2p.peerstore",
    "lua_libp2p.peerstore.codec",
    "lua_libp2p.peerstore.datastore",
    "lua_libp2p.relay_discovery",
    "lua_libp2p.resource_manager",
    "lua_libp2p.util.tables",
    "lua_libp2p.transport_circuit_relay_v2.protocol",
    "lua_libp2p.protocol_identify.protocol",
    "lua_libp2p.protocol.autonat_v1",
    "lua_libp2p.protocol.autonat_v2",
    "lua_libp2p.protocol_ping.protocol",
    "lua_libp2p.protocol_perf.protocol",
    "lua_libp2p.transport_circuit_relay_v2.autorelay",
    "lua_libp2p.transport_circuit_relay_v2.client",
    "lua_libp2p.protocol_identify.service",
    "lua_libp2p.protocol_ping.service",
    "lua_libp2p.protocol_perf.service",
    "lua_libp2p.transport_tcp.transport",
    "lua_libp2p.transport_tcp.luv",
    "lua_libp2p.transport_tcp.luv_native",
    "lua_libp2p.connection_encrypter_tls.protocol",
    "lua_libp2p.connection_encrypter_tls.verification",
    "lua_libp2p.nat_pmp.client",
    "lua_libp2p.nat_pmp.service",
    "lua_libp2p.pcp.client",
    "lua_libp2p.pcp.service",
  }

  for _, module_name in ipairs(required) do
    local path = modules[module_name]
    if type(path) ~= "string" or path == "" then
      return nil, "rockspec is missing module export: " .. module_name
    end

    local file = io.open(path, "r")
    if not file then
      return nil, "rockspec module path does not exist: " .. module_name .. " -> " .. path
    end
    file:close()

    local require_ok, require_err = pcall(require, module_name)
    if not require_ok then
      return nil, "rockspec module is not requireable: " .. module_name .. ": " .. tostring(require_err)
    end
  end

  return true
end

return {
  name = "rockspec exports host service modules",
  run = run,
}
