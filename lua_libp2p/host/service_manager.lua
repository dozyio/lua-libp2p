--- Host service graph construction and dependency ordering.
---@class Libp2pServiceSpec
---@field module table Service module with `new(host, config, name)`.
---@field config? table Service-specific configuration passed to `module.new`.
---@field provides? string[] Capabilities provided by this service.
---@field requires? string[] Capabilities required before this service starts.

---@alias Libp2pServicesConfig table<string, Libp2pServiceSpec|table>

local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log").subsystem("host")

local M = {}

local function normalize_capability_list(values, default_value)
  local out = {}
  local source = values
  if source == nil then
    source = { default_value }
  end
  if type(source) ~= "table" then
    return nil, error_mod.new("input", "service capability metadata must be a list")
  end
  local seen = {}
  for _, capability in ipairs(source) do
    if type(capability) ~= "string" or capability == "" then
      return nil, error_mod.new("input", "service capability names must be non-empty strings")
    end
    if not seen[capability] then
      seen[capability] = true
      out[#out + 1] = capability
    end
  end
  return out
end

local function set_service_alias(host, service_name, instance)
  if service_name == "autorelay" then
    host.autorelay = instance
  elseif service_name == "autonat" then
    host.autonat = instance
  elseif service_name == "upnp_nat" then
    host.upnp_nat = instance
  elseif service_name == "kad_dht" then
    host.kad_dht = instance
  end
end

local function provider_started(service_defs, provider_name)
  for _, candidate in ipairs(service_defs) do
    if candidate.name == provider_name then
      return candidate.started == true
    end
  end
  return false
end

local function build_service_defs(host, services)
  local service_defs = {}
  local capability_providers = {}

  for service_name, service_spec in pairs(services) do
    if type(service_name) ~= "string" or service_name == "" then
      return nil, nil, error_mod.new("input", "service names must be non-empty strings")
    end

    local module = service_spec
    local service_opts = {}
    local service_provides = nil
    local service_requires = nil
    if type(service_spec) == "table" and service_spec.module ~= nil then
      module = service_spec.module
      service_opts = service_spec.config or {}
      service_provides = service_spec.provides
      service_requires = service_spec.requires
    end
    if type(module) ~= "table" or type(module.new) ~= "function" then
      return nil,
        nil,
        error_mod.new("input", "service entry must be module with .new(host, config)", {
          service = service_name,
        })
    end

    local provides, provides_err = normalize_capability_list(service_provides or module.provides, service_name)
    if not provides then
      return nil,
        nil,
        error_mod.wrap("input", "invalid service provides metadata", provides_err, {
          service = service_name,
        })
    end
    local requires, requires_err = normalize_capability_list(service_requires or module.requires, nil)
    if not requires then
      return nil,
        nil,
        error_mod.wrap("input", "invalid service requires metadata", requires_err, {
          service = service_name,
        })
    end

    local instance, new_err = module.new(host, service_opts, service_name)
    if not instance then
      return nil, nil, new_err
    end
    log.debug("host service constructed", {
      service = service_name,
      provides = table.concat(provides, ","),
      requires = table.concat(requires, ","),
    })

    local def = {
      name = service_name,
      module = module,
      opts = service_opts,
      instance = instance,
      provides = provides,
      requires = requires,
      started = false,
    }
    service_defs[#service_defs + 1] = def

    for _, capability in ipairs(provides) do
      local existing = capability_providers[capability]
      if existing and existing ~= service_name then
        return nil,
          nil,
          error_mod.new("input", "duplicate service capability provider", {
            capability = capability,
            existing_service = existing,
            conflicting_service = service_name,
          })
      end
      capability_providers[capability] = service_name
      host.components[capability] = instance
      host.capabilities[capability] = instance
    end

    host._service_options[service_name] = service_opts
    host.services[service_name] = instance
    host._services[service_name] = instance
    set_service_alias(host, service_name, instance)
  end

  return service_defs, capability_providers
end

local function validate_dependencies(service_defs, capability_providers)
  for _, def in ipairs(service_defs) do
    for _, required in ipairs(def.requires) do
      if capability_providers[required] == nil then
        return nil,
          error_mod.new("state", "service dependency is missing", {
            service = def.name,
            required_capability = required,
          })
      end
    end
  end
  return true
end

local function start_services(host, service_defs, capability_providers)
  local started_count = 0
  host._service_order = {}
  while started_count < #service_defs do
    local progressed = false
    for _, def in ipairs(service_defs) do
      if not def.started then
        local can_start = true
        for _, required in ipairs(def.requires) do
          local provider_name = capability_providers[required]
          if provider_name ~= def.name and not provider_started(service_defs, provider_name) then
            can_start = false
            break
          end
        end

        if can_start then
          log.debug("host service starting", {
            service = def.name,
          })
          if type(def.instance.start) == "function" then
            local started, start_err = def.instance:start()
            if not started then
              log.debug("host service start failed", {
                service = def.name,
                cause = tostring(start_err),
              })
              return nil, start_err
            end
          end
          def.started = true
          started_count = started_count + 1
          host._service_order[#host._service_order + 1] = def.name
          log.debug("host service started", {
            service = def.name,
          })
          progressed = true
        end
      end
    end

    if not progressed then
      local blocked = {}
      for _, def in ipairs(service_defs) do
        if not def.started then
          blocked[#blocked + 1] = def.name
        end
      end
      return nil,
        error_mod.new("state", "service dependency cycle or unresolved dependency", {
          blocked_services = blocked,
        })
    end
  end

  return true
end

local function expose_capabilities(host, capability_providers)
  for capability, provider_name in pairs(capability_providers) do
    local provider = host._services[provider_name]
    if host[capability] == nil then
      host[capability] = provider
    end
  end
end

function M.install(host, services)
  if services == nil then
    return true
  end
  if type(services) ~= "table" then
    return nil, error_mod.new("input", "services must be a map")
  end

  local service_defs, capability_providers, build_err = build_service_defs(host, services)
  if not service_defs then
    return nil, build_err
  end

  local deps_ok, deps_err = validate_dependencies(service_defs, capability_providers)
  if not deps_ok then
    return nil, deps_err
  end

  local started, start_err = start_services(host, service_defs, capability_providers)
  if not started then
    return nil, start_err
  end

  expose_capabilities(host, capability_providers)
  return true
end

function M.on_host_started(host)
  local seen = {}
  for _, service_name in ipairs(host._service_order or {}) do
    local service = host._services and host._services[service_name]
    seen[service_name] = true
    if service and type(service.on_host_started) == "function" then
      log.debug("host service on_host_started", {
        service = service_name,
      })
      local ok, err = service:on_host_started()
      if not ok then
        return nil, err
      end
    end
  end
  for service_name, service in pairs(host._services or {}) do
    if not seen[service_name] and type(service.on_host_started) == "function" then
      local ok, err = service:on_host_started()
      if not ok then
        return nil, err
      end
    end
  end
  return true
end

function M.stop(host)
  local stopped = {}
  for i = #(host._service_order or {}), 1, -1 do
    local service_name = host._service_order[i]
    local service = host._services and host._services[service_name]
    stopped[service_name] = true
    if service and type(service.stop) == "function" then
      log.debug("host service stopping", {
        service = service_name,
      })
      local ok, err = service:stop()
      if not ok then
        return nil, err
      end
    end
  end
  for service_name, service in pairs(host._services or {}) do
    if not stopped[service_name] and type(service.stop) == "function" then
      local ok, err = service:stop()
      if not ok then
        return nil, err
      end
    end
  end
  return true
end

return M
