--- Security transport registry.
-- Keeps protocol IDs centralized while loading implementations only when used.
---@class Libp2pSecurityTransportConfig
---@field noise? boolean Enable Noise (`/noise`). Default: true.
---@field tls? boolean Enable experimental libp2p TLS (`/tls/1.0.0`). Requires `fd_tls` for luv-native TLS.
---@field plaintext? boolean Enable plaintext (`/plaintext/2.0.0`). Intended for tests only.
local error_mod = require("lua_libp2p.error")

local M = {}

M.NOISE = "/noise"
M.TLS = "/tls/1.0.0"
M.PLAINTEXT = "/plaintext/2.0.0"

local order = { "noise", "tls", "plaintext" }

local registry = {
  noise = {
    protocol_id = M.NOISE,
    load = function()
      return require("lua_libp2p.connection_encrypter_noise.protocol")
    end,
  },
  tls = {
    protocol_id = M.TLS,
    load = function()
      return require("lua_libp2p.connection_encrypter_tls.protocol")
    end,
  },
  plaintext = {
    protocol_id = M.PLAINTEXT,
    load = function()
      return require("lua_libp2p.connection_encrypter_plaintext.protocol")
    end,
  },
}

local by_protocol = {}
for name, entry in pairs(registry) do
  by_protocol[entry.protocol_id] = entry
  entry.name = name
end

function M.default_protocols()
  return { M.NOISE }
end

function M.protocol_ids()
  return { M.NOISE, M.TLS, M.PLAINTEXT }
end

function M.normalize_protocols(spec)
  if spec == nil then
    return M.default_protocols()
  end
  if type(spec) ~= "table" then
    return nil, error_mod.new("input", "security_transports must be a map")
  end

  if #spec > 0 then
    return nil, error_mod.new("input", "security_transports protocol ID lists are not supported; use a map")
  end

  local out = {}
  for _, name in ipairs(order) do
    local transport_spec = spec[name]
    if transport_spec ~= nil and transport_spec ~= false then
      local entry = registry[name]
      out[#out + 1] = entry.protocol_id
    end
  end
  for name in pairs(spec) do
    if registry[name] == nil then
      return nil,
        error_mod.new("input", "unknown security transport", {
          transport = name,
        })
    end
  end
  if #out == 0 then
    return nil, error_mod.new("input", "security_transports must enable at least one transport")
  end
  return out
end

function M.load(protocol_id)
  local entry = by_protocol[protocol_id]
  if not entry then
    return nil,
      error_mod.new("unsupported", "security protocol not supported", {
        protocol = protocol_id,
      })
  end
  local ok, mod = pcall(entry.load)
  if not ok then
    return nil,
      error_mod.new("unsupported", "security protocol implementation failed to load", {
        protocol = protocol_id,
        cause = mod,
      })
  end
  return mod
end

return M
