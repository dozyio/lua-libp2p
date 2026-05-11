--- Security transport registry.
-- Keeps protocol IDs centralized while loading implementations only when used.
-- @module lua_libp2p.connection_encrypter
local error_mod = require("lua_libp2p.error")

local M = {}

M.NOISE = "/noise"
M.TLS = "/tls/1.0.0"
M.PLAINTEXT = "/plaintext/2.0.0"

local registry = {
  [M.NOISE] = {
    load = function()
      return require("lua_libp2p.connection_encrypter_noise.protocol")
    end,
  },
  [M.TLS] = {
    load = function()
      return require("lua_libp2p.connection_encrypter_tls.protocol")
    end,
  },
  [M.PLAINTEXT] = {
    load = function()
      return require("lua_libp2p.connection_encrypter_plaintext.protocol")
    end,
  },
}

function M.default_protocols()
  return { M.NOISE }
end

function M.protocol_ids()
  return { M.NOISE, M.TLS, M.PLAINTEXT }
end

function M.load(protocol_id)
  local entry = registry[protocol_id]
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
