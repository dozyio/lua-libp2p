--- Cross-platform route and neighbor discovery facade.
---@class Libp2pOsRoutingOptions
---@field platform? 'macos'|'linux'|'windows'

---@class Libp2pOsRoute
---@field gateway? string
---@field interface? string

---@class Libp2pOsRoutingSnapshot
---@field platform string
---@field default_route_v4? Libp2pOsRoute
---@field default_route_v6? Libp2pOsRoute
---@field neighbors_v6? table[]
---@field router_candidates_v6? table[]

local error_mod = require("lua_libp2p.error")

local macos = require("lua_libp2p.os_routing.macos")
local linux = require("lua_libp2p.os_routing.linux")
local windows = require("lua_libp2p.os_routing.windows")

local M = {}

local function detect_platform()
  local ok_jit, jit_mod = pcall(function()
    return _G.jit
  end)
  if ok_jit and jit_mod and type(jit_mod.os) == "string" then
    local os_name = jit_mod.os:lower()
    if os_name == "osx" then
      return "macos"
    elseif os_name == "linux" then
      return "linux"
    elseif os_name == "windows" then
      return "windows"
    end
  end
  local dir_sep = package.config:sub(1, 1)
  if dir_sep == "\\" then
    return "windows"
  end
  local uname = io.popen("uname -s 2>/dev/null")
  if uname then
    local value = (uname:read("*a") or ""):lower()
    uname:close()
    if value:find("darwin", 1, true) then
      return "macos"
    elseif value:find("linux", 1, true) then
      return "linux"
    end
  end
  return "unknown"
end

local function adapter_for_platform(platform)
  if platform == "macos" then
    return macos
  elseif platform == "linux" then
    return linux
  elseif platform == "windows" then
    return windows
  end
  return nil
end

---@param opts? Libp2pOsRoutingOptions
---@return Libp2pOsRoutingSnapshot|nil snapshot
---@return table|nil err
function M.snapshot(opts)
  local options = opts or {}
  local platform = options.platform or detect_platform()
  local adapter = adapter_for_platform(platform)
  if not adapter then
    return nil, error_mod.new("unsupported", "os_routing platform is not supported", { platform = platform })
  end
  return adapter.snapshot(options)
end

---@param opts? Libp2pOsRoutingOptions
---@return Libp2pOsRoute|nil route
---@return table|nil err
function M.default_route_v4(opts)
  local snapshot, snapshot_err = M.snapshot(opts)
  if not snapshot then
    return nil, snapshot_err
  end
  if not snapshot.default_route_v4 then
    return nil, snapshot.default_route_v4_error or error_mod.new("state", "ipv4 default route not found")
  end
  return snapshot.default_route_v4
end

---@param opts? Libp2pOsRoutingOptions
---@return Libp2pOsRoute|nil route
---@return table|nil err
function M.default_route_v6(opts)
  local snapshot, snapshot_err = M.snapshot(opts)
  if not snapshot then
    return nil, snapshot_err
  end
  if not snapshot.default_route_v6 then
    return nil, snapshot.default_route_v6_error or error_mod.new("state", "ipv6 default route not found")
  end
  return snapshot.default_route_v6
end

---@param opts? Libp2pOsRoutingOptions
---@return table[]|nil neighbors
---@return table|nil err
function M.neighbors_v6(opts)
  local snapshot, snapshot_err = M.snapshot(opts)
  if not snapshot then
    return nil, snapshot_err
  end
  return snapshot.neighbors_v6 or {}
end

---@param opts? Libp2pOsRoutingOptions
---@return table[]|nil candidates
---@return table|nil err
function M.router_candidates_v6(opts)
  local snapshot, snapshot_err = M.snapshot(opts)
  if not snapshot then
    return nil, snapshot_err
  end
  return snapshot.router_candidates_v6 or {}
end

M.detect_platform = detect_platform

return M
