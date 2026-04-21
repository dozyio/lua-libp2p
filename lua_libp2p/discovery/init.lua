local error_mod = require("lua_libp2p.error")

local M = {}

local Discovery = {}
Discovery.__index = Discovery

local function normalize_source(source)
  if type(source) == "function" then
    return source
  end
  if type(source) == "table" and type(source.discover) == "function" then
    return function(opts)
      return source:discover(opts)
    end
  end
  return nil
end

local function candidate_key(candidate)
  if type(candidate) ~= "table" then
    return nil
  end
  local id = candidate.peer_id or ""
  local addr = ""
  if type(candidate.addrs) == "table" and type(candidate.addrs[1]) == "string" then
    addr = candidate.addrs[1]
  end
  if id == "" and addr == "" then
    return nil
  end
  return id .. "|" .. addr
end

function Discovery:add_source(source)
  local resolved = normalize_source(source)
  if not resolved then
    return nil, error_mod.new("input", "discovery source must be function or { discover = fn }")
  end
  self._sources[#self._sources + 1] = resolved
  return true
end

function Discovery:discover(opts)
  local out = {}
  local seen = {}

  for _, source in ipairs(self._sources) do
    local peers, discover_err = source(opts)
    if not peers then
      if opts and opts.ignore_source_errors then
        goto continue
      end
      return nil, discover_err
    end
    if type(peers) ~= "table" then
      if opts and opts.ignore_source_errors then
        goto continue
      end
      return nil, error_mod.new("input", "discovery source must return list of peers")
    end

    for _, candidate in ipairs(peers) do
      if type(candidate) == "table" and type(candidate.addrs) == "table" and #candidate.addrs > 0 then
        local key = candidate_key(candidate)
        if not key or not seen[key] then
          out[#out + 1] = candidate
          if key then
            seen[key] = true
          end
        end
      end
    end

    ::continue::
  end

  return out
end

function M.new(opts)
  local self_obj = setmetatable({
    _sources = {},
  }, Discovery)

  local options = opts or {}
  if type(options.sources) == "table" then
    for _, source in ipairs(options.sources) do
      local ok, err = self_obj:add_source(source)
      if not ok then
        return nil, err
      end
    end
  end

  return self_obj
end

M.Discovery = Discovery

return M
