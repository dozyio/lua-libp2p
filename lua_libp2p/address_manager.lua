local M = {}

local AddressManager = {}
AddressManager.__index = AddressManager

local function copy_list(values)
  local out = {}
  for i, value in ipairs(values or {}) do
    out[i] = value
  end
  return out
end

local function add_unique(out, seen, addr)
  if type(addr) ~= "string" or addr == "" or seen[addr] then
    return
  end
  seen[addr] = true
  out[#out + 1] = addr
end

local function as_set(values)
  local out = {}
  for _, value in ipairs(values or {}) do
    if type(value) == "string" and value ~= "" then
      out[value] = true
    end
  end
  return out
end

function AddressManager:set_listen_addrs(addrs)
  self._listen_addrs = copy_list(addrs)
  return true
end

function AddressManager:get_listen_addrs()
  return copy_list(self._listen_addrs)
end

function AddressManager:set_announce_addrs(addrs)
  self._announce_addrs = copy_list(addrs)
  return true
end

function AddressManager:add_announce_addr(addr)
  add_unique(self._announce_addrs, as_set(self._announce_addrs), addr)
  return true
end

function AddressManager:set_no_announce_addrs(addrs)
  self._no_announce_addrs = copy_list(addrs)
  return true
end

function AddressManager:add_observed_addr(addr)
  add_unique(self._observed_addrs, as_set(self._observed_addrs), addr)
  return true
end

function AddressManager:get_observed_addrs()
  return copy_list(self._observed_addrs)
end

function AddressManager:add_relay_addr(addr)
  add_unique(self._relay_addrs, as_set(self._relay_addrs), addr)
  return true
end

function AddressManager:remove_relay_addr(addr)
  for i = #self._relay_addrs, 1, -1 do
    if self._relay_addrs[i] == addr then
      table.remove(self._relay_addrs, i)
      return true
    end
  end
  return false
end

function AddressManager:remove_relay_addrs(addrs)
  local removed = 0
  for _, addr in ipairs(addrs or {}) do
    if self:remove_relay_addr(addr) then
      removed = removed + 1
    end
  end
  return removed
end

function AddressManager:get_relay_addrs()
  return copy_list(self._relay_addrs)
end

function AddressManager:get_advertise_addrs()
  local out = {}
  local seen = {}
  local blocked = as_set(self._no_announce_addrs)
  local sources = {}
  if #self._announce_addrs > 0 then
    sources[#sources + 1] = self._announce_addrs
  else
    sources[#sources + 1] = self._listen_addrs
    if self._advertise_observed then
      sources[#sources + 1] = self._observed_addrs
    end
  end
  sources[#sources + 1] = self._relay_addrs

  for _, list in ipairs(sources) do
    for _, addr in ipairs(list) do
      if not blocked[addr] then
        add_unique(out, seen, addr)
      end
    end
  end
  return out
end

function M.new(opts)
  local options = opts or {}
  return setmetatable({
    _listen_addrs = copy_list(options.listen_addrs),
    _announce_addrs = copy_list(options.announce_addrs),
    _no_announce_addrs = copy_list(options.no_announce_addrs),
    _observed_addrs = copy_list(options.observed_addrs),
    _relay_addrs = copy_list(options.relay_addrs),
    _advertise_observed = options.advertise_observed == true,
  }, AddressManager)
end

M.AddressManager = AddressManager

return M
