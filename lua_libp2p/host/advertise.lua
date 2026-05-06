--- Host advertised address and protocol state.
-- @module lua_libp2p.host.advertise
local log = require("lua_libp2p.log").subsystem("host")
local multiaddr = require("lua_libp2p.multiaddr")
local table_utils = require("lua_libp2p.util.tables")

local M = {}

local list_copy = table_utils.copy_list

local function list_equal(a, b)
  if #a ~= #b then
    return false
  end
  for i = 1, #a do
    if a[i] ~= b[i] then
      return false
    end
  end
  return true
end

local function has_terminal_peer_id(addr)
  local parsed = multiaddr.parse(addr)
  if not parsed or type(parsed.components) ~= "table" or #parsed.components == 0 then
    return false
  end
  local last = parsed.components[#parsed.components]
  return last.protocol == "p2p" and type(last.value) == "string" and last.value ~= ""
end

local function list_protocol_handlers(handlers)
  local out = {}
  for protocol_id in pairs(handlers or {}) do
    out[#out + 1] = protocol_id
  end
  table.sort(out)
  return out
end

function M.init(host)
  host._last_advertised_addrs = nil
  host._last_advertised_protocols = nil
end

function M.install(Host)
  function Host:_list_protocol_handlers()
    return list_protocol_handlers(self._handlers)
  end

  function Host:get_listen_addrs()
    if self.address_manager then
      self.address_manager:set_listen_addrs(self.listen_addrs)
    end
    return list_copy(self.listen_addrs)
  end

  function Host:get_multiaddrs_raw()
    if self.address_manager then
      self.address_manager:set_listen_addrs(self.listen_addrs)
      return self.address_manager:get_advertise_addrs()
    end
    return list_copy(self.listen_addrs)
  end

  function Host:_emit_self_peer_update_if_changed()
    local addrs = self:get_multiaddrs_raw()
    local protocols = self:_list_protocol_handlers()
    if self._last_advertised_addrs
      and self._last_advertised_protocols
      and list_equal(self._last_advertised_addrs, addrs)
      and list_equal(self._last_advertised_protocols, protocols)
    then
      return true
    end

    self._last_advertised_addrs = list_copy(addrs)
    self._last_advertised_protocols = list_copy(protocols)
    log.info("self peer addresses updated", {
      peer_id = self:peer_id().id,
      addrs = #addrs,
      multiaddrs = table.concat(addrs, ","),
      protocols = table.concat(protocols, ","),
    })
    return self:emit("self_peer_update", {
      peer_id = self:peer_id().id,
      addrs = list_copy(addrs),
      protocols = protocols,
    })
  end

  function Host:get_multiaddrs()
    local out = {}
    local pid = self._peer_id and self._peer_id.id or nil
    for _, addr in ipairs(self:get_multiaddrs_raw()) do
      if has_terminal_peer_id(addr) then
        out[#out + 1] = addr
      elseif pid then
        out[#out + 1] = addr .. "/p2p/" .. pid
      else
        out[#out + 1] = addr
      end
    end
    return out
  end
end

return M
