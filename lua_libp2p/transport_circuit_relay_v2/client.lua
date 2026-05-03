--- Circuit Relay v2 client.
-- Creates relay reservations and tracks local reservation state.
-- @module lua_libp2p.transport_circuit_relay_v2.client
local error_mod = require("lua_libp2p.error")
local multiaddr = require("lua_libp2p.multiaddr")
local relay_proto = require("lua_libp2p.transport_circuit_relay_v2.protocol")

local M = {}

local Client = {}
Client.__index = Client

local function copy_list(values)
  local out = {}
  for i, value in ipairs(values or {}) do
    out[i] = value
  end
  return out
end

local function peer_id_from_addr(addr)
  local parsed = multiaddr.parse(addr)
  if not parsed then
    return nil
  end
  for i = #parsed.components, 1, -1 do
    local component = parsed.components[i]
    if component.protocol == "p2p" then
      return component.value
    end
  end
  return nil
end

local function normalize_relay_target(target)
  if type(target) == "string" then
    if target:sub(1, 1) == "/" then
      return {
        peer_id = peer_id_from_addr(target),
        addr = target,
      }
    end
    return { peer_id = target }
  end
  if type(target) == "table" then
    return {
      peer_id = target.peer_id,
      addr = target.addr,
      addrs = target.addrs,
    }
  end
  return nil, error_mod.new("input", "relay target must be peer id, multiaddr, or table")
end

local function relay_dial_target(target)
  if target.addr then
    return { peer_id = target.peer_id, addr = target.addr }
  end
  if target.addrs and target.addrs[1] then
    return { peer_id = target.peer_id, addrs = target.addrs }
  end
  return target.peer_id
end

--- Create a relay reservation via HOP/RESERVE.
-- `opts` supports `stream_opts`, `local_peer_id`, `allow_private_relay_addrs`,
-- and `ignore_addr_errors`.
-- @param target Relay peer id, relay multiaddr, or target table.
-- @tparam[opt] table opts
-- @treturn table|nil reservation
-- @treturn[opt] table err
function Client:reserve(target, opts)
  local options = opts or {}
  local relay_target, target_err = normalize_relay_target(target)
  if not relay_target then
    return nil, target_err
  end
  if type(relay_target.peer_id) ~= "string" or relay_target.peer_id == "" then
    return nil, error_mod.new("input", "relay target must include relay peer id")
  end
  if not self.host or type(self.host.new_stream) ~= "function" then
    return nil, error_mod.new("state", "relay client requires host:new_stream")
  end

  local stream, selected, conn, state_or_err = self.host:new_stream(
    relay_dial_target(relay_target),
    { relay_proto.HOP_ID },
    options.stream_opts
  )
  if not stream then
    return nil, state_or_err
  end

  local reservation, response_or_err = relay_proto.reserve(stream, options)
  if type(stream.close) == "function" then
    pcall(function()
      stream:close()
    end)
  end
  if not reservation then
    return nil, response_or_err
  end

  local local_peer = self.host.peer_id and self.host:peer_id()
  local local_peer_id = options.local_peer_id or (local_peer and local_peer.id)
  if type(local_peer_id) ~= "string" or local_peer_id == "" then
    return nil, error_mod.new("state", "relay reservation requires local peer id")
  end

  local relay_addrs = {}
  for _, addr in ipairs(reservation.addrs or {}) do
    local text = addr
    if type(text) == "string" and text:sub(1, 1) ~= "/" then
      local parsed = multiaddr.from_bytes(text)
      text = parsed and parsed.text or nil
    end
    if type(text) == "string" and text ~= "" then
      if multiaddr.is_private_addr(text) and not options.allow_private_relay_addrs then
        goto continue_addr
      end
      local relayed, relayed_err = multiaddr.relay_destination_addr(text, local_peer_id)
      if relayed then
        relay_addrs[#relay_addrs + 1] = relayed
      elseif not options.ignore_addr_errors then
        return nil, relayed_err
      end
    end
    ::continue_addr::
  end

  if self.host.address_manager then
    for _, addr in ipairs(relay_addrs) do
      self.host.address_manager:add_relay_addr(addr, {
        relay_peer_id = relay_target.peer_id,
        expires = reservation.expire,
      })
    end
    if type(self.host._emit_self_peer_update_if_changed) == "function" then
      local ok, update_err = self.host:_emit_self_peer_update_if_changed()
      if not ok then
        return nil, update_err
      end
    end
  end

  local result = {
    relay_peer_id = relay_target.peer_id,
    reservation = reservation,
    relay_addrs = relay_addrs,
    protocol = selected,
    connection = conn,
    connection_id = state_or_err and state_or_err.connection_id,
    state = state_or_err,
  }
  self._reservations[relay_target.peer_id] = result
  return result
end

--- Reserve all configured relay targets.
-- `opts.relays` overrides default relay list. `opts.fail_fast=true` aborts on first failure.
-- @tparam[opt] table opts
-- @treturn table|nil report
-- @treturn[opt] table err
function Client:reserve_all(opts)
  local options = opts or {}
  local report = {
    attempted = 0,
    reserved = 0,
    failed = 0,
    reservations = {},
    errors = {},
  }
  for _, target in ipairs(options.relays or self.relays) do
    report.attempted = report.attempted + 1
    local reservation, err = self:reserve(target, options)
    if reservation then
      report.reserved = report.reserved + 1
      report.reservations[#report.reservations + 1] = reservation
    else
      report.failed = report.failed + 1
      report.errors[#report.errors + 1] = err
      if options.fail_fast then
        return nil, err
      end
    end
  end
  return report
end

--- Return active reservation list.
-- @treturn table
function Client:get_reservations()
  local out = {}
  for _, reservation in pairs(self._reservations) do
    out[#out + 1] = reservation
  end
  table.sort(out, function(a, b)
    return tostring(a.relay_peer_id) < tostring(b.relay_peer_id)
  end)
  return out
end

--- Remove one reservation from local tracking.
-- @tparam string peer_id Relay peer id.
-- @treturn boolean removed
-- @treturn[opt] table reservation
function Client:remove_reservation(peer_id)
  local reservation = self._reservations[peer_id]
  self._reservations[peer_id] = nil
  return reservation ~= nil, reservation
end

--- Construct a relay client bound to a host.
-- @tparam table host Host instance.
-- @tparam[opt] table opts Client options.
-- `opts.relays` provides default relay targets for @{reserve_all}.
-- @treturn table|nil client
-- @treturn[opt] table err
function M.new(host, opts)
  if type(host) ~= "table" then
    return nil, error_mod.new("input", "relay client requires host")
  end
  local options = opts or {}
  return setmetatable({
    host = host,
    relays = copy_list(options.relays),
    _reservations = {},
  }, Client)
end

M.Client = Client
M.HOP_ID = relay_proto.HOP_ID
M.STOP_ID = relay_proto.STOP_ID

return M
