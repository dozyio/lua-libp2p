local error_mod = require("lua_libp2p.error")

local M = {}

local Store = {}
Store.__index = Store

local function copy_list(values)
  local out = {}
  for i, value in ipairs(values or {}) do
    out[i] = value
  end
  return out
end

local function ensure_peer(self, peer_id)
  if type(peer_id) ~= "string" or peer_id == "" then
    return nil, error_mod.new("input", "peer id must be a non-empty string")
  end
  local peer = self._peers[peer_id]
  if not peer then
    peer = {
      peer_id = peer_id,
      addrs = {},
      protocols = {},
      metadata = {},
      public_key = nil,
      peer_record_envelope = nil,
      updated_at = os.time(),
    }
    self._peers[peer_id] = peer
  end
  return peer
end

local function addr_expired(addr_entry, now)
  return type(addr_entry.expires_at) == "number" and addr_entry.expires_at <= now
end

local function normalize_addr_ttl(self, opts)
  local options = opts or {}
  local ttl = options.ttl
  if ttl == nil then
    ttl = self.default_addr_ttl
  end
  if ttl == false or ttl == math.huge then
    return nil
  end
  if type(ttl) ~= "number" or ttl <= 0 then
    return nil
  end
  return ttl
end

function Store:add_addrs(peer_id, addrs, opts)
  if type(addrs) ~= "table" then
    return nil, error_mod.new("input", "addrs must be a list")
  end
  local peer, peer_err = ensure_peer(self, peer_id)
  if not peer then
    return nil, peer_err
  end

  local now = os.time()
  local ttl = normalize_addr_ttl(self, opts)
  local expires_at = ttl and (now + ttl) or nil
  local certified = opts and opts.certified == true or false
  local added = 0

  for _, addr in ipairs(addrs) do
    if type(addr) == "string" and addr ~= "" then
      local existing = peer.addrs[addr]
      if not existing then
        added = added + 1
      end
      peer.addrs[addr] = {
        addr = addr,
        expires_at = expires_at,
        certified = certified or (existing and existing.certified) or false,
        last_seen = now,
      }
    end
  end

  peer.updated_at = now
  return added
end

function Store:get_addrs(peer_id)
  local peer = self._peers[peer_id]
  if not peer then
    return {}
  end
  local now = os.time()
  local out = {}
  for addr, entry in pairs(peer.addrs) do
    if addr_expired(entry, now) then
      peer.addrs[addr] = nil
    else
      out[#out + 1] = addr
    end
  end
  table.sort(out)
  return out
end

function Store:add_protocols(peer_id, protocols)
  if type(protocols) ~= "table" then
    return nil, error_mod.new("input", "protocols must be a list")
  end
  local peer, peer_err = ensure_peer(self, peer_id)
  if not peer then
    return nil, peer_err
  end
  local added = 0
  for _, protocol in ipairs(protocols) do
    if type(protocol) == "string" and protocol ~= "" and not peer.protocols[protocol] then
      peer.protocols[protocol] = true
      added = added + 1
    end
  end
  peer.updated_at = os.time()
  return added
end

function Store:get_protocols(peer_id)
  local peer = self._peers[peer_id]
  if not peer then
    return {}
  end
  local out = {}
  for protocol in pairs(peer.protocols) do
    out[#out + 1] = protocol
  end
  table.sort(out)
  return out
end

function Store:supports_protocol(peer_id, protocol)
  local peer = self._peers[peer_id]
  return peer ~= nil and peer.protocols[protocol] == true
end

function Store:merge(peer_id, data, opts)
  local peer, peer_err = ensure_peer(self, peer_id)
  if not peer then
    return nil, peer_err
  end
  local value = data or {}
  if value.addrs then
    local _, addr_err = self:add_addrs(peer_id, value.addrs, opts)
    if addr_err then
      return nil, addr_err
    end
  end
  if value.protocols then
    local _, proto_err = self:add_protocols(peer_id, value.protocols)
    if proto_err then
      return nil, proto_err
    end
  end
  if value.public_key ~= nil then
    peer.public_key = value.public_key
  end
  if value.peer_record_envelope ~= nil then
    peer.peer_record_envelope = value.peer_record_envelope
  end
  if type(value.metadata) == "table" then
    for k, v in pairs(value.metadata) do
      peer.metadata[k] = v
    end
  end
  peer.updated_at = os.time()
  return self:get(peer_id)
end

function Store:patch(peer_id, data, opts)
  local peer, peer_err = ensure_peer(self, peer_id)
  if not peer then
    return nil, peer_err
  end
  local value = data or {}
  if value.addrs ~= nil then
    peer.addrs = {}
    local _, addr_err = self:add_addrs(peer_id, value.addrs, opts)
    if addr_err then
      return nil, addr_err
    end
  end
  if value.protocols ~= nil then
    peer.protocols = {}
    local _, proto_err = self:add_protocols(peer_id, value.protocols)
    if proto_err then
      return nil, proto_err
    end
  end
  if value.public_key ~= nil then
    peer.public_key = value.public_key
  end
  if value.peer_record_envelope ~= nil then
    peer.peer_record_envelope = value.peer_record_envelope
  end
  if value.metadata ~= nil then
    peer.metadata = {}
    for k, v in pairs(value.metadata or {}) do
      peer.metadata[k] = v
    end
  end
  peer.updated_at = os.time()
  return self:get(peer_id)
end

function Store:get(peer_id)
  local peer = self._peers[peer_id]
  if not peer then
    return nil
  end
  return {
    peer_id = peer.peer_id,
    addrs = self:get_addrs(peer_id),
    protocols = self:get_protocols(peer_id),
    public_key = peer.public_key,
    metadata = peer.metadata,
    peer_record_envelope = peer.peer_record_envelope,
    updated_at = peer.updated_at,
  }
end

function Store:has(peer_id)
  return self._peers[peer_id] ~= nil
end

function Store:delete(peer_id)
  local existed = self._peers[peer_id] ~= nil
  self._peers[peer_id] = nil
  return existed
end

function Store:all()
  local out = {}
  for peer_id in pairs(self._peers) do
    out[#out + 1] = self:get(peer_id)
  end
  table.sort(out, function(a, b)
    return a.peer_id < b.peer_id
  end)
  return out
end

function Store:clear_expired()
  local now = os.time()
  local removed = 0
  for _, peer in pairs(self._peers) do
    for addr, entry in pairs(peer.addrs) do
      if addr_expired(entry, now) then
        peer.addrs[addr] = nil
        removed = removed + 1
      end
    end
  end
  return removed
end

function M.new(opts)
  local options = opts or {}
  return setmetatable({
    default_addr_ttl = options.default_addr_ttl or 3600,
    _peers = {},
  }, Store)
end

M.Store = Store

return M
