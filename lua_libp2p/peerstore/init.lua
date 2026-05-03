--- Peerstore.
-- @module lua_libp2p.peerstore
local error_mod = require("lua_libp2p.error")

local M = {}

local Store = {}
Store.__index = Store

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
      tags = {},
      metadata = {},
      public_key = nil,
      peer_record_envelope = nil,
      updated_at = os.time(),
    }
    self._peers[peer_id] = peer
  end
  return peer
end

local function tag_expired(tag, now)
  return type(tag.expires_at) == "number" and tag.expires_at <= now
end

local function normalize_tag_ttl(ttl)
  if ttl == false or ttl == math.huge then
    return nil
  end
  if ttl == nil then
    return 120
  end
  if type(ttl) ~= "number" or ttl <= 0 then
    return nil
  end
  return ttl
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

--- Add peer addresses.
-- `opts` supports `ttl` (seconds|false) and `certified` (boolean).
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

--- Get non-expired addresses for a peer.
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

--- Add supported protocol IDs for a peer.
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

--- Get sorted protocol IDs for a peer.
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

--- Check whether a peer supports a protocol.
function Store:supports_protocol(peer_id, protocol)
  local peer = self._peers[peer_id]
  return peer ~= nil and peer.protocols[protocol] == true
end

--- Add or update a peer tag.
-- `opts` supports `value` and `ttl`.
function Store:tag(peer_id, name, opts)
  if type(name) ~= "string" or name == "" then
    return nil, error_mod.new("input", "tag name must be non-empty")
  end
  local peer, peer_err = ensure_peer(self, peer_id)
  if not peer then
    return nil, peer_err
  end
  local options = opts or {}
  local ttl = normalize_tag_ttl(options.ttl)
  peer.tags[name] = {
    name = name,
    value = options.value or 0,
    expires_at = ttl and (os.time() + ttl) or nil,
  }
  peer.updated_at = os.time()
  return true
end

--- Get active peer tags.
function Store:get_tags(peer_id)
  local peer = self._peers[peer_id]
  if not peer then
    return {}
  end
  local now = os.time()
  local out = {}
  for name, tag in pairs(peer.tags) do
    if tag_expired(tag, now) then
      peer.tags[name] = nil
    else
      out[name] = {
        name = tag.name,
        value = tag.value,
        expires_at = tag.expires_at,
      }
    end
  end
  return out
end

--- Remove a peer tag.
function Store:untag(peer_id, name)
  if type(name) ~= "string" or name == "" then
    return nil, error_mod.new("input", "tag name must be non-empty")
  end
  local peer = self._peers[peer_id]
  if not peer then
    return false
  end
  local existed = peer.tags[name] ~= nil
  peer.tags[name] = nil
  if existed then
    peer.updated_at = os.time()
  end
  return existed
end

--- Merge fields into an existing peer record.
-- `opts` is forwarded to address merge helpers.
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

--- Replace selected peer fields.
-- `opts` is forwarded to address patch helpers.
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

--- Get full peer view.
function Store:get(peer_id)
  local peer = self._peers[peer_id]
  if not peer then
    return nil
  end
  return {
    peer_id = peer.peer_id,
    addrs = self:get_addrs(peer_id),
    protocols = self:get_protocols(peer_id),
    tags = self:get_tags(peer_id),
    public_key = peer.public_key,
    metadata = peer.metadata,
    peer_record_envelope = peer.peer_record_envelope,
    updated_at = peer.updated_at,
  }
end

--- Check if a peer exists.
function Store:has(peer_id)
  return self._peers[peer_id] ~= nil
end

--- Delete a peer.
function Store:delete(peer_id)
  local existed = self._peers[peer_id] ~= nil
  self._peers[peer_id] = nil
  return existed
end

--- List all peers.
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

--- Remove expired addresses and tags across peers.
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
    for name, tag in pairs(peer.tags) do
      if tag_expired(tag, now) then
        peer.tags[name] = nil
        removed = removed + 1
      end
    end
  end
  return removed
end

--- Construct a peerstore instance.
-- Defaults to the datastore-backed implementation with an in-memory datastore.
-- `opts.default_addr_ttl` controls default address TTL in seconds.
-- `opts.datastore` supplies a custom datastore backend.
-- @tparam[opt] table opts
-- @treturn table store
function M.new(opts)
  local options = opts or {}
  local datastore_store = require("lua_libp2p.peerstore.datastore")
  if options.datastore == nil then
    local memory_datastore = require("lua_libp2p.datastore.memory")
    options.datastore = memory_datastore.new()
  end
  return datastore_store.new(options)
end

--- Construct the old direct in-memory peerstore implementation.
-- Prefer @{new}; this helper exists for focused implementation tests.
function M.new_memory_direct(opts)
  local options = opts or {}
  return setmetatable({
    default_addr_ttl = options.default_addr_ttl or 3600,
    _peers = {},
  }, Store)
end

M.Store = Store

return M
