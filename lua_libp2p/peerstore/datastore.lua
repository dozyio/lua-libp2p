--- Datastore-backed peerstore.
-- Stores one logical peer record per datastore key.
-- Uses the synchronous datastore contract; choose local/fast backends here.
-- Remote or potentially blocking stores should use a future async peerstore
-- adapter rather than this implementation.
local datastore = require("lua_libp2p.datastore")
local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log").subsystem("peerstore")
local peerstore_codec = require("lua_libp2p.peerstore.codec")

local M = {}

local Store = {}
Store.__index = Store

local DEFAULT_PREFIX = "peerstore/peers"

local function now_seconds()
  local ok_socket, socket = pcall(require, "socket")
  if ok_socket and type(socket.gettime) == "function" then
    return socket.gettime()
  end
  return os.time()
end

local function clone(value)
  if type(value) ~= "table" then
    return value
  end
  local out = {}
  for k, v in pairs(value) do
    out[k] = clone(v)
  end
  return out
end

local function peer_key(self, peer_id)
  return datastore.key(self._prefix, peer_id)
end

local function validate_peer_id(peer_id)
  if type(peer_id) ~= "string" or peer_id == "" then
    return nil, error_mod.new("input", "peer id must be a non-empty string")
  end
  return true
end

local function tag_expired(tag, now)
  return type(tag.expires_at) == "number" and tag.expires_at <= now
end

local function addr_expired(addr_entry, now)
  return type(addr_entry.expires_at) == "number" and addr_entry.expires_at <= now
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

local function normalize_peer(peer_id, peer)
  local value = type(peer) == "table" and clone(peer) or {}
  value.peer_id = peer_id
  value.addrs = type(value.addrs) == "table" and value.addrs or {}
  value.protocols = type(value.protocols) == "table" and value.protocols or {}
  value.tags = type(value.tags) == "table" and value.tags or {}
  value.metadata = type(value.metadata) == "table" and value.metadata or {}
  value.updated_at = type(value.updated_at) == "number" and value.updated_at or os.time()
  return value
end

local function load_peer(self, peer_id)
  local ok, peer_err = validate_peer_id(peer_id)
  if not ok then
    return nil, peer_err
  end
  local key, key_err = peer_key(self, peer_id)
  if not key then
    return nil, key_err
  end
  local peer_bytes, get_err = self._datastore:get(key)
  if get_err then
    return nil, get_err
  end
  if peer_bytes == nil then
    return nil
  end
  local peer, decode_err = peerstore_codec.decode(peer_bytes)
  if not peer then
    return nil, decode_err
  end
  return normalize_peer(peer_id, peer)
end

local function ensure_peer(self, peer_id)
  local peer, err = load_peer(self, peer_id)
  if err then
    return nil, err
  end
  if peer then
    return peer
  end
  return normalize_peer(peer_id, nil)
end

local function save_peer(self, peer)
  local key, key_err = peer_key(self, peer.peer_id)
  if not key then
    return nil, key_err
  end
  local encoded, encode_err = peerstore_codec.encode(peer)
  if not encoded then
    return nil, encode_err
  end
  return self._datastore:put(key, encoded)
end

local function active_addrs(peer)
  local now = os.time()
  local changed = false
  local out = {}
  for addr, entry in pairs(peer.addrs) do
    if addr_expired(entry, now) then
      peer.addrs[addr] = nil
      changed = true
    else
      out[#out + 1] = addr
    end
  end
  table.sort(out)
  return out, changed
end

local function active_tags(peer)
  local now = os.time()
  local changed = false
  local out = {}
  for name, tag in pairs(peer.tags) do
    if tag_expired(tag, now) then
      peer.tags[name] = nil
      changed = true
    else
      out[name] = {
        name = tag.name,
        value = tag.value,
        expires_at = tag.expires_at,
      }
    end
  end
  return out, changed
end

local function peer_view_from_record(self, peer_id, encoded)
  local peer, decode_err = peerstore_codec.decode(encoded)
  if not peer then
    return nil, decode_err
  end
  peer = normalize_peer(peer_id, peer)
  local addrs, addrs_changed = active_addrs(peer)
  local tags, tags_changed = active_tags(peer)
  if addrs_changed or tags_changed then
    save_peer(self, peer)
  end
  local protocols = {}
  for protocol in pairs(peer.protocols) do
    protocols[#protocols + 1] = protocol
  end
  table.sort(protocols)
  return {
    peer_id = peer.peer_id,
    addrs = addrs,
    protocols = protocols,
    tags = tags,
    public_key = peer.public_key,
    metadata = clone(peer.metadata),
    peer_record_envelope = peer.peer_record_envelope,
    updated_at = peer.updated_at,
  }
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
  local saved, save_err = save_peer(self, peer)
  if not saved then
    return nil, save_err
  end
  return added
end

function Store:get_addrs(peer_id)
  local peer, err = load_peer(self, peer_id)
  if err or not peer then
    return {}
  end
  local out, changed = active_addrs(peer)
  if changed then
    save_peer(self, peer)
  end
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
  local saved, save_err = save_peer(self, peer)
  if not saved then
    return nil, save_err
  end
  return added
end

function Store:get_protocols(peer_id)
  local peer = load_peer(self, peer_id)
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
  local peer = load_peer(self, peer_id)
  return peer ~= nil and peer.protocols[protocol] == true
end

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
  return save_peer(self, peer)
end

function Store:get_tags(peer_id)
  local peer, err = load_peer(self, peer_id)
  if err or not peer then
    return {}
  end
  local out, changed = active_tags(peer)
  if changed then
    save_peer(self, peer)
  end
  return out
end

function Store:untag(peer_id, name)
  if type(name) ~= "string" or name == "" then
    return nil, error_mod.new("input", "tag name must be non-empty")
  end
  local peer, err = load_peer(self, peer_id)
  if err then
    return nil, err
  end
  if not peer then
    return false
  end
  local existed = peer.tags[name] ~= nil
  peer.tags[name] = nil
  if existed then
    peer.updated_at = os.time()
    local saved, save_err = save_peer(self, peer)
    if not saved then
      return nil, save_err
    end
  end
  return existed
end

function Store:merge(peer_id, data, opts)
  local peer, peer_err = ensure_peer(self, peer_id)
  if not peer then
    return nil, peer_err
  end
  local value = data or {}
  if value.addrs then
    local now = os.time()
    local ttl = normalize_addr_ttl(self, opts)
    local expires_at = ttl and (now + ttl) or nil
    local certified = opts and opts.certified == true or false
    for _, addr in ipairs(value.addrs) do
      if type(addr) == "string" and addr ~= "" then
        local existing = peer.addrs[addr]
        peer.addrs[addr] = {
          addr = addr,
          expires_at = expires_at,
          certified = certified or (existing and existing.certified) or false,
          last_seen = now,
        }
      end
    end
  end
  if value.protocols then
    for _, protocol in ipairs(value.protocols) do
      if type(protocol) == "string" and protocol ~= "" then
        peer.protocols[protocol] = true
      end
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
  local saved, save_err = save_peer(self, peer)
  if not saved then
    return nil, save_err
  end
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
    local now = os.time()
    local ttl = normalize_addr_ttl(self, opts)
    local expires_at = ttl and (now + ttl) or nil
    local certified = opts and opts.certified == true or false
    for _, addr in ipairs(value.addrs) do
      if type(addr) == "string" and addr ~= "" then
        peer.addrs[addr] = {
          addr = addr,
          expires_at = expires_at,
          certified = certified,
          last_seen = now,
        }
      end
    end
  end
  if value.protocols ~= nil then
    peer.protocols = {}
    for _, protocol in ipairs(value.protocols) do
      if type(protocol) == "string" and protocol ~= "" then
        peer.protocols[protocol] = true
      end
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
  local saved, save_err = save_peer(self, peer)
  if not saved then
    return nil, save_err
  end
  return self:get(peer_id)
end

function Store:get(peer_id)
  local peer, err = load_peer(self, peer_id)
  if err or not peer then
    return nil
  end
  local addrs, addrs_changed = active_addrs(peer)
  local tags, tags_changed = active_tags(peer)
  if addrs_changed or tags_changed then
    save_peer(self, peer)
  end
  local protocols = {}
  for protocol in pairs(peer.protocols) do
    protocols[#protocols + 1] = protocol
  end
  table.sort(protocols)
  return {
    peer_id = peer.peer_id,
    addrs = addrs,
    protocols = protocols,
    tags = tags,
    public_key = peer.public_key,
    metadata = clone(peer.metadata),
    peer_record_envelope = peer.peer_record_envelope,
    updated_at = peer.updated_at,
  }
end

function Store:has(peer_id)
  local peer = load_peer(self, peer_id)
  return peer ~= nil
end

function Store:delete(peer_id)
  local ok, peer_err = validate_peer_id(peer_id)
  if not ok then
    return nil, peer_err
  end
  local key, key_err = peer_key(self, peer_id)
  if not key then
    return nil, key_err
  end
  return self._datastore:delete(key)
end

function Store:all()
  local started_at = now_seconds()
  log.debug("peerstore all start", { prefix = self._prefix })
  if type(self.each) == "function" then
    local out = {}
    local ok, each_err = self:each(function(peer)
      out[#out + 1] = peer
      return true
    end)
    if not ok then
      return nil, each_err
    end
    log.debug("peerstore all done", {
      prefix = self._prefix,
      peers = #out,
      elapsed_ms = string.format("%.1f", (now_seconds() - started_at) * 1000),
    })
    return out
  end

  local keys, list_err = self._datastore:list(self._prefix)
  if not keys then
    return nil, list_err
  end
  log.debug("peerstore all keys loaded", {
    prefix = self._prefix,
    keys = #keys,
    elapsed_ms = string.format("%.1f", (now_seconds() - started_at) * 1000),
  })
  local out = {}
  local prefix_len = #self._prefix + 2
  for _, key in ipairs(keys) do
    local peer_id = key:sub(prefix_len)
    local peer = self:get(peer_id)
    if peer then
      out[#out + 1] = peer
    end
  end
  table.sort(out, function(a, b)
    return a.peer_id < b.peer_id
  end)
  log.debug("peerstore all done", {
    prefix = self._prefix,
    keys = #keys,
    peers = #out,
    elapsed_ms = string.format("%.1f", (now_seconds() - started_at) * 1000),
  })
  return out
end

function Store:each(fn, opts)
  if type(fn) ~= "function" then
    return nil, error_mod.new("input", "peerstore each requires a callback")
  end
  local options = opts or {}
  local started_at = now_seconds()
  log.debug("peerstore each start", { prefix = self._prefix, limit = options.limit })
  local query_fn = self._datastore and self._datastore.query
  if type(query_fn) ~= "function" then
    local keys, list_err = self._datastore:list(self._prefix)
    if not keys then
      return nil, list_err
    end
    local prefix_len = #self._prefix + 2
    for _, key in ipairs(keys) do
      local peer = self:get(key:sub(prefix_len))
      if peer then
        local keep_going, cb_err = fn(peer)
        if cb_err then
          return nil, cb_err
        end
        if keep_going == false then
          break
        end
      end
    end
    return true
  end

  local results, query_err = self._datastore:query({
    prefix = self._prefix,
    keys_only = false,
    limit = options.limit,
    offset = options.offset,
  })
  if not results then
    return nil, query_err
  end
  local prefix_len = #self._prefix + 2
  local scanned = 0
  local yielded = 0
  while true do
    local entry, entry_err = results:next()
    if entry_err then
      results:close()
      return nil, entry_err
    end
    if not entry then
      break
    end
    scanned = scanned + 1
    local peer_id = entry.key:sub(prefix_len)
    local peer, peer_err = peer_view_from_record(self, peer_id, entry.value)
    if peer_err then
      results:close()
      return nil, peer_err
    end
    if peer then
      yielded = yielded + 1
      local keep_going, cb_err = fn(peer)
      if cb_err then
        results:close()
        return nil, cb_err
      end
      if keep_going == false then
        break
      end
    end
  end
  results:close()
  log.debug("peerstore each done", {
    prefix = self._prefix,
    scanned = scanned,
    yielded = yielded,
    elapsed_ms = string.format("%.1f", (now_seconds() - started_at) * 1000),
  })
  return true
end

function Store:count()
  local started_at = now_seconds()
  if type(self._datastore.count) == "function" then
    local count, count_err = self._datastore:count(self._prefix)
    log.debug("peerstore count done", {
      prefix = self._prefix,
      count = count or 0,
      elapsed_ms = string.format("%.1f", (now_seconds() - started_at) * 1000),
    })
    return count, count_err
  end
  local keys, list_err = self._datastore:list(self._prefix)
  if not keys then
    return nil, list_err
  end
  log.debug("peerstore count done", {
    prefix = self._prefix,
    count = #keys,
    elapsed_ms = string.format("%.1f", (now_seconds() - started_at) * 1000),
  })
  return #keys
end

function Store:clear_expired()
  local peers, peers_err = self:all()
  if not peers then
    return nil, peers_err
  end
  local removed = 0
  local now = os.time()
  for _, peer_view in ipairs(peers) do
    local peer = load_peer(self, peer_view.peer_id)
    if peer then
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
      if removed > 0 then
        save_peer(self, peer)
      end
    end
  end
  return removed
end

function M.new(opts)
  local options = opts or {}
  local store, store_err = datastore.assert_store(options.datastore)
  if not store then
    return nil, store_err
  end
  return setmetatable({
    default_addr_ttl = options.default_addr_ttl or 3600,
    _datastore = store,
    _prefix = options.prefix or DEFAULT_PREFIX,
  }, Store)
end

M.Store = Store

return M
