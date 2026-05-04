local kad_dht = require("lua_libp2p.kad_dht")
local kad_protocol = require("lua_libp2p.kad_dht.protocol")
local providers = require("lua_libp2p.kad_dht.providers")
local varint = require("lua_libp2p.multiformats.varint")

local function fake_hash(value)
  local map = {
    local_peer = string.char(0) .. string.rep("\0", 31),
    provider_a = string.char(128) .. string.rep("\0", 31),
    provider_b = string.char(129) .. string.rep("\0", 31),
    ["cid-key"] = string.char(128) .. string.rep("\0", 31),
  }
  return map[value] or (string.char(255) .. string.rep("\0", 31))
end

local function new_host()
  local host = {
    _peer = { id = "local_peer" },
    peerstore = {
      _merged = {},
    },
  }
  function host:peer_id()
    return self._peer
  end
  function host.peerstore:merge(peer_id, info)
    self._merged[peer_id] = info
    return true
  end
  function host.peerstore:get_addrs()
    return {}
  end
  return host
end

local function new_scripted_stream(message)
  local payload = assert(kad_protocol.encode_message(message))
  local frame = assert(varint.encode_u64(#payload)) .. payload
  local stream = {
    _in = frame,
    _out = "",
  }
  function stream:read(n)
    if #self._in < n then
      return nil, "unexpected EOF"
    end
    local out = self._in:sub(1, n)
    self._in = self._in:sub(n + 1)
    return out
  end
  function stream:write(payload_bytes)
    self._out = self._out .. payload_bytes
    return true
  end
  function stream:close_write()
    return true
  end
  return stream
end

local function read_response(stream)
  local reader = { _in = stream._out }
  function reader:read(n)
    if #self._in < n then
      return nil, "unexpected EOF"
    end
    local out = self._in:sub(1, n)
    self._in = self._in:sub(n + 1)
    return out
  end
  return kad_protocol.read(reader)
end

local function run()
  local now = 1000
  local store = providers.new({
    default_ttl_seconds = 10,
    now = function()
      return now
    end,
  })

  local added, add_err = store:add("cid-key", {
    peer_id = "provider_a",
    addrs = { "/ip4/8.8.8.8/tcp/4001" },
  })
  if not added then
    return nil, add_err
  end
  local entries, entries_err = store:get("cid-key")
  if not entries then
    return nil, entries_err
  end
  if #entries ~= 1 or entries[1].peer_id ~= "provider_a" then
    return nil, "provider store should return stored provider"
  end
  if entries[1].first_seen_at ~= 1000 or entries[1].updated_at ~= 1000 or entries[1].expires_at ~= 1010 then
    return nil, "provider store should set provider timestamps"
  end

  now = 1011
  entries = assert(store:get("cid-key"))
  if #entries ~= 0 then
    return nil, "provider store should expire provider entries"
  end

  local host = new_host()
  local dht = assert(kad_dht.new(host, {
    mode = "server",
    hash_function = fake_hash,
    address_filter = "all",
    provider_ttl_seconds = 60,
  }))

  local add_stream = new_scripted_stream({
    type = kad_protocol.MESSAGE_TYPE.ADD_PROVIDER,
    key = "cid-key",
    provider_peers = {
      { id = "provider_a", addrs = { "/ip4/8.8.8.8/tcp/4001" } },
    },
  })
  local handled, handle_err = dht:_handle_rpc(add_stream)
  if not handled then
    return nil, handle_err
  end
  local add_response, add_response_err = read_response(add_stream)
  if not add_response then
    return nil, add_response_err
  end
  if add_response.type ~= kad_protocol.MESSAGE_TYPE.ADD_PROVIDER then
    return nil, "ADD_PROVIDER should return ADD_PROVIDER response"
  end
  local local_providers = assert(dht:get_local_providers("cid-key"))
  if #local_providers ~= 1 or local_providers[1].peer_id ~= "provider_a" then
    return nil, "ADD_PROVIDER should store provider locally"
  end
  if not host.peerstore._merged.provider_a then
    return nil, "ADD_PROVIDER should merge provider addrs into peerstore"
  end

  local get_stream = new_scripted_stream({
    type = kad_protocol.MESSAGE_TYPE.GET_PROVIDERS,
    key = "cid-key",
  })
  handled, handle_err = dht:_handle_rpc(get_stream)
  if not handled then
    return nil, handle_err
  end
  local get_response, get_response_err = read_response(get_stream)
  if not get_response then
    return nil, get_response_err
  end
  if get_response.type ~= kad_protocol.MESSAGE_TYPE.GET_PROVIDERS then
    return nil, "GET_PROVIDERS should return GET_PROVIDERS response"
  end
  if #get_response.provider_peers ~= 1 or get_response.provider_peers[1].id ~= "provider_a" then
    return nil, "GET_PROVIDERS should return local provider peers"
  end

  local client_dht = assert(kad_dht.new(new_host(), {
    mode = "client",
    hash_function = fake_hash,
    address_filter = "all",
  }))
  local rejected, rejected_err = client_dht:_handle_add_provider({
    type = kad_protocol.MESSAGE_TYPE.ADD_PROVIDER,
    key = "cid-key",
    provider_peers = {
      { id = "provider_b" },
    },
  })
  if rejected ~= nil or not rejected_err or rejected_err.kind ~= "unsupported" then
    return nil, "client-mode dht should reject provider writes"
  end

  return true
end

return {
  name = "kad-dht provider store and server RPCs",
  run = run,
}
