local kad_dht = require("lua_libp2p.kad_dht")
local kad_protocol = require("lua_libp2p.kad_dht.protocol")
local varint = require("lua_libp2p.multiformats.varint")

local function fake_hash(value)
  local map = {
    ["local"] = string.char(0) .. string.rep("\0", 31),
    ["peer-a"] = string.char(128) .. string.rep("\0", 31),
    ["peer-b"] = string.char(129) .. string.rep("\0", 31),
    ["peer-c"] = string.char(130) .. string.rep("\0", 31),
    ["peer-d"] = string.char(64) .. string.rep("\0", 31),
    ["peer-e"] = string.char(32) .. string.rep("\0", 31),
  }
  return map[value] or (string.char(255) .. string.rep("\0", 31))
end

local function run()
  local dial_calls = 0
  local host = {}

  function host:dial(target)
    dial_calls = dial_calls + 1
    if target.addr == "/ip4/1.2.3.4/tcp/4001" then
      return {}, { remote_peer_id = "peer-a" }
    end
    if target.addr == "/ip4/2.3.4.5/tcp/4001" then
      return {}, { remote_peer_id = "peer-c" }
    end
    return nil, nil, "dial failed"
  end

  function host:new_stream(peer_or_addr)
    local target_peer = peer_or_addr
    if type(peer_or_addr) == "table" then
      target_peer = peer_or_addr.peer_id
    end
    local closer = {
      { id = "peer-d", addrs = { "/ip4/4.4.4.4/tcp/4001" } },
    }
    if target_peer == "peer-d" then
      closer = {
        { id = "peer-e", addrs = { "/ip4/5.5.5.5/tcp/4001" } },
      }
    end
    local response, response_err = kad_protocol.encode_message({
      type = kad_protocol.MESSAGE_TYPE.FIND_NODE,
      key = "local",
      closer_peers = closer,
    })
    if not response then
      return nil, nil, nil, response_err
    end
    local len, len_err = varint.encode_u64(#response)
    if not len then
      return nil, nil, nil, len_err
    end

    local stream = {
      _in = len .. response,
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
    function stream:write(payload)
      if target_peer == "peer-c" then
        return nil, "query stream failed"
      end
      self._out = self._out .. payload
      return true
    end
    function stream:close_write()
      return true
    end
    function stream:close()
      return true
    end

    return stream, kad_dht.PROTOCOL_ID, {}, {}
  end

  local peer_discovery = {
    discover = function()
      return {
        {
          peer_id = "peer-a",
          addrs = {
            "/ip4/1.2.3.4/tcp/4001",
            "/ip4/1.2.3.4/tcp/4001",
          },
        },
        {
          peer_id = "peer-b",
          addrs = {
            "/ip4/9.9.9.9/tcp/4001",
          },
        },
        {
          addrs = {
            "/ip4/2.3.4.5/tcp/4001",
          },
        },
      }
    end,
  }

  local dht, dht_err = kad_dht.new(host, {
    local_peer_id = "local",
    hash_function = fake_hash,
    peer_discovery = peer_discovery,
  })
  if not dht then
    return nil, dht_err
  end

  local report, bootstrap_err = dht:_bootstrap()
  if not report then
    return nil, bootstrap_err
  end
  if report.attempted ~= 3 then
    return nil, "expected deduped bootstrap attempts"
  end
  if report.connected ~= 2 then
    return nil, "expected two successful bootstrap dials"
  end
  if report.failed ~= 1 then
    return nil, "expected one failed bootstrap dial"
  end
  if report.added ~= 2 then
    return nil, "expected two peers added during bootstrap"
  end
  if dial_calls ~= 3 then
    return nil, "unexpected dial call count"
  end

  local peer_a = dht:get_local_peer("peer-a")
  local peer_c = dht:get_local_peer("peer-c")
  if not peer_a or not peer_c then
    return nil, "bootstrap should populate routing table"
  end

  local fail_fast_discovery = {
    discover = function()
      return {
        {
          peer_id = "peer-b",
          addrs = {
            "/ip4/9.9.9.9/tcp/4001",
          },
        },
      }
    end,
  }

  local fast_ok, fast_err = dht:_bootstrap({
    peer_discovery = fail_fast_discovery,
    fail_fast = true,
  })
  if fast_ok ~= nil or fast_err == nil then
    return nil, "expected fail_fast bootstrap to return first dial error"
  end

  local walk, walk_err = dht:_random_walk({
    max_queries = 2,
  })
  if not walk then
    return nil, walk_err
  end
  if walk.queried < 3 then
    return nil, "expected random walk to continue querying discovered peers"
  end
  if walk.responses < 2 or walk.failed ~= 1 then
    return nil, "expected random walk successes plus one initial failure"
  end
  if walk.added < 2 then
    return nil, "expected random walk to add discovered peers"
  end

  local peer_d = dht:get_local_peer("peer-d")
  if not peer_d then
    return nil, "random walk should add discovered peer-d"
  end
  local peer_e = dht:get_local_peer("peer-e")
  if not peer_e then
    return nil, "random walk should query discovered peer-d and add peer-e"
  end

  return true
end

return {
  name = "kad-dht bootstrap via discovery",
  run = run,
}
