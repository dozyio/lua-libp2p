local kad_dht = require("lua_libp2p.kad_dht")
local kad_protocol = require("lua_libp2p.kad_dht.protocol")
local varint = require("lua_libp2p.multiformats.varint")

local function fake_hash(value)
  local map = {
    ["local"] = string.char(0) .. string.rep("\0", 31),
    ["peer-a"] = string.char(128) .. string.rep("\0", 31),
    ["peer-b"] = string.char(129) .. string.rep("\0", 31),
    ["peer-c"] = string.char(130) .. string.rep("\0", 31),
    ["peer-d"] = string.char(131) .. string.rep("\0", 31),
  }
  return map[value] or (string.char(255) .. string.rep("\0", 31))
end

local function run()
  local dial_calls = 0
  local host = {}

  function host:dial(target)
    dial_calls = dial_calls + 1
    if target.addr == "/ip4/1.2.3.4/tcp/4001/p2p/peer-a" then
      return {}, { remote_peer_id = "peer-a" }
    end
    if target.addr == "/ip4/2.3.4.5/tcp/4001/p2p/peer-c" then
      return {}, { remote_peer_id = "peer-c" }
    end
    return nil, nil, "dial failed"
  end

  function host:new_stream(peer_or_addr)
    local response, response_err = kad_protocol.encode_message({
      type = kad_protocol.MESSAGE_TYPE.FIND_NODE,
      key = "local",
      closer_peers = {
        { id = "peer-d" },
      },
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
      if peer_or_addr == "peer-c" then
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
            "/ip4/1.2.3.4/tcp/4001/p2p/peer-a",
            "/ip4/1.2.3.4/tcp/4001/p2p/peer-a",
          },
        },
        {
          peer_id = "peer-b",
          addrs = {
            "/ip4/9.9.9.9/tcp/4001/p2p/peer-b",
          },
        },
        {
          addrs = {
            "/ip4/2.3.4.5/tcp/4001/p2p/peer-c",
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

  local report, bootstrap_err = dht:bootstrap()
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

  local peer_a = dht:find_peer("peer-a")
  local peer_c = dht:find_peer("peer-c")
  if not peer_a or not peer_c then
    return nil, "bootstrap should populate routing table"
  end

  local fail_fast_discovery = {
    discover = function()
      return {
        {
          peer_id = "peer-b",
          addrs = {
            "/ip4/9.9.9.9/tcp/4001/p2p/peer-b",
          },
        },
      }
    end,
  }

  local fast_ok, fast_err = dht:bootstrap({
    peer_discovery = fail_fast_discovery,
    fail_fast = true,
  })
  if fast_ok ~= nil or fast_err == nil then
    return nil, "expected fail_fast bootstrap to return first dial error"
  end

  local walk, walk_err = dht:random_walk({
    max_queries = 2,
  })
  if not walk then
    return nil, walk_err
  end
  if walk.queried ~= 2 then
    return nil, "expected two random walk queries"
  end
  if walk.responses ~= 1 or walk.failed ~= 1 then
    return nil, "expected one random walk success and one failure"
  end
  if walk.added < 1 then
    return nil, "expected random walk to add discovered peers"
  end

  local peer_d = dht:find_peer("peer-d")
  if not peer_d then
    return nil, "random walk should add discovered peer-d"
  end

  return true
end

return {
  name = "kad-dht bootstrap via discovery",
  run = run,
}
