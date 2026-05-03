local kad_dht = require("lua_libp2p.kad_dht")

local function fake_hash(value)
  local map = {
    ["local"] = string.char(0) .. string.rep("\0", 31),
    ["peer-a"] = string.char(128) .. string.rep("\0", 31),
    ["peer-b"] = string.char(129) .. string.rep("\0", 31),
  }
  return map[value] or (string.char(255) .. string.rep("\0", 31))
end

local function run()
  local handlers = {}
  local host = {}

  function host:on(name, fn)
    handlers[name] = handlers[name] or {}
    handlers[name][#handlers[name] + 1] = fn
    return true
  end

  function host:off(name, fn)
    local list = handlers[name]
    if not list then
      return false
    end
    for i = 1, #list do
      if list[i] == fn then
        table.remove(list, i)
        return true
      end
    end
    return false
  end

  function host:new_stream(peer_or_addr)
    if peer_or_addr == "peer-b" then
      return nil, nil, nil, "peer-b not serving kad"
    end
    local stream = {}
    function stream:close_write() return true end
    function stream:close() return true end
    return stream, kad_dht.PROTOCOL_ID, {}, {}
  end

  local dht, dht_err = kad_dht.new(host, {
    local_peer_id = "local",
    hash_function = fake_hash,
  })
  if not dht then
    return nil, dht_err
  end

  local added_a, add_a_err = dht:add_peer("peer-a")
  if not added_a then
    return nil, add_a_err
  end
  local added_b, add_b_err = dht:add_peer("peer-b")
  if not added_b then
    return nil, add_b_err
  end

  dht._peer_health["peer-b"] = { stale = true }
  if dht._peer_health["peer-b"] == nil or dht._peer_health["peer-b"].stale ~= true then
    return nil, "expected peer-b to be marked stale"
  end

  local report, refresh_err = dht:refresh_once({
    max_checks = 5,
    min_recheck_seconds = 0,
  })
  if not report then
    return nil, refresh_err
  end
  if report.removed ~= 1 then
    return nil, "expected stale unsupported peer to be evicted during refresh"
  end

  local peer_b = dht:find_peer("peer-b")
  if peer_b ~= nil then
    return nil, "peer-b should be removed from routing table"
  end

  local stopped, stop_err = dht:stop()
  if not stopped then
    return nil, stop_err
  end

  return true
end

return {
  name = "kad-dht refresh manager stale peer policy",
  run = run,
}
