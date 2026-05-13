local kbucket = require("lua_libp2p.kad_dht.kbucket")

local function fake_hash(value)
  local map = {
    ["local"] = string.char(0) .. string.rep("\0", 31),
    ["peer-a"] = string.char(128) .. string.rep("\0", 31),
    ["peer-b"] = string.char(129) .. string.rep("\0", 31),
    ["target-a"] = string.char(128) .. string.rep("\0", 31),
    ["target-b"] = string.char(129) .. string.rep("\0", 31),
  }
  return map[value] or (string.char(255) .. string.rep("\0", 31))
end

local function run()
  local rt, rt_err = kbucket.new({
    local_peer_id = "local",
    bucket_size = 1,
    hash_function = fake_hash,
  })
  if not rt then
    return nil, rt_err
  end

  local added, add_err = rt:try_add_peer("peer-a")
  if not added then
    return nil, add_err or "expected first peer to be added"
  end

  local second_added, second_err = rt:try_add_peer("peer-b")
  if second_added ~= nil then
    return nil, "expected full bucket add to fail without replacement"
  end
  if not second_err or second_err.kind ~= "capacity" then
    return nil, "expected capacity error when bucket is full"
  end

  local replaced, evicted_or_err = rt:try_add_peer("peer-b", { allow_replace = true })
  if not replaced then
    return nil, evicted_or_err or "expected replacement add to succeed"
  end
  if evicted_or_err ~= "peer-a" then
    return nil, "expected peer-a eviction on replacement"
  end

  local nearest_a, nearest_a_err = rt:nearest_peers("target-a", 1)
  if not nearest_a then
    return nil, nearest_a_err
  end
  if #nearest_a ~= 1 or nearest_a[1].peer_id ~= "peer-b" then
    return nil, "unexpected nearest peer result after replacement"
  end

  local removed, remove_err = rt:remove_peer("peer-b")
  if not removed then
    return nil, remove_err or "expected remove_peer to return true"
  end
  if rt:size() ~= 0 then
    return nil, "routing table should be empty after removal"
  end

  local cpl, cpl_err = kbucket.common_prefix_length("peer-a", "target-a")
  if cpl == nil then
    return nil, cpl_err
  end
  if type(cpl) ~= "number" then
    return nil, "common_prefix_length should return number"
  end

  return true
end

return {
  name = "kbucket routing table primitives",
  run = run,
}
