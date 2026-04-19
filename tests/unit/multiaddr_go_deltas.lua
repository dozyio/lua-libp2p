local multiaddr = require("lua_libp2p.multiaddr")
local deltas = require("tests.helpers.multiaddr_go_deltas")

local function run()
  for _, c in ipairs(deltas.cases) do
    local parsed = multiaddr.parse(c.input)
    local lua_valid = parsed ~= nil

    if lua_valid ~= c.lua_valid then
      return nil, string.format("lua delta expectation mismatch for %s", c.input)
    end

    if c.go_valid == c.lua_valid then
      return nil, string.format("delta case does not differ from go for %s", c.input)
    end
  end

  return true
end

return {
  name = "multiaddr go strictness deltas",
  run = run,
}
