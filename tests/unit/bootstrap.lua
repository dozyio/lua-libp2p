local bootstrap = require("lua_libp2p.bootstrap")

local function run()
  if #bootstrap.DEFAULT_BOOTSTRAPPERS ~= 6 then
    return nil, "expected six default bootstrappers"
  end
  local all = bootstrap.default_bootstrappers()
  if #all ~= 6 then
    return nil, "expected default_bootstrappers to return all defaults"
  end
  local dialable = bootstrap.default_bootstrappers({ dialable_only = true })
  if #dialable ~= 1 then
    return nil, "expected only concrete tcp bootstrapper to be directly dialable"
  end
  return true
end

return {
  name = "bootstrap default peer list",
  run = run,
}
