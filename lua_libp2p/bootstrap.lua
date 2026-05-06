--- Bootstrap peer list constants.
-- @module lua_libp2p.bootstrap
local multiaddr = require("lua_libp2p.multiaddr")
local table_utils = require("lua_libp2p.util.tables")

local M = {}

M.DEFAULT_BOOTSTRAPPERS = {
  "/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
  "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
  "/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
  "/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6L6K6TQK6KiBovQ",
  "/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
  "/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
}

--- Return default bootstrap list.
-- `opts.dialable_only` (`boolean`) filters to TCP-dialable entries.
-- @tparam[opt] table opts
-- @treturn table addrs
function M.default_bootstrappers(opts)
  local options = opts or {}
  local list = table_utils.copy_list(M.DEFAULT_BOOTSTRAPPERS)
  if not options.dialable_only then
    return list
  end

  local out = {}
  for _, addr in ipairs(list) do
    local endpoint = multiaddr.to_tcp_endpoint(addr)
    if endpoint then
      out[#out + 1] = addr
    end
  end
  return out
end

return M
