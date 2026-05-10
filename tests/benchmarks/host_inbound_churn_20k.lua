local bench = require("tests.integration.host_inbound_churn_stability")

local M = {
  name = "host inbound churn 20k",
}

function M.run()
  return bench.run({
    iterations = tonumber(os.getenv("LUA_LIBP2P_BENCH_INBOUND_CHURN_ITERS")) or 20000,
    progress_interval = tonumber(os.getenv("LUA_LIBP2P_BENCH_INBOUND_CHURN_PROGRESS")) or 1000,
  })
end

return M
