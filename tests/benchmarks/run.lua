package.path = table.concat({
  "./?.lua",
  "./?/init.lua",
  package.path,
}, ";")

local benchmarks = {
  "tests.benchmarks.host_scheduler_hot_paths",
  "tests.benchmarks.host_inbound_churn_20k",
  "tests.benchmarks.identify_throughput",
  "tests.benchmarks.kad_dht_hot_paths",
}

local selected = {}
for i = 1, #arg do
  selected[arg[i]] = true
end

local function should_run(module_name)
  if next(selected) == nil then
    return true
  end
  return selected[module_name] or selected[module_name:match("([^%.]+)$") or module_name]
end

local failed = false
for _, module_name in ipairs(benchmarks) do
  if should_run(module_name) then
    local bench = require(module_name)
    io.stdout:write("benchmark " .. tostring(bench.name or module_name) .. "\n")
    local ok, err = bench.run()
    if not ok then
      failed = true
      io.stderr:write("benchmark failed: " .. module_name .. ": " .. tostring(err) .. "\n")
    end
  end
end

if failed then
  os.exit(1)
end
