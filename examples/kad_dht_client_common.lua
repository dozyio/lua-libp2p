package.path = table.concat({
  "./?.lua",
  "./?/init.lua",
  package.path,
}, ";")

local host_mod = require("lua_libp2p.host")
local identify_service = require("lua_libp2p.protocol_identify.service")
local ping_service = require("lua_libp2p.protocol_ping.service")
local kad_dht_service = require("lua_libp2p.kad_dht")
local peer_discovery_bootstrap = require("lua_libp2p.peer_discovery_bootstrap")

local M = {}

function M.parse_args(args)
  local opts = {
    bootstrappers = {},
    alpha = 10,
    disjoint_paths = 10,
    count = 20,
    limit = 5,
    connect_timeout = 6,
    io_timeout = 10,
  }

  local i = 1
  while i <= #args do
    local name = args[i]
    local value = args[i + 1]
    if name == "--bootstrap" then
      if not value then
        return nil, "--bootstrap requires a multiaddr"
      end
      opts.bootstrappers[#opts.bootstrappers + 1] = value
      i = i + 2
    elseif name == "--target" then
      opts.target = value
      i = i + 2
    elseif name == "--key" then
      opts.key = value
      i = i + 2
    elseif name == "--key-hex" then
      opts.key_hex = value
      i = i + 2
    elseif name == "--count" then
      opts.count = tonumber(value)
      i = i + 2
    elseif name == "--limit" then
      opts.limit = tonumber(value)
      i = i + 2
    elseif name == "--alpha" then
      opts.alpha = tonumber(value)
      i = i + 2
    elseif name == "--disjoint-paths" then
      opts.disjoint_paths = tonumber(value)
      i = i + 2
    elseif name == "--connect-timeout" then
      opts.connect_timeout = tonumber(value)
      i = i + 2
    elseif name == "--io-timeout" then
      opts.io_timeout = tonumber(value)
      i = i + 2
    elseif name == "--private-addrs" then
      opts.address_filter = "all"
      i = i + 1
    else
      return nil, "unknown argument: " .. tostring(name)
    end
  end

  return opts
end

function M.hex_to_bytes(hex)
  if type(hex) ~= "string" or hex == "" or (#hex % 2) ~= 0 or hex:match("[^0-9a-fA-F]") then
    return nil, "hex key must be non-empty even-length hex"
  end
  local out = {}
  for i = 1, #hex, 2 do
    out[#out + 1] = string.char(tonumber(hex:sub(i, i + 1), 16))
  end
  return table.concat(out)
end

function M.key_from_opts(opts)
  if opts.key_hex then
    return M.hex_to_bytes(opts.key_hex)
  end
  if opts.key and opts.key ~= "" then
    return opts.key
  end
  return nil, "provide --key <bytes-as-text> or --key-hex <hex>"
end

local function bootstrap_config(opts)
  if #opts.bootstrappers > 0 then
    return {
      list = opts.bootstrappers,
      dialable_only = true,
      ignore_resolve_errors = true,
      dial_on_start = true,
    }
  end
  return {
    ignore_resolve_errors = true,
    dial_on_start = true,
  }
end

local function print_error_summary(errors)
  local counts = {}
  for _, err in ipairs(errors or {}) do
    local key = tostring(err)
    counts[key] = (counts[key] or 0) + 1
  end
  local rows = {}
  for message, count in pairs(counts) do
    rows[#rows + 1] = { message = message, count = count }
  end
  table.sort(rows, function(a, b)
    if a.count == b.count then
      return a.message < b.message
    end
    return a.count > b.count
  end)
  for _, row in ipairs(rows) do
    io.stdout:write("    " .. tostring(row.count) .. "x " .. row.message .. "\n")
  end
end

function M.print_lookup(label, lookup)
  io.stdout:write(label .. " lookup:\n")
  io.stdout:write("  queried: " .. tostring(lookup and lookup.queried or 0) .. "\n")
  io.stdout:write("  responses: " .. tostring(lookup and lookup.responses or 0) .. "\n")
  io.stdout:write("  failed: " .. tostring(lookup and lookup.failed or 0) .. "\n")
  io.stdout:write("  cancelled: " .. tostring(lookup and lookup.cancelled or 0) .. "\n")
  io.stdout:write("  active_peak: " .. tostring(lookup and lookup.active_peak or 0) .. "\n")
  io.stdout:write("  termination: " .. tostring(lookup and lookup.termination or "unknown") .. "\n")
  if lookup and lookup.errors and #lookup.errors > 0 then
    io.stdout:write("  errors:\n")
    print_error_summary(lookup.errors)
  end
end

function M.wait_for_routing_table(host, dht, opts)
  local options = opts or {}
  local timeout = options.timeout or 30
  local min_peers = options.min_peers or 1
  local ctx = options.ctx
  local started_at = os.time()
  while os.time() - started_at < timeout do
    if #(dht.routing_table:all_peers()) >= min_peers then
      return true
    end
    local ok, err = host:sleep(0.25, { ctx = ctx })
    if ok == nil and err then
      return nil, err
    end
  end
  return nil, "timed out waiting for DHT routing table peers"
end

function M.new_client(opts)
  local host, host_err = host_mod.new({
    runtime = "luv",
    peer_discovery = {
      bootstrap = {
        module = peer_discovery_bootstrap,
        config = bootstrap_config(opts),
      },
    },
    listen_addrs = { "/ip4/127.0.0.1/tcp/0" },
    services = {
      identify = { module = identify_service },
      ping = { module = ping_service },
      kad_dht = {
        module = kad_dht_service,
        config = {
          mode = "client",
          alpha = opts.alpha,
          disjoint_paths = opts.disjoint_paths,
          address_filter = opts.address_filter,
        },
      },
    },
    blocking = false,
    connect_timeout = opts.connect_timeout,
    io_timeout = opts.io_timeout,
    accept_timeout = 0.05,
  })
  if not host then
    return nil, host_err
  end

  local started, start_err = host:start()
  if not started then
    return nil, start_err
  end

  local dht = host.kad_dht

  local seeded, bootstrap_err = M.wait_for_routing_table(host, dht, {
    timeout = opts.bootstrap_timeout or 30,
  })
  if not seeded then
    host:stop()
    return nil, bootstrap_err
  end

  io.stdout:write("routing table seeded: peers=" .. tostring(#(dht.routing_table:all_peers())) .. "\n")

  return {
    host = host,
    dht = dht,
    opts = opts,
  }
end

function M.stop(client)
  if client and client.host then
    client.host:stop()
  end
end

return M
