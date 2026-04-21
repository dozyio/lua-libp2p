package.path = table.concat({
  "./?.lua",
  "./?/init.lua",
  package.path,
}, ";")

local discovery = require("lua_libp2p.discovery")
local discovery_bootstrap = require("lua_libp2p.discovery.bootstrap")
local host_mod = require("lua_libp2p.host")
local kad_dht = require("lua_libp2p.kad_dht")
local multiaddr = require("lua_libp2p.multiaddr")
local socket = require("socket")

local mode = arg[1]

local function read_file(path)
  local f = io.open(path, "rb")
  if not f then
    return nil
  end
  local data = f:read("*a")
  f:close()
  return data
end

local function system_dns_servers()
  local data = read_file("/etc/resolv.conf") or ""
  local out = {}
  local seen = {}
  for line in data:gmatch("[^\r\n]+") do
    local ns = line:match("^%s*nameserver%s+([^%s#;]+)")
    if ns and not seen[ns] then
      seen[ns] = true
      out[#out + 1] = ns
    end
  end
  if #out == 0 then
    out = { "127.0.0.1", "::1" }
  end
  return out
end

local function u16be(n)
  local hi = math.floor(n / 256) % 256
  local lo = n % 256
  return string.char(hi, lo)
end

local function parse_u16be(bytes, offset)
  local a, b = bytes:byte(offset, offset + 1)
  if not a or not b then
    return nil
  end
  return a * 256 + b
end

local function encode_dns_name(name)
  local out = {}
  for label in tostring(name):gmatch("[^.]+") do
    if #label == 0 or #label > 63 then
      return nil, "invalid dns label length"
    end
    out[#out + 1] = string.char(#label)
    out[#out + 1] = label
  end
  out[#out + 1] = "\0"
  return table.concat(out)
end

local function skip_dns_name(packet, offset)
  local i = offset
  while true do
    local len = packet:byte(i)
    if not len then
      return nil
    end
    if len == 0 then
      return i + 1
    end
    if len >= 192 then
      local next_byte = packet:byte(i + 1)
      if not next_byte then
        return nil
      end
      return i + 2
    end
    i = i + 1 + len
  end
end

local function parse_txt_rdata_parts(rdata)
  local parts = {}
  local i = 1
  while i <= #rdata do
    local len = rdata:byte(i)
    if not len then
      break
    end
    local start_i = i + 1
    local end_i = start_i + len - 1
    if end_i > #rdata then
      break
    end
    parts[#parts + 1] = rdata:sub(start_i, end_i)
    i = end_i + 1
  end
  return parts
end

local function resolve_dnsaddr_with_udp(domain, nameserver)
  local qname, qname_err = encode_dns_name("_dnsaddr." .. domain)
  if not qname then
    return nil, qname_err
  end

  local txid = math.random(0, 65535)
  local header = table.concat({
    u16be(txid),
    u16be(0x0100), -- recursion desired
    u16be(1),
    u16be(0),
    u16be(0),
    u16be(0),
  })
  local question = qname .. u16be(16) .. u16be(1) -- TXT IN
  local query = header .. question

  local udp = assert(socket.udp())
  udp:settimeout(3)
  udp:setpeername(nameserver, 53)

  local ok, send_err = udp:send(query)
  if not ok then
    udp:close()
    return nil, send_err or "dns send failed"
  end

  local response, recv_err = udp:receive()
  udp:close()
  if not response then
    return nil, recv_err or "dns receive failed"
  end
  if #response < 12 then
    return nil, "short dns response"
  end

  local got_id = parse_u16be(response, 1)
  if got_id ~= txid then
    return nil, "dns txid mismatch"
  end
  local flags = parse_u16be(response, 3)
  local rcode = flags % 16
  if rcode ~= 0 then
    return nil, "dns error rcode=" .. tostring(rcode)
  end

  local qdcount = parse_u16be(response, 5) or 0
  local ancount = parse_u16be(response, 7) or 0

  local offset = 13
  for _ = 1, qdcount do
    offset = skip_dns_name(response, offset)
    if not offset or offset + 3 > #response then
      return nil, "malformed dns question"
    end
    offset = offset + 4
  end

  local out = {}
  for _ = 1, ancount do
    offset = skip_dns_name(response, offset)
    if not offset or offset + 9 > #response then
      return nil, "malformed dns answer"
    end
    local rr_type = parse_u16be(response, offset)
    local rdlen = parse_u16be(response, offset + 8)
    if not rr_type or not rdlen then
      return nil, "malformed dns rr"
    end
    local rdata_start = offset + 10
    local rdata_end = rdata_start + rdlen - 1
    if rdata_end > #response then
      return nil, "truncated dns rdata"
    end
    if rr_type == 16 then
      local parts = parse_txt_rdata_parts(response:sub(rdata_start, rdata_end))
      for _, txt in ipairs(parts) do
        if txt and txt:sub(1, 8) == "dnsaddr=" then
          out[#out + 1] = txt
        end
      end
    end
    offset = rdata_end + 1
  end

  return out
end

local function resolve_dnsaddr_system(domain)
  local last_err = nil
  local servers = system_dns_servers()
  for _, ns in ipairs(servers) do
    local out, err = resolve_dnsaddr_with_udp(domain, ns)
    if out then
      return out
    end
    last_err = err
  end
  return nil, last_err or "dns lookup failed"
end

local function usage()
  io.stderr:write("usage:\n")
  io.stderr:write("  lua examples/kad_dht_bootstrap_demo.lua server [listen-multiaddr]\n")
  io.stderr:write("  lua examples/kad_dht_bootstrap_demo.lua client </bootstrap-multiaddr-with-p2p>\n")
  io.stderr:write("  lua examples/kad_dht_bootstrap_demo.lua client --default-bootstrap\n")
  io.stderr:write("  (note: bootstrap multiaddr must start with '/'; e.g. /ip4/127.0.0.1/tcp/12345/p2p/12D3KooW...)\n")
end

local function run_server()
  local listen_addr = arg[2] or "/ip4/127.0.0.1/tcp/0"

  local host, host_err = host_mod.new({
    listen_addrs = { listen_addr },
    services = { "identify", "ping" },
    blocking = true,
    accept_timeout = 0.05,
    poll_interval = 0.01,
    on_started = function(h)
      io.stdout:write("kad-dht demo server listening:\n")
      for _, addr in ipairs(h:get_multiaddrs()) do
        io.stdout:write("  " .. addr .. "\n")
      end
      io.stdout:write("running; Ctrl-C to stop\n")
      io.stdout:flush()
    end,
  })
  if not host then
    io.stderr:write("host init failed: " .. tostring(host_err) .. "\n")
    os.exit(1)
  end

  local dht, dht_err = kad_dht.new(host, {})
  if not dht then
    io.stderr:write("dht init failed: " .. tostring(dht_err) .. "\n")
    os.exit(1)
  end

  local dht_started, dht_start_err = dht:start()
  if not dht_started then
    io.stderr:write("dht start failed: " .. tostring(dht_start_err) .. "\n")
    os.exit(1)
  end

  local started, start_err = host:start()
  if not started then
    io.stderr:write("host stopped with error: " .. tostring(start_err) .. "\n")
    os.exit(1)
  end
end

local function run_client()
  local bootstrap_arg = arg[2]
  local bootstrap_addrs = {}
  local parsed_input = nil
  local dns_cache = {}
  local function dnsaddr_resolver(domain)
    if dns_cache[domain] ~= nil then
      local cached = dns_cache[domain]
      io.stdout:write("resolving dnsaddr domain: " .. tostring(domain) .. " (cached)\n")
      if cached.ok then
        io.stdout:write("  got " .. tostring(#cached.records) .. " TXT dnsaddr record(s)\n")
        return cached.records
      end
      io.stdout:write("  resolve failed: " .. tostring(cached.err) .. "\n")
      return nil, cached.err
    end

    io.stdout:write("resolving dnsaddr domain: " .. tostring(domain) .. "\n")
    local records, err = resolve_dnsaddr_system(domain)
    if not records then
      io.stdout:write("  resolve failed: " .. tostring(err) .. "\n")
      dns_cache[domain] = { ok = false, err = err }
      return nil, err
    end
    io.stdout:write("  got " .. tostring(#records) .. " TXT dnsaddr record(s)\n")
    for _, rec in ipairs(records) do
      io.stdout:write("    " .. tostring(rec) .. "\n")
    end
    dns_cache[domain] = { ok = true, records = records }
    return records
  end
  io.stdout:write("dnsaddr resolver: system nameservers from /etc/resolv.conf\n")

  if bootstrap_arg == "--default-bootstrap" then
    bootstrap_addrs = kad_dht.default_bootstrappers()
    if #bootstrap_addrs == 0 then
      io.stderr:write("no default bootstrap addresses available\n")
      os.exit(1)
    end
    io.stdout:write("using default bootstrappers:\n")
    for _, addr in ipairs(bootstrap_addrs) do
      io.stdout:write("  " .. addr .. "\n")
    end
    parsed_input = multiaddr.parse(bootstrap_addrs[#bootstrap_addrs])
  else
    local bootstrap_addr = bootstrap_arg
    if not bootstrap_addr or bootstrap_addr == "" then
      usage()
      os.exit(2)
    end

    local parse_err
    parsed_input, parse_err = multiaddr.parse(bootstrap_addr)
    if not parsed_input then
      io.stderr:write("invalid bootstrap multiaddr: " .. tostring(parse_err) .. "\n")
      usage()
      os.exit(2)
    end
    bootstrap_addrs = { bootstrap_addr }
  end

  local host, host_err = host_mod.new({
    listen_addrs = { "/ip4/127.0.0.1/tcp/0" },
    services = { "identify" },
    blocking = false,
    accept_timeout = 0.05,
  })
  if not host then
    io.stderr:write("host init failed: " .. tostring(host_err) .. "\n")
    os.exit(1)
  end

  local started, start_err = host:start()
  if not started then
    io.stderr:write("host start failed: " .. tostring(start_err) .. "\n")
    os.exit(1)
  end

  local function pump_host(iterations)
    for _ = 1, iterations do
      local ok, poll_err = host:poll_once(0)
      if not ok then
        return nil, poll_err
      end
    end
    return true
  end

  local bootstrap_source, source_err = discovery_bootstrap.new({
    list = bootstrap_addrs,
    dialable_only = true,
    dnsaddr_resolver = dnsaddr_resolver,
    ignore_resolve_errors = false,
  })
  if not bootstrap_source then
    io.stderr:write("bootstrap source init failed: " .. tostring(source_err) .. "\n")
    os.exit(1)
  end

  local peer_discovery, discovery_err = discovery.new({
    sources = { bootstrap_source },
  })
  if not peer_discovery then
    io.stderr:write("peer discovery init failed: " .. tostring(discovery_err) .. "\n")
    os.exit(1)
  end

  local discovered, discovered_err = peer_discovery:discover({
    dialable_only = true,
  })
  if not discovered then
    io.stderr:write("discovery failed: " .. tostring(discovered_err) .. "\n")
    os.exit(1)
  end
  io.stdout:write("discovery candidates:\n")
  for _, candidate in ipairs(discovered) do
    io.stdout:write("  peer=" .. tostring(candidate.peer_id) .. " addr=" .. tostring((candidate.addrs or {})[1]) .. "\n")
  end

  local dht, dht_err = kad_dht.new(host, {
    peer_discovery = peer_discovery,
  })
  if not dht then
    io.stderr:write("dht init failed: " .. tostring(dht_err) .. "\n")
    os.exit(1)
  end

  local dht_started, dht_start_err = dht:start()
  if not dht_started then
    io.stderr:write("dht start failed: " .. tostring(dht_start_err) .. "\n")
    os.exit(1)
  end

  local report, bootstrap_err = dht:bootstrap({
    max_success = 1,
  })
  if not report then
    io.stderr:write("bootstrap failed: " .. tostring(bootstrap_err) .. "\n")
    os.exit(1)
  end

  io.stdout:write("bootstrap report:\n")
  io.stdout:write("  attempted: " .. tostring(report.attempted) .. "\n")
  io.stdout:write("  connected: " .. tostring(report.connected) .. "\n")
  io.stdout:write("  added: " .. tostring(report.added) .. "\n")
  io.stdout:write("  failed: " .. tostring(report.failed) .. "\n")
  if #report.errors > 0 then
    io.stdout:write("  errors:\n")
    for _, err in ipairs(report.errors) do
      local suffix = ""
      if type(err) == "table" and type(err.context) == "table" then
        local key_name = err.context.received_key_type_name
        local key_num = err.context.received_key_type
        if key_name or key_num then
          suffix = string.format(" (received_key_type=%s/%s)", tostring(key_name), tostring(key_num))
        end
      end
      io.stdout:write("    " .. tostring(err) .. suffix .. "\n")
    end
  end

  local pumped, pump_err = pump_host(20)
  if not pumped then
    io.stderr:write("host pump failed: " .. tostring(pump_err) .. "\n")
  end
  if report.attempted == 0 then
    io.stdout:write("  hint: no dialable peers discovered from bootstrap input\n")
  end

  local peers = dht.routing_table:all_peers()
  io.stdout:write("routing table peers:\n")
  for _, p in ipairs(peers) do
    io.stdout:write("  " .. tostring(p.peer_id) .. "\n")
  end

  local parsed_bootstrap = parsed_input
  local bootstrap_peer_id = nil
  if parsed_bootstrap and type(parsed_bootstrap.components) == "table" then
    for i = #parsed_bootstrap.components, 1, -1 do
      local c = parsed_bootstrap.components[i]
      if c.protocol == "p2p" then
        bootstrap_peer_id = c.value
        break
      end
    end
  end

  if bootstrap_peer_id then
    io.stdout:write("running one FIND_NODE query against bootstrap peer...\n")
    local lookup, lookup_err = dht:find_node(bootstrap_addrs[1], host:peer_id().id)
    if not lookup then
      io.stderr:write("FIND_NODE failed: " .. tostring(lookup_err) .. "\n")
    else
      io.stdout:write("FIND_NODE closer peers: " .. tostring(#lookup) .. "\n")
      for _, candidate in ipairs(lookup) do
        io.stdout:write("  " .. tostring(candidate.peer_id) .. "\n")
      end
    end

    io.stdout:write("running one random-walk refresh (target=self peer id)...\n")
    local walk, walk_err = dht:random_walk({
      max_queries = 1,
      bootstrap_if_empty = true,
    })
    if not walk then
      io.stderr:write("random walk failed: " .. tostring(walk_err) .. "\n")
    else
      io.stdout:write("random walk report:\n")
      io.stdout:write("  queried: " .. tostring(walk.queried) .. "\n")
      io.stdout:write("  responses: " .. tostring(walk.responses) .. "\n")
      io.stdout:write("  added: " .. tostring(walk.added) .. "\n")
      io.stdout:write("  failed: " .. tostring(walk.failed) .. "\n")
    end
  else
    io.stdout:write("bootstrap addr has no /p2p component; skipping FIND_NODE demo\n")
  end

  host:stop()
end

if mode == "server" then
  run_server()
elseif mode == "client" then
  run_client()
else
  usage()
  os.exit(2)
end
