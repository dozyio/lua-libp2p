package.path = table.concat({
  "./?.lua",
  "./?/init.lua",
  package.path,
}, ";")

local igd = require("lua_libp2p.port_mapping.upnp.igd")
local ssdp = require("lua_libp2p.port_mapping.upnp.ssdp")

local opts = {
  timeout = 3,
  mx = 2,
  parse_igd = false,
}

local i = 1
while i <= #arg do
  local name = arg[i]
  local value = arg[i + 1]
  if name == "--timeout" then
    opts.timeout = tonumber(value) or opts.timeout
    i = i + 2
  elseif name == "--mx" then
    opts.mx = tonumber(value) or opts.mx
    i = i + 2
  elseif name == "--interface" then
    opts.interface = value
    i = i + 2
  elseif name == "--parse-igd" then
    opts.parse_igd = true
    i = i + 1
  elseif name == "--help" or name == "-h" then
    print("usage: lua examples/ssdp_discover.lua [--timeout seconds] [--mx seconds] [--interface ip] [--parse-igd]")
    os.exit(0)
  else
    io.stderr:write("unknown argument: " .. tostring(name) .. "\n")
    os.exit(1)
  end
end

print(string.format("Searching for UPnP IGD gateways via SSDP (IPv4) for %.1fs...", opts.timeout))

local responses, err = ssdp.discover(opts)
if not responses then
  error(err)
end

if #responses == 0 then
  print("No SSDP gateway responses found.")
  os.exit(0)
end

for index, response in ipairs(responses) do
  print(string.format("\n[%d] %s", index, response.location or "(no location)"))
  if response.st then
    print("  st: " .. response.st)
  end
  if response.usn then
    print("  usn: " .. response.usn)
  end
  if opts.parse_igd and response.location then
    local client, client_err = igd.from_location(response.location)
    if client then
      print("  service_type: " .. tostring(client.service_type))
      print("  control_url: " .. tostring(client.control_url))
      local external_ip, ip_err = client:get_external_ip()
      if external_ip then
        print("  external_ip: " .. external_ip)
      else
        print("  external_ip_error: " .. tostring(ip_err))
      end
    else
      print("  igd_error: " .. tostring(client_err))
    end
  end
end
